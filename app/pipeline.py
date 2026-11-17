"""批次流水线：取数(超时重试/迟到归类) -> 并行两项质控 -> 幂等入仓。
配对批次：同一委托的两支平行管各自走取数与两项质控，均明确通过后按
RPD=2|a-b|/(a+b) 比较浓度，合格则两条原始测值同一事务成对入仓。
全程真实计时，每次调用一个 span，request_id 跨服务关联。"""
import asyncio
import json
import os
import time
import uuid

import httpx

import analysis
import db

SOURCE_URL = os.environ.get("SOURCE_URL", "http://localhost:8001")
QC_URL = os.environ.get("QC_URL", "http://localhost:8002")

# httpx 硬超时只是兜底；逻辑超时由 asyncio.wait 实现，超时后旧请求不被取消、继续运行
HARD_TIMEOUT_S = 30.0

_client = None

# batch_id -> 仍在收尾的迟到尝试任务；批次定稿前要等它们结束，结论才完整
_finalizers = {}


def _track_finalizer(batch_id, task):
    _finalizers.setdefault(batch_id, set()).add(task)
    task.add_done_callback(lambda t: _finalizers.get(batch_id, set()).discard(t))


def http():
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=HARD_TIMEOUT_S)
    return _client


def _rid():
    return uuid.uuid4().hex[:16]


# ---------------- 取数 ----------------

async def call_source(batch_id, sample_id, attempt, req_id, span_id):
    try:
        r = await http().get(f"{SOURCE_URL}/fetch", params={
            "sample_id": sample_id, "request_id": req_id,
            "batch_id": batch_id, "attempt": attempt})
        end = time.time()
        if r.status_code == 200:
            data = r.json()
            db.finish_span(span_id, end, "ok", {"response": data})
            return ("ok", data)
        db.finish_span(span_id, end, "failed",
                       {"status_code": r.status_code, "body": r.text[:200]})
        return ("failed", None)
    except Exception as e:
        db.finish_span(span_id, time.time(), "failed",
                       {"error": f"{type(e).__name__}: {e}"})
        return ("failed", None)


async def fetch_with_retry(batch_id, sample_id, cfg):
    """首个有效结果胜出。超时只放弃等待、不取消旧请求：
    尝试次数用完后仍在途的旧请求继续等待收尾(可能反超成为有效尝试)，
    其余迟到完成的记为 late，结果绝不参与入仓。"""
    timeout_s = cfg["client_timeout_ms"] / 1000.0
    max_attempts = int(cfg["max_attempts"])
    pending = {}   # task -> {attempt, req_id, span_id}
    state = {"winner": None, "winner_req": None}

    def collect(done):
        for t in done:
            meta = pending.pop(t)
            status, data = t.result()
            if status == "ok" and state["winner"] is None:
                state["winner"], state["winner_req"] = data, meta["req_id"]
                db.mark_status(meta["span_id"], "effective", note=" 该尝试结果被采用。")
            elif status == "ok":
                db.mark_status(meta["span_id"], "late", note=" 完成时已有有效结果，迟到未采用。")

    attempt = 0
    # 发起阶段：逻辑超时即追加新尝试，旧尝试继续运行
    while state["winner"] is None and attempt < max_attempts:
        attempt += 1
        req_id = _rid()
        span_id = db.insert_span(batch_id, sample_id, "fetch", "client", "orchestrator",
                                 attempt, req_id, "running", time.time())
        task = asyncio.create_task(call_source(batch_id, sample_id, attempt, req_id, span_id))
        pending[task] = {"attempt": attempt, "req_id": req_id, "span_id": span_id}
        done, _ = await asyncio.wait(set(pending), timeout=timeout_s,
                                     return_when=asyncio.FIRST_COMPLETED)
        collect(done)
        if state["winner"] is None and not done:
            # 逻辑超时：标记客户端放弃，但任务继续运行(源站可能仍在执行)
            for t, meta in pending.items():
                db.mark_status(meta["span_id"], "timeout",
                               note=f" 客户端超时(等待超 {cfg['client_timeout_ms']}ms)放弃并发起新尝试；"
                                    f"旧请求可能仍在源站运行。",
                               only_if="running")
    # 收尾阶段：不再发新请求，已在途的尝试仍可能给出首个有效结果
    while state["winner"] is None and pending:
        done, _ = await asyncio.wait(set(pending), timeout=HARD_TIMEOUT_S,
                                     return_when=asyncio.FIRST_COMPLETED)
        if not done:
            break
        collect(done)
    if pending:
        _track_finalizer(batch_id, asyncio.create_task(_finalize_late(dict(pending))))
    if state["winner"] is None:
        return None
    state["winner"]["_req_id"] = state["winner_req"]
    return state["winner"]


async def _finalize_late(pending):
    """胜出/放弃后仍在跑的旧尝试：完成时如实记录为 late，结果不入仓。"""
    for t, meta in pending.items():
        try:
            status, _ = await t
            if status == "ok":
                db.mark_status(meta["span_id"], "late",
                               note=" 迟到完成，结果未采用，不参与入仓。")
        except Exception:
            pass


# ---------------- 质控(两项并行) ----------------

async def qc_call(batch_id, sample_id, check, payload):
    """返回三态：'pass' 明确通过 / 'reject' 明确拒绝(回执 pass=false) /
    'unknown' 未确认(质控报错、HTTP 非 200 或回执缺少 pass 字段)——
    未确认绝不冒充明确拒绝。"""
    stage = f"qc_{check}"
    req_id = _rid()
    span_id = db.insert_span(batch_id, sample_id, stage, "client", "orchestrator",
                             1, req_id, "running", time.time())
    try:
        r = await http().post(f"{QC_URL}/check/{check}",
                              json={**payload, "request_id": req_id, "batch_id": batch_id})
        end = time.time()
        if r.status_code == 200:
            res = r.json()
            if "pass" in res:
                ok = bool(res.get("pass"))
                db.finish_span(span_id, end, "ok" if ok else "failed", {"response": res},
                               note=None if ok else " 质控明确判定未通过。")
                return "pass" if ok else "reject"
            db.finish_span(span_id, end, "failed", {"response": res},
                           note=" 质控回执缺少 pass 字段，结论未确认。")
            return "unknown"
        db.finish_span(span_id, end, "failed",
                       {"status_code": r.status_code, "body": r.text[:200]},
                       note=" 质控服务报错，结论未确认。")
        return "unknown"
    except Exception as e:
        db.finish_span(span_id, time.time(), "failed",
                       {"error": f"{type(e).__name__}: {e}"},
                       note=" 质控调用异常，结论未确认。")
        return "unknown"


# ---------------- 单样品流程 ----------------

async def process_sample(batch_id, sample_id, cfg):
    data = await fetch_with_retry(batch_id, sample_id, cfg)
    if data is None:
        return False
    req_id_used = data.pop("_req_id")
    returned_id = str(data.get("sample_id"))
    payload = {"sample_id": returned_id, "expected_sample_id": sample_id,
               "concentration": data.get("concentration")}
    # 两项质控并行：墙钟取较慢者，不相加；明确通过才算通过
    ok1, ok2 = await asyncio.gather(
        qc_call(batch_id, sample_id, "nonneg", payload),
        qc_call(batch_id, sample_id, "match", payload))
    if ok1 != "pass" or ok2 != "pass":
        return False
    span_id = db.insert_span(batch_id, sample_id, "warehouse", "client", "orchestrator",
                             1, _rid(), "running", time.time())
    inserted = db.warehouse_insert(batch_id, sample_id, data.get("concentration"), req_id_used)
    db.finish_span(span_id, time.time(), "ok",
                   {"inserted": inserted, "concentration": data.get("concentration"),
                    "source_request_id": req_id_used},
                   note=None if inserted else " 唯一约束去重：该样品已有入仓记录，未重复写入。")
    return True


# ---------------- 平行管配对流程 ----------------

def compute_rpd(a, b):
    """RPD = 2|a-b|/(a+b)；两值均为零时约定记 0。两管均已通过非负质控，故 a+b==0 即同为零。"""
    a, b = float(a), float(b)
    if a + b == 0:
        return 0.0
    return 2.0 * abs(a - b) / (a + b)


async def process_pair(batch_id, sample_a, sample_b, rpd_limit, cfg):
    """同一委托的两支平行管成对入仓：
    1) 两管各自沿用取数重试；2) 各走原有两项质控且须均明确通过；
    3) 按 RPD 比较浓度，超限整对不入仓；
    4) 任一环节未确认(取数失败/质控报错/回执不明)记 unconfirmed，不冒充明确拒绝；
    5) 合格时两条原始测值同一事务写入，失败回滚不留半对；
    6) 判定只下一次，迟到取数不得改变已定配对结果。"""
    pair_id = db.pair_create(batch_id, sample_a, sample_b, rpd_limit)
    pair_label = f"{sample_a}+{sample_b}"
    t0 = time.time()
    span_id = db.insert_span(batch_id, pair_label, "pair_compare", "client", "orchestrator",
                             1, _rid(), "running", t0)

    def decide(conc_a, conc_b, rpd, decision, reason, extra=None):
        db.pair_decide(pair_id, conc_a, conc_b, rpd, decision, reason)
        status = {"stored": "ok", "rejected": "failed"}.get(decision, "unconfirmed")
        db.finish_span(span_id, time.time(), status,
                       {"pair_id": pair_id, "sample_a": sample_a, "sample_b": sample_b,
                        "conc_a": conc_a, "conc_b": conc_b, "rpd": rpd,
                        "rpd_limit": rpd_limit, "decision": decision,
                        "reason": reason, **(extra or {})},
                       note=f" 配对结论:{reason}")
        return decision == "stored"

    # 1) 两管并行取数(沿用超时重试；首个有效结果胜出，迟到不入仓)
    data_a, data_b = await asyncio.gather(fetch_with_retry(batch_id, sample_a, cfg),
                                          fetch_with_retry(batch_id, sample_b, cfg))
    if data_a is None or data_b is None:
        missing = [s for s, d in ((sample_a, data_a), (sample_b, data_b)) if d is None]
        return decide(None, None, None, "unconfirmed",
                      f"未确认：管 {'、'.join(missing)} 取数失败(尝试耗尽无有效结果)，"
                      f"非明确拒绝，整对不入仓。")
    req_a, req_b = data_a.pop("_req_id"), data_b.pop("_req_id")
    conc_a, conc_b = data_a.get("concentration"), data_b.get("concentration")

    # 2) 两管 × 两项质控全部并行(沿用原有 nonneg/match 两项)
    def payload(sample_id, data):
        return {"sample_id": str(data.get("sample_id")), "expected_sample_id": sample_id,
                "concentration": data.get("concentration")}
    ra1, ra2, rb1, rb2 = await asyncio.gather(
        qc_call(batch_id, sample_a, "nonneg", payload(sample_a, data_a)),
        qc_call(batch_id, sample_a, "match", payload(sample_a, data_a)),
        qc_call(batch_id, sample_b, "nonneg", payload(sample_b, data_b)),
        qc_call(batch_id, sample_b, "match", payload(sample_b, data_b)))
    results = {f"{sample_a}·质控①非负值": ra1, f"{sample_a}·质控②样品号对应": ra2,
               f"{sample_b}·质控①非负值": rb1, f"{sample_b}·质控②样品号对应": rb2}
    unknown = [k for k, v in results.items() if v == "unknown"]
    if unknown:
        return decide(_num(conc_a), _num(conc_b), None, "unconfirmed",
                      f"未确认：质控报错或回执不明({ '、'.join(unknown) })，"
                      f"不冒充明确拒绝，整对不入仓。")
    rejected = [k for k, v in results.items() if v == "reject"]
    if rejected:
        return decide(_num(conc_a), _num(conc_b), None, "rejected",
                      f"明确拒绝：{ '、'.join(rejected) } 质控判定未通过，整对不入仓。")

    # 3) 两管四项质控均明确通过 → RPD 比较
    rpd = compute_rpd(conc_a, conc_b)
    if rpd > rpd_limit:
        return decide(_num(conc_a), _num(conc_b), round(rpd, 6), "rejected",
                      f"明确拒绝：RPD={rpd:.6f} 超上限 {rpd_limit}"
                      f"(2|{conc_a}-{conc_b}|/({conc_a}+{conc_b}))，整对不入仓。")

    # 4) 合格：两条原始测值同一事务成对入仓，失败回滚不留半对
    w_start = time.time()
    inserted = db.warehouse_insert_pair(batch_id, sample_a, conc_a, req_a,
                                        sample_b, conc_b, req_b)
    if not inserted:
        return decide(_num(conc_a), _num(conc_b), round(rpd, 6), "unconfirmed",
                      "未确认：成对入仓写入失败，事务已整体回滚，未留下半对。")
    end = time.time()
    for sid, conc, req in ((sample_a, conc_a, req_a), (sample_b, conc_b, req_b)):
        wspan = db.insert_span(batch_id, sid, "warehouse", "client", "orchestrator",
                               1, _rid(), "running", w_start)
        db.finish_span(wspan, end, "ok",
                       {"inserted": True, "concentration": conc, "source_request_id": req,
                        "atomic_pair": pair_label},
                       note=" 配对原子写入：两条原始测值同一事务入仓。")
    return decide(_num(conc_a), _num(conc_b), round(rpd, 6), "stored",
                  f"合格：双管四项质控均明确通过，RPD={rpd:.6f} ≤ 上限 {rpd_limit}，"
                  f"两条原始测值已同一事务成对入仓。")


def _num(x):
    """浓度数值化(非数值不入 pairs 浓度列，避免脏数据)。"""
    return x if isinstance(x, (int, float)) and not isinstance(x, bool) else None


async def run_batch(batch_id, sample_ids, mode="normal", rpd_limit=None):
    cfg = db.get_config()  # 批次开始时快照配置，批内一致
    try:
        if mode == "paired":
            # 配对批次：恰两支不同样品号构成一对，成对判定、成对入仓
            await process_pair(batch_id, sample_ids[0], sample_ids[1], rpd_limit, cfg)
        else:
            await asyncio.gather(*(process_sample(batch_id, s, cfg) for s in sample_ids),
                                 return_exceptions=True)
    except Exception:
        pass
    # 等在途的迟到尝试收尾(有 httpx 硬超时兜底)，使定稿结论如实包含迟到记录
    pending = list(_finalizers.get(batch_id, ()))
    if pending:
        await asyncio.wait(pending)
    _finalizers.pop(batch_id, None)
    concl = analysis.analyze(batch_id)
    status = "done" if concl.get("status") == "done" else "failed"
    db.execute("UPDATE batches SET status=?, finished_at=?, conclusion=? WHERE id=?",
               (status, time.time(), json.dumps(concl, ensure_ascii=False), batch_id))
