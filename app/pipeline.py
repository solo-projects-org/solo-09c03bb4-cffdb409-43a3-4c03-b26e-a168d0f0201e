"""批次流水线：取数(超时重试/迟到归类) -> 并行两项质控 -> 幂等入仓。
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
            ok = bool(res.get("pass"))
            db.finish_span(span_id, end, "ok" if ok else "failed", {"response": res},
                           note=None if ok else " 质控判定未通过。")
            return ok
        db.finish_span(span_id, end, "failed",
                       {"status_code": r.status_code, "body": r.text[:200]})
        return False
    except Exception as e:
        db.finish_span(span_id, time.time(), "failed",
                       {"error": f"{type(e).__name__}: {e}"})
        return False


# ---------------- 单样品流程 ----------------

async def process_sample(batch_id, sample_id, cfg):
    data = await fetch_with_retry(batch_id, sample_id, cfg)
    if data is None:
        return False
    req_id_used = data.pop("_req_id")
    returned_id = str(data.get("sample_id"))
    payload = {"sample_id": returned_id, "expected_sample_id": sample_id,
               "concentration": data.get("concentration")}
    # 两项质控并行：墙钟取较慢者，不相加
    ok1, ok2 = await asyncio.gather(
        qc_call(batch_id, sample_id, "nonneg", payload),
        qc_call(batch_id, sample_id, "match", payload))
    if not (ok1 and ok2):
        return False
    span_id = db.insert_span(batch_id, sample_id, "warehouse", "client", "orchestrator",
                             1, _rid(), "running", time.time())
    inserted = db.warehouse_insert(batch_id, sample_id, data.get("concentration"), req_id_used)
    db.finish_span(span_id, time.time(), "ok",
                   {"inserted": inserted, "concentration": data.get("concentration"),
                    "source_request_id": req_id_used},
                   note=None if inserted else " 唯一约束去重：该样品已有入仓记录，未重复写入。")
    return True


async def run_batch(batch_id, sample_ids):
    cfg = db.get_config()  # 批次开始时快照配置，批内一致
    try:
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
