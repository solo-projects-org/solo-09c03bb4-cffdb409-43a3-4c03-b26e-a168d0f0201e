"""批次结论分析：关键链(决定完成时间的等待与执行)、瓶颈判定、失败阻断定位。
所有结论只依据 spans(真实调用) 与 warehouse(真实入仓记录)。"""
import json

import db

STAGE_LABELS = {
    "fetch": "取数",
    "qc_nonneg": "质控①非负值",
    "qc_match": "质控②样品号对应",
    "warehouse": "入仓",
}


def _f(x):
    return round(float(x), 3)


def _err_of(span):
    d = span.get("detail")
    if d:
        try:
            j = json.loads(d)
            if isinstance(j, dict):
                if j.get("error"):
                    return str(j["error"])
                if j.get("status_code"):
                    return f"HTTP {j['status_code']}"
                if j.get("response") is not None:
                    return json.dumps(j["response"], ensure_ascii=False)
        except Exception:
            pass
    return span["status"]


def analyze(batch_id):
    batch = db.query_one("SELECT * FROM batches WHERE id=?", (batch_id,))
    if not batch:
        return {"status": "failed", "text": "批次不存在"}
    sample_ids = list(dict.fromkeys(json.loads(batch["sample_ids"])))
    spans = db.query("SELECT * FROM spans WHERE batch_id=? ORDER BY start_ts,id", (batch_id,))
    wh = db.query("SELECT * FROM warehouse WHERE batch_id=?", (batch_id,))
    client = [s for s in spans if s["side"] == "client"]
    server_by_req = {s["request_id"]: s for s in spans if s["side"] == "server"}

    t0 = min([s["start_ts"] for s in spans], default=batch["created_at"])
    ends = [s["end_ts"] for s in spans if s["end_ts"]] + [w["stored_at"] for w in wh]
    t1 = max(ends) if ends else t0
    wh_by_sample = {w["sample_id"]: w for w in wh}
    done = bool(sample_ids) and all(s in wh_by_sample for s in sample_ids)

    # 各阶段统计：sum(各次耗时求和) 与 wall(墙钟跨度) 分开，并行/重叠不相加
    per_stage = {}
    for st in ("fetch", "qc_nonneg", "qc_match", "warehouse"):
        ss = [s for s in client if s["stage"] == st and s["end_ts"] is not None]
        if not ss:
            continue
        per_stage[st] = {
            "label": STAGE_LABELS[st],
            "attempts": len(ss),
            "sum_s": _f(sum(s["end_ts"] - s["start_ts"] for s in ss)),
            "wall_s": _f(max(s["end_ts"] for s in ss) - min(s["start_ts"] for s in ss)),
            "effective": sum(1 for s in ss if s["status"] == "effective"),
            "ok": sum(1 for s in ss if s["status"] == "ok"),
            "failed": sum(1 for s in ss if s["status"] == "failed"),
            "timeout": sum(1 for s in ss if s["status"] == "timeout"),
            "late": sum(1 for s in ss if s["status"] == "late"),
            # 超时放弃是经历而非最终状态(之后会转为有效/迟到/失败)，从备注中统计
            "gave_up": sum(1 for s in ss if "超时" in (s["note"] or "")),
        }

    # 工作量归因：等名额(源站服务端上报的槽位等待) / 源站执行 / 两项质控 / 入仓
    eff_fetch = [s for s in client if s["stage"] == "fetch" and s["status"] == "effective"]
    slot_wait = sum((server_by_req.get(s["request_id"]) or {}).get("queue_ms") or 0
                    for s in eff_fetch) / 1000.0
    fetch_sum = sum(s["end_ts"] - s["start_ts"] for s in eff_fetch)
    parts = {
        "等名额(源站槽位)": slot_wait,
        "源站取数执行": max(fetch_sum - slot_wait, 0.0),
        "质控①非负值": per_stage.get("qc_nonneg", {}).get("sum_s", 0.0),
        "质控②样品号对应": per_stage.get("qc_match", {}).get("sum_s", 0.0),
        "入仓": per_stage.get("warehouse", {}).get("sum_s", 0.0),
    }
    bottleneck = max(parts, key=lambda k: parts[k]) if parts else None

    result = {
        "status": "done" if done else "failed",
        "t0": t0, "t1": t1, "wall_s": _f(t1 - t0),
        "samples_total": len(sample_ids),
        "samples_stored": len(wh_by_sample),
        "warehouse_rows": len(wh),
        "client_calls": len(client),
        "per_stage": per_stage,
        "work_parts_s": {k: _f(v) for k, v in parts.items()},
        "bottleneck": bottleneck,
        "chain": None,
        "blocking": None,
    }

    if done:
        # 关键链：最后完成入仓的样品决定了批次完成时间
        last_w = max(wh, key=lambda w: w["stored_at"])
        sid = last_w["sample_id"]
        sps = [s for s in client if s["sample_id"] == sid]
        fe = next((s for s in sps if s["stage"] == "fetch" and s["status"] == "effective"), None)
        wh_span = next((s for s in sps if s["stage"] == "warehouse" and s["end_ts"]), None)
        qcs = [s for s in sps if s["stage"] in ("qc_nonneg", "qc_match") and s["end_ts"]]
        segs = []
        if fe and wh_span:
            srv = server_by_req.get(fe["request_id"])
            note = ""
            if srv and srv.get("queue_ms"):
                note = f"含等名额 {srv['queue_ms']:.0f}ms(源站服务端记录)"
            segs.append({"kind": "exec", "label": f"取数·尝试#{fe['attempt']}(有效)",
                         "start": fe["start_ts"], "end": fe["end_ts"],
                         "span_ids": [fe["id"]], "note": note})
            prev = fe["end_ts"]
            if qcs:
                qs, qe = min(s["start_ts"] for s in qcs), max(s["end_ts"] for s in qcs)
                if qs - prev > 0.0005:
                    segs.append({"kind": "wait", "label": "调度间隙",
                                 "start": prev, "end": qs, "span_ids": []})
                slower = max(qcs, key=lambda s: s["end_ts"] - s["start_ts"])
                segs.append({"kind": "exec",
                             "label": f"并行质控×{len(qcs)}(较慢:{STAGE_LABELS[slower['stage']]})",
                             "start": qs, "end": qe,
                             "span_ids": [s["id"] for s in qcs],
                             "note": "两项并行，耗时取较慢者，不相加"})
                prev = qe
            if wh_span["start_ts"] - prev > 0.0005:
                segs.append({"kind": "wait", "label": "调度间隙",
                             "start": prev, "end": wh_span["start_ts"], "span_ids": []})
            segs.append({"kind": "exec", "label": "入仓",
                         "start": wh_span["start_ts"], "end": wh_span["end_ts"],
                         "span_ids": [wh_span["id"]], "note": f"入仓记录#{last_w['id']}"})
            result["chain"] = {"sample_id": sid, "segments": segs,
                               "total_s": _f(wh_span["end_ts"] - fe["start_ts"])}
        bits = []
        for seg in segs:
            d = _f(seg["end"] - seg["start"])
            bits.append(f"{seg['label']} {d:.3f}s" + (f"({seg['note']})" if seg.get("note") else ""))
        fr = per_stage.get("fetch", {})
        result["text"] = (
            f"批次#{batch_id} 已完成:{len(wh_by_sample)}/{len(sample_ids)} 样品入仓"
            f"(入仓记录 {len(wh)} 条、跨服务调用 {len(client)} 次,结论以实际记录为据)。"
            f"墙钟总耗时 {_f(t1 - t0)}s。"
            f"决定完成时间的关键链(样品 {sid}): " + " → ".join(bits) + "。"
            f"瓶颈判定: {bottleneck}(各环节工作量秒: "
            + "、".join(f"{k}={v}" for k, v in result["work_parts_s"].items())
            + ";并行质控与重叠重试按墙钟计,不直接相加)。"
            f"取数尝试 {fr.get('attempts', 0)} 次:有效 {fr.get('effective', 0)}、"
            f"失败 {fr.get('failed', 0)}、迟到 {fr.get('late', 0)}"
            f"(其中曾超时放弃 {fr.get('gave_up', 0)} 次)。")
    else:
        blocking = []
        for sid in sample_ids:
            if sid in wh_by_sample:
                continue
            sps = [s for s in client if s["sample_id"] == sid]
            fetches = [s for s in sps if s["stage"] == "fetch"]
            eff = [s for s in fetches if s["status"] == "effective"]
            if fetches and not eff:
                last = max(fetches, key=lambda s: s["start_ts"])
                tries = "、".join(f"#{s['attempt']}:{s['status']}"
                                 for s in sorted(fetches, key=lambda x: x["attempt"]))
                blocking.append({"sample_id": sid, "stage": "fetch",
                                 "detail": f"取数 {len(fetches)} 次尝试均未成功({tries});"
                                           f"最后错误:{_err_of(last)}"})
                continue
            bad_qc = [s for s in sps if s["stage"] in ("qc_nonneg", "qc_match")
                      and s["status"] != "ok"]
            if bad_qc:
                s = bad_qc[0]
                blocking.append({"sample_id": sid, "stage": s["stage"],
                                 "detail": f"{STAGE_LABELS[s['stage']]}未通过/失败:{_err_of(s)}"})
                continue
            if not fetches:
                blocking.append({"sample_id": sid, "stage": "fetch",
                                 "detail": "尚无取数记录(批次可能仍在运行)"})
            else:
                blocking.append({"sample_id": sid, "stage": "warehouse",
                                 "detail": "入仓环节未完成"})
        result["blocking"] = blocking
        bl = "；".join(f"样品 {b['sample_id']} 阻断于{STAGE_LABELS.get(b['stage'], b['stage'])}"
                       f"({b['detail']})" for b in blocking) or "无阻断明细"
        result["text"] = (
            f"批次#{batch_id} 未完成:{len(wh_by_sample)}/{len(sample_ids)} 样品入仓"
            f"(入仓记录 {len(wh)} 条)。阻断环节: {bl}。失败批次不生成成功链。")
    return result
