"""诊断网页与编排 API：提交批次、查询批次/时间线、运行时调参并下发到源站/质控。"""
import asyncio
import json
import os
import time

import httpx
from fastapi import Body, FastAPI
from fastapi.responses import FileResponse, JSONResponse

import analysis
import db
import pipeline

SOURCE_URL = os.environ.get("SOURCE_URL", "http://localhost:8001")
QC_URL = os.environ.get("QC_URL", "http://localhost:8002")
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

app = FastAPI(title="样品入仓链路诊断")


@app.get("/")
async def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.post("/api/batches")
async def create_batch(body: dict = Body(...)):
    raw = body.get("sample_ids") or []
    ids = list(dict.fromkeys(str(s).strip() for s in raw if str(s).strip()))
    if not ids:
        return JSONResponse({"error": "样品号清单为空"}, status_code=400)
    cur = db.execute(
        "INSERT INTO batches(created_at, sample_ids, status) VALUES(?,?,?)",
        (time.time(), json.dumps(ids, ensure_ascii=False), "running"))
    bid = cur.lastrowid
    asyncio.create_task(pipeline.run_batch(bid, ids))
    return {"id": bid, "samples": len(ids)}


@app.get("/api/batches")
async def list_batches():
    rows = db.query("SELECT * FROM batches ORDER BY id DESC LIMIT 100")
    out = []
    for b in rows:
        item = {"id": b["id"], "status": b["status"], "created_at": b["created_at"],
                "finished_at": b["finished_at"], "samples": len(json.loads(b["sample_ids"]))}
        if b["conclusion"]:
            c = json.loads(b["conclusion"])
            item.update({"bottleneck": c.get("bottleneck"), "wall_s": c.get("wall_s"),
                         "stored": c.get("samples_stored")})
        out.append(item)
    return out


@app.get("/api/batches/{bid}")
async def batch_detail(bid: int):
    b = db.query_one("SELECT * FROM batches WHERE id=?", (bid,))
    if not b:
        return JSONResponse({"error": "not found"}, status_code=404)
    spans = db.query("SELECT * FROM spans WHERE batch_id=? ORDER BY start_ts,id", (bid,))
    wh = db.query("SELECT * FROM warehouse WHERE batch_id=? ORDER BY stored_at", (bid,))
    # 结论按当前记录实时重算：迟到跨度随时可能补记，看到的一定与记录一致
    concl = analysis.analyze(bid) if spans else None
    return {
        "batch": {"id": b["id"], "status": b["status"], "created_at": b["created_at"],
                  "finished_at": b["finished_at"], "sample_ids": json.loads(b["sample_ids"])},
        "spans": spans,
        "warehouse": wh,
        "conclusion": concl,
    }


@app.post("/api/ingest")
async def ingest(body: dict = Body(...)):
    """接收源站/质控的服务端跨度(含等名额 queue_ms)，按 request_id 与客户端跨度关联。"""
    try:
        db.execute(
            "INSERT INTO spans(batch_id,sample_id,stage,side,service,attempt,request_id,"
            "status,start_ts,end_ts,queue_ms,detail) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (int(body.get("batch_id", 0)), str(body.get("sample_id", "")),
             str(body.get("stage", "")), "server", str(body.get("service", "")),
             int(body.get("attempt", 1)), str(body.get("request_id", "")),
             str(body.get("status", "ok")), float(body.get("start_ts", 0)),
             body.get("end_ts"), body.get("queue_ms"),
             json.dumps(body.get("detail"), ensure_ascii=False)
             if body.get("detail") is not None else None))
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    return {"ok": True}


@app.get("/api/config")
async def get_config():
    return db.get_config()


@app.post("/api/config")
async def set_config(body: dict = Body(...)):
    cfg = db.get_config()
    for k in db.CONFIG_DEFAULTS:
        if k in body:
            cfg[k] = body[k]
    db.save_config(cfg)
    push = await _push_config(cfg)
    return {"config": cfg, "push": push}


async def _push_config(cfg):
    """把运行时参数下发到源站与质控服务；服务不可达时如实返回错误。"""
    out = {}
    async with httpx.AsyncClient(timeout=3) as c:
        try:
            r = await c.post(f"{SOURCE_URL}/config", json={
                "delay_ms": cfg["source_delay_ms"],
                "slots": cfg["source_slots"],
                "fail_first": cfg["source_fail_first"]})
            out["source"] = "ok" if r.status_code == 200 else f"http {r.status_code}"
        except Exception as e:
            out["source"] = f"error: {e}"
        try:
            r = await c.post(f"{QC_URL}/config", json={"checks": {
                "nonneg": {"delay_ms": cfg["qc_nonneg_delay_ms"],
                           "fail_first": cfg["qc_nonneg_fail_first"]},
                "match": {"delay_ms": cfg["qc_match_delay_ms"],
                          "fail_first": cfg["qc_match_fail_first"]}}})
            out["qc"] = "ok" if r.status_code == 200 else f"http {r.status_code}"
        except Exception as e:
            out["qc"] = f"error: {e}"
    return out


@app.get("/api/health")
async def health():
    return {"ok": True}
