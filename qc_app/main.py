"""质控服务：两项独立检查 —— ①浓度非负 ②返回样品号与委托样品号对应。
每项可单独配置延迟与首次失败；每次检查向编排端上报服务端跨度。"""
import asyncio
import os
import time

import httpx
from fastapi import Body, FastAPI, HTTPException

ORCH_URL = os.environ.get("ORCH_URL", "http://localhost:8000")

app = FastAPI(title="质控服务")

CHECKS = {
    "nonneg": {"delay_ms": int(os.environ.get("QC_NONNEG_DELAY_MS", "200")),
               "fail_first": os.environ.get("QC_NONNEG_FAIL_FIRST", "0") == "1"},
    "match": {"delay_ms": int(os.environ.get("QC_MATCH_DELAY_MS", "200")),
              "fail_first": os.environ.get("QC_MATCH_FAIL_FIRST", "0") == "1"},
}
_seen = {"nonneg": set(), "match": set()}


async def report(span):
    try:
        async with httpx.AsyncClient(timeout=2) as c:
            await c.post(f"{ORCH_URL}/api/ingest", json=span)
    except Exception:
        pass


@app.post("/check/{check}")
async def check(check: str, body: dict = Body(...)):
    if check not in CHECKS:
        raise HTTPException(404, f"unknown check: {check}")
    cfg = CHECKS[check]
    sample_id = str(body.get("sample_id", ""))
    start = time.time()
    await asyncio.sleep(cfg["delay_ms"] / 1000.0)
    if cfg["fail_first"] and sample_id not in _seen[check]:
        _seen[check].add(sample_id)
        await report({"batch_id": body.get("batch_id", 0), "sample_id": sample_id,
                      "stage": f"qc_{check}", "service": "qc", "attempt": 1,
                      "request_id": body.get("request_id", ""), "status": "failed",
                      "start_ts": start, "end_ts": time.time(), "queue_ms": 0,
                      "detail": {"error": f"injected first failure ({check})"}})
        raise HTTPException(500, f"injected first failure ({check})")
    if check == "nonneg":
        conc = body.get("concentration")
        passed = isinstance(conc, (int, float)) and conc >= 0
        detail = {"concentration": conc, "rule": "concentration >= 0"}
    else:
        expected = str(body.get("expected_sample_id", ""))
        passed = sample_id == expected
        detail = {"returned": sample_id, "expected": expected, "rule": "returned == expected"}
    await report({"batch_id": body.get("batch_id", 0), "sample_id": sample_id,
                  "stage": f"qc_{check}", "service": "qc", "attempt": 1,
                  "request_id": body.get("request_id", ""), "status": "ok",
                  "start_ts": start, "end_ts": time.time(), "queue_ms": 0,
                  "detail": {**detail, "pass": passed}})
    return {"pass": passed, "check": check, **detail}


@app.post("/config")
async def set_config(body: dict = Body(...)):
    for name, c in (body.get("checks") or {}).items():
        if name in CHECKS:
            if "delay_ms" in c:
                CHECKS[name]["delay_ms"] = max(0, int(c["delay_ms"]))
            if "fail_first" in c:
                CHECKS[name]["fail_first"] = bool(c["fail_first"])
            _seen[name].clear()
    return CHECKS


@app.get("/config")
async def get_config():
    return CHECKS


@app.get("/health")
async def health():
    return {"ok": True}
