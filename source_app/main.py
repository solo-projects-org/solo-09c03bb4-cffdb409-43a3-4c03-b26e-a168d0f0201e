"""检验源服务：返回样品号与浓度。支持运行时配置延迟、并行名额(槽位)、首次失败。
客户端超时断开后，已受理的请求仍继续执行并向编排端上报服务端跨度(迟到证据)。"""
import asyncio
import hashlib
import os
import time

import httpx
from fastapi import FastAPI, HTTPException

ORCH_URL = os.environ.get("ORCH_URL", "http://localhost:8000")

app = FastAPI(title="检验源")

CFG = {
    "delay_ms": int(os.environ.get("SOURCE_DELAY_MS", "300")),
    "slots": int(os.environ.get("SOURCE_SLOTS", "3")),
    "fail_first": os.environ.get("SOURCE_FAIL_FIRST", "0") == "1",
}
_seen = set()  # fail_first 已触发过的样品号


class SlotGate:
    """并行名额(槽位)：超出名额的请求排队等名额，等待时长计入 queue_ms。"""

    def __init__(self, n):
        self.n = n
        self.cur = 0
        self.cond = asyncio.Condition()

    async def acquire(self):
        async with self.cond:
            while self.cur >= self.n:
                await self.cond.wait()
            self.cur += 1

    async def release(self):
        async with self.cond:
            self.cur -= 1
            self.cond.notify()


gate = SlotGate(CFG["slots"])


def concentration_for(sid: str) -> float:
    h = int(hashlib.md5(sid.encode()).hexdigest()[:8], 16)
    val = round((h % 1000) / 100.0, 2)  # 0.00 .. 9.99
    if "NEG" in sid:  # 演示钩子：负浓度触发质控①不通过
        val = -(val + 0.5)
    return val


async def report(span):
    try:
        async with httpx.AsyncClient(timeout=2) as c:
            await c.post(f"{ORCH_URL}/api/ingest", json=span)
    except Exception:
        pass  # 编排端不可达不影响源站自身工作


class InjectedFailure(Exception):
    pass


async def do_work(sample_id, request_id, batch_id, attempt):
    start = time.time()
    delay_ms = CFG["delay_ms"]   # 按请求受理时的配置执行，避免运行中被改配置导致记录失真
    slots = gate.n
    tq = time.time()
    await gate.acquire()
    queue_ms = (time.time() - tq) * 1000.0
    status, payload, err = "ok", None, None
    try:
        await asyncio.sleep(delay_ms / 1000.0)
        if CFG["fail_first"] and sample_id not in _seen:
            _seen.add(sample_id)
            status, err = "failed", "injected first failure (fail_first)"
        else:
            actual = sample_id + "-X" if "MIX" in sample_id else sample_id  # 演示钩子：错位样品号
            payload = {"sample_id": actual, "concentration": concentration_for(sample_id)}
    finally:
        await gate.release()
    end = time.time()
    await report({"batch_id": batch_id, "sample_id": sample_id, "stage": "fetch",
                  "service": "source", "attempt": attempt, "request_id": request_id,
                  "status": status, "start_ts": start, "end_ts": end,
                  "queue_ms": round(queue_ms, 1),
                  "detail": {"delay_ms": delay_ms, "slots": slots,
                             **({"error": err} if err else {})}})
    if err:
        raise InjectedFailure(err)
    return payload


@app.get("/fetch")
async def fetch(sample_id: str, request_id: str = "", batch_id: int = 0, attempt: int = 1):
    task = asyncio.create_task(do_work(sample_id, request_id, batch_id, attempt))
    try:
        return await asyncio.shield(task)
    except InjectedFailure as e:
        raise HTTPException(status_code=500, detail=str(e))
    except asyncio.CancelledError:
        # 客户端已放弃：任务继续跑完并上报跨度，结果成为"迟到结果"
        task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
        raise


@app.post("/config")
async def set_config(body: dict):
    if "delay_ms" in body:
        CFG["delay_ms"] = max(0, int(body["delay_ms"]))
    if "slots" in body:
        CFG["slots"] = max(1, int(body["slots"]))
        gate.n = CFG["slots"]
    if "fail_first" in body:
        CFG["fail_first"] = bool(body["fail_first"])
    _seen.clear()
    return CFG


@app.get("/config")
async def get_config():
    return CFG


@app.get("/health")
async def health():
    return {"ok": True}
