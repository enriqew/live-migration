"""The gateway: where traffic is switched, and where the cutover pause happens.

Clients (the load generator) talk to the gateway only. `/dublin/...` and `/helsinki/...` are
forwarded to the current upstream of each system: the legacy API before the cutover, the unified
API after it. The cutover is three admin calls:

- `POST /admin/fence`: from now on, requests are held, not refused. The call returns once every
  request already forwarded has finished, so when it returns, nothing is in flight to the old
  upstream and nothing new will reach it.
- `POST /admin/route`: point each system at its new upstream.
- `POST /admin/release`: held requests go to the new upstream, in the order they arrived.

A held request is not an error to the client, only a slower answer. That wait is the pause the
project measures, and the generator measures it from when each request was due.

An upstream that cannot be reached, or does not answer in time, is a 502 or a 504: statuses a
client may retry, and with its idempotency key a retry cannot apply a write twice. The gateway does
not retry on its own; the client owns the key and the deadline.
"""

from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, Response
from pydantic import BaseModel

UPSTREAMS = {
    "dublin": os.environ.get("UPSTREAM_DUBLIN", "http://api-a:8000"),
    "helsinki": os.environ.get("UPSTREAM_HELSINKI", "http://api-b:8000"),
}
HOP_BY_HOP = {"connection", "keep-alive", "transfer-encoding", "content-length", "host"}


class State:
    def __init__(self):
        self.open = asyncio.Event()
        self.open.set()
        self.in_flight = 0
        self.idle = asyncio.Event()
        self.idle.set()
        self.fenced_at: float | None = None
        self.held = 0
        self.held_max = 0


state = State()
client: httpx.AsyncClient


@asynccontextmanager
async def lifespan(app: FastAPI):
    global client
    # uvicorn closes an idle connection after 5 s. A pooled connection kept as long as that may be
    # picked for a request just as the server closes it, and the request dies with a ReadError
    # (4 writes did, in the full run of 2026-11-20). Expiring idle connections at 2 s leaves the
    # pool only connections the server still holds open.
    client = httpx.AsyncClient(
        timeout=30.0,
        limits=httpx.Limits(max_connections=400, keepalive_expiry=2.0),
    )
    yield
    await client.aclose()


app = FastAPI(title="gateway", lifespan=lifespan)


class Route(BaseModel):
    dublin: str | None = None
    helsinki: str | None = None


@app.post("/admin/fence")
async def fence():
    state.open.clear()
    state.fenced_at = time.monotonic()
    started = time.monotonic()
    await state.idle.wait()  # every request forwarded before the fence has finished
    return {"drained_ms": round((time.monotonic() - started) * 1000, 3), "held": state.held}


@app.post("/admin/route")
async def route(body: Route):
    if state.open.is_set():
        return Response('{"detail": "fence first"}', status_code=409, media_type="application/json")
    for system, url in body.model_dump(exclude_none=True).items():
        UPSTREAMS[system] = url
    return dict(UPSTREAMS)


@app.post("/admin/release")
async def release():
    fenced_ms = None
    if state.fenced_at is not None:
        fenced_ms = round((time.monotonic() - state.fenced_at) * 1000, 3)
    held = state.held_max
    state.fenced_at, state.held_max = None, 0
    state.open.set()
    return {"fenced_ms": fenced_ms, "held_max": held, "upstreams": dict(UPSTREAMS)}


@app.get("/admin/state")
async def admin_state():
    return {
        "open": state.open.is_set(),
        "in_flight": state.in_flight,
        "held": state.held,
        "upstreams": dict(UPSTREAMS),
    }


def _upstream_error(status: int, e: Exception) -> Response:
    detail = f"{type(e).__name__}: {e}"[:200].replace('"', "'")
    return Response(f'{{"detail": "{detail}"}}', status_code=status, media_type="application/json")


@app.api_route("/{system}/{path:path}", methods=["GET", "POST", "PATCH", "DELETE"])
async def forward(system: str, path: str, request: Request):
    if system not in UPSTREAMS:
        return Response(status_code=404)
    if not state.open.is_set():
        state.held += 1
        state.held_max = max(state.held_max, state.held)
        try:
            await state.open.wait()
        finally:
            state.held -= 1
    # Counted before the upstream is chosen, so a fence that sees no request in flight cannot
    # miss one about to be sent.
    state.in_flight += 1
    state.idle.clear()
    try:
        upstream = UPSTREAMS[system]
        headers = {k: v for k, v in request.headers.items() if k.lower() not in HOP_BY_HOP}
        try:
            resp = await client.request(
                request.method,
                f"{upstream}/{path}",
                content=await request.body(),
                headers=headers,
                params=request.query_params,
            )
        except httpx.TimeoutException as e:
            return _upstream_error(504, e)
        except httpx.TransportError as e:
            return _upstream_error(502, e)
        out_headers = {k: v for k, v in resp.headers.items() if k.lower() not in HOP_BY_HOP}
        return Response(resp.content, status_code=resp.status_code, headers=out_headers)
    finally:
        state.in_flight -= 1
        if state.in_flight == 0:
            state.idle.set()
