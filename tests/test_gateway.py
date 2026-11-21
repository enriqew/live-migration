import asyncio

import httpx

from gateway import app as gw


def run(coro):
    return asyncio.run(coro, loop_factory=asyncio.SelectorEventLoop)


async def scenario():
    seen: list[tuple[str, float]] = []

    async def upstream(request: httpx.Request):
        await asyncio.sleep(0.2)  # a write that takes a while
        seen.append((request.url.host, asyncio.get_running_loop().time()))
        return httpx.Response(200, json={"host": request.url.host})

    gw.state.__init__()
    gw.UPSTREAMS.update(dublin="http://old", helsinki="http://old-b")
    gw.client = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
    transport = httpx.ASGITransport(app=gw.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw") as c:
        loop = asyncio.get_running_loop()
        in_flight = asyncio.create_task(c.post("/dublin/rentals", json={}))
        await asyncio.sleep(0.05)  # it is being forwarded to the old upstream
        fenced = await c.post("/admin/fence")
        fence_returned = loop.time()
        held = [asyncio.create_task(c.post("/dublin/rentals", json={})) for _ in range(3)]
        await asyncio.sleep(0.1)
        state = (await c.get("/admin/state")).json()
        refused = await c.post("/admin/route", json={})  # allowed: fenced
        await c.post("/admin/route", json={"dublin": "http://new"})
        released = (await c.post("/admin/release")).json()
        first = await in_flight
        rest = await asyncio.gather(*held)
    return seen, fence_returned, fenced.json(), state, refused, released, first, rest


def test_fence_drains_holds_and_switches_without_a_failure():
    seen, fence_returned, fenced, state, refused, released, first, rest = run(scenario())
    # The request already forwarded finished on the old upstream before the fence returned.
    assert first.json() == {"host": "old"}
    assert seen[0][0] == "old" and seen[0][1] <= fence_returned
    # Requests that arrived during the fence waited, then all went to the new upstream.
    assert state["held"] == 3 and not state["open"]
    assert [r.status_code for r in rest] == [200, 200, 200]
    assert {r.json()["host"] for r in rest} == {"new"}
    assert released["held_max"] == 3 and released["fenced_ms"] > 0
    assert refused.status_code == 200


def test_route_is_refused_unless_fenced():
    async def go():
        gw.state.__init__()
        transport = httpx.ASGITransport(app=gw.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://gw") as c:
            return await c.post("/admin/route", json={"dublin": "http://new"})

    assert run(go()).status_code == 409


def test_an_unreachable_upstream_is_a_status_the_client_may_retry():
    async def go(exc):
        def upstream(request: httpx.Request):
            raise exc

        gw.state.__init__()
        gw.UPSTREAMS.update(dublin="http://old")
        gw.client = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
        transport = httpx.ASGITransport(app=gw.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://gw") as c:
            resp = await c.post("/dublin/rentals", json={})
            return resp, gw.state.in_flight

    # A connection the upstream closed as the request was sent: 502, not a 500.
    resp, in_flight = run(go(httpx.ReadError("closed")))
    assert resp.status_code == 502 and "ReadError" in resp.json()["detail"]
    assert in_flight == 0
    resp, _ = run(go(httpx.ReadTimeout("slow")))
    assert resp.status_code == 504
