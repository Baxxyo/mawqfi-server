"""
Mawqfi test server
- يخدم صفحة الويب (static/index.html)
- يخزن: مخطط الموقف، البصمات، تقارير الركن/المغادرة، نتائج الاختبار
- يبث كل تغيير لجميع الأجهزة لحظياً (SSE) مع مزامنة احتياطية بالـpolling
تشغيل محلي:  uvicorn server:app --host 0.0.0.0 --port 8000
"""
import asyncio
import json
import os
import re
import sqlite3
import threading
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from typing import Literal, Optional

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

BASE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("DB_PATH", os.path.join(BASE, "mawqfi.db"))
ADMIN_KEY = os.environ.get("ADMIN_KEY", "")  # اختياري: لو انحط، مسح بيانات الجميع يحتاجه

SPOT_RE = r"^P\d{1,4}$"
UID_RE = r"^[A-Za-z0-9_\-]{4,40}$"

lock = threading.Lock()
db = sqlite3.connect(DB_PATH, check_same_thread=False)
db.row_factory = sqlite3.Row
db.executescript(
    """
CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS reports(
  spot TEXT, uid TEXT, name TEXT, state TEXT, src TEXT, conf REAL, ts REAL,
  PRIMARY KEY(spot, uid));
CREATE TABLE IF NOT EXISTS fps(
  spot TEXT, uid TEXT, name TEXT, lat REAL, lng REAL, acc REAL, v INTEGER, ts REAL,
  PRIMARY KEY(spot, uid));
CREATE TABLE IF NOT EXISTS tests(
  id INTEGER PRIMARY KEY AUTOINCREMENT, uid TEXT, name TEXT, ts REAL,
  lat REAL, lng REAL, acc REAL, pred TEXT, conf REAL, method TEXT, truth TEXT);
"""
)
db.commit()


def q(sql, args=()):
    with lock:
        cur = db.execute(sql, args)
        rows = [dict(r) for r in cur.fetchall()]
        db.commit()
        return rows


def x(sql, args=()):
    with lock:
        cur = db.execute(sql, args)
        db.commit()
        return cur.lastrowid


# ---------------------------------------------------------------- realtime hub
class Hub:
    def __init__(self):
        self.subs: set[asyncio.Queue] = set()

    def subscribe(self):
        qu = asyncio.Queue(maxsize=300)
        self.subs.add(qu)
        return qu

    def unsubscribe(self, qu):
        self.subs.discard(qu)

    def publish(self, t, d):
        msg = json.dumps({"t": t, "d": d}, ensure_ascii=False)
        for qu in list(self.subs):
            try:
                qu.put_nowait(msg)
            except asyncio.QueueFull:
                self.subs.discard(qu)  # العميل يرجع يتزامن لما يعيد الاتصال


hub = Hub()
presence: dict[str, dict] = {}
last_presence_push = 0.0
rate = defaultdict(lambda: deque(maxlen=60))


def presence_list():
    now = time.time()
    return [
        {"uid": u, "name": p["name"], "mode": p["mode"], "st": p["st"], "spot": p["spot"],
         "sensing": p["sensing"], "ago": round(now - p["seen"], 1)}
        for u, p in presence.items()
        if now - p["seen"] < 20
    ]


def push_presence(force=False):
    global last_presence_push
    now = time.time()
    if force or now - last_presence_push >= 1.0:
        last_presence_push = now
        hub.publish("presence", presence_list())


@asynccontextmanager
async def lifespan(app):
    async def sweeper():
        while True:
            await asyncio.sleep(5)
            for u in [u for u, p in presence.items() if time.time() - p["seen"] > 60]:
                presence.pop(u, None)
            push_presence(force=True)

    task = asyncio.create_task(sweeper())
    yield
    task.cancel()


app = FastAPI(title="Mawqfi", lifespan=lifespan)


def limit(uid: str, per_min=40):
    now = time.time()
    dq = rate[uid]
    while dq and now - dq[0] > 60:
        dq.popleft()
    if len(dq) >= per_min:
        raise HTTPException(429, "طلبات كثيرة، جرّب بعد شوي")
    dq.append(now)


def clean_name(n: str) -> str:
    return re.sub(r"[<>&\"']", "", (n or "").strip())[:24] or "مجهولة"


# ---------------------------------------------------------------- models
class LotIn(BaseModel):
    lat: float = Field(ge=-90, le=90)
    lng: float = Field(ge=-180, le=180)
    rot: float = Field(ge=-360, le=360)
    rows: int = Field(ge=1, le=20)
    sides: Literal["both", "left", "right"]
    spotW: float = Field(ge=2.0, le=4.0)
    spotL: float = Field(ge=3.5, le=7.0)
    aisle: float = Field(ge=2.5, le=14.0)
    start: int = Field(default=101, ge=1, le=9000)
    by: Optional[str] = None


class ReportIn(BaseModel):
    spot: str = Field(pattern=SPOT_RE)
    uid: str = Field(pattern=UID_RE)
    name: str = ""
    state: Literal["free", "occupied"]
    src: Literal["sdk", "manual"] = "sdk"
    conf: float = Field(ge=0, le=1)


class FpIn(BaseModel):
    spot: str = Field(pattern=SPOT_RE)
    uid: str = Field(pattern=UID_RE)
    name: str = ""
    lat: float = Field(ge=-90, le=90)
    lng: float = Field(ge=-180, le=180)
    acc: float = Field(ge=0, le=500)


class TestIn(BaseModel):
    uid: str = Field(pattern=UID_RE)
    name: str = ""
    lat: float = Field(ge=-90, le=90)
    lng: float = Field(ge=-180, le=180)
    acc: float = Field(ge=0, le=500)
    pred: str = Field(pattern=SPOT_RE)
    conf: float = Field(ge=0, le=1)
    method: Literal["geo", "fp", "mixed"]
    truth: str = Field(pattern=SPOT_RE)


class PingIn(BaseModel):
    uid: str = Field(pattern=UID_RE)
    name: str = ""
    mode: Literal["sim", "real"] = "real"
    st: str = "OUT"
    spot: Optional[str] = Field(default=None, pattern=SPOT_RE)
    sensing: bool = False


class ResetIn(BaseModel):
    scope: Literal["reports", "fps", "tests", "all"]


# ---------------------------------------------------------------- helpers
def get_lot():
    r = q("SELECT v FROM kv WHERE k='lot'")
    return json.loads(r[0]["v"]) if r else None


# ---------------------------------------------------------------- API
@app.get("/api/health")
def health():
    return {"ok": True, "now": time.time()}


@app.get("/api/state")
def state():
    return {
        "now": time.time(),
        "lot": get_lot(),
        "reports": q("SELECT * FROM reports"),
        "fps": q("SELECT * FROM fps"),
        "tests": q("SELECT * FROM tests ORDER BY id DESC LIMIT 500"),
        "presence": presence_list(),
    }


@app.post("/api/lot")
def set_lot(b: LotIn):
    old = get_lot()
    lot = b.model_dump()
    lot["rev"] = (old["rev"] + 1) if old else 1
    lot["ts"] = time.time()
    with lock:
        db.execute("INSERT OR REPLACE INTO kv(k,v) VALUES('lot',?)", (json.dumps(lot),))
        db.commit()
    hub.publish("lot", lot)
    return {"ok": True, "lot": lot}


@app.post("/api/report")
def report(b: ReportIn):
    limit(b.uid)
    row = {"spot": b.spot, "uid": b.uid, "name": clean_name(b.name), "state": b.state,
           "src": b.src, "conf": b.conf, "ts": time.time()}
    x("INSERT OR REPLACE INTO reports(spot,uid,name,state,src,conf,ts) VALUES(:spot,:uid,:name,:state,:src,:conf,:ts)", row)
    hub.publish("report", row)
    return {"ok": True, "row": row}


@app.delete("/api/report")
def del_report(spot: str = Query(pattern=SPOT_RE), uid: str = Query(pattern=UID_RE)):
    x("DELETE FROM reports WHERE spot=? AND uid=?", (spot, uid))
    hub.publish("report_del", {"spot": spot, "uid": uid})
    return {"ok": True}


@app.post("/api/fingerprint")
def fingerprint(b: FpIn):
    limit(b.uid)
    old = q("SELECT * FROM fps WHERE spot=? AND uid=?", (b.spot, b.uid))
    if old:
        o = old[0]
        v0 = min(o["v"], 7)
        lat = (o["lat"] * v0 + b.lat) / (v0 + 1)
        lng = (o["lng"] * v0 + b.lng) / (v0 + 1)
        row = {"spot": b.spot, "uid": b.uid, "name": clean_name(b.name), "lat": lat, "lng": lng,
               "acc": b.acc, "v": v0 + 1, "ts": time.time()}
    else:
        row = {"spot": b.spot, "uid": b.uid, "name": clean_name(b.name), "lat": b.lat, "lng": b.lng,
               "acc": b.acc, "v": 1, "ts": time.time()}
    x("INSERT OR REPLACE INTO fps(spot,uid,name,lat,lng,acc,v,ts) VALUES(:spot,:uid,:name,:lat,:lng,:acc,:v,:ts)", row)
    hub.publish("fp", row)
    return {"ok": True, "row": row}


@app.delete("/api/fingerprint")
def del_fingerprint(uid: str = Query(pattern=UID_RE), spot: Optional[str] = Query(default=None, pattern=SPOT_RE)):
    if spot:
        x("DELETE FROM fps WHERE uid=? AND spot=?", (uid, spot))
    else:
        x("DELETE FROM fps WHERE uid=?", (uid,))
    hub.publish("fp_del", {"uid": uid, "spot": spot})
    return {"ok": True}


@app.post("/api/test")
def add_test(b: TestIn):
    limit(b.uid)
    row = b.model_dump()
    row["name"] = clean_name(b.name)
    row["ts"] = time.time()
    row["id"] = x(
        "INSERT INTO tests(uid,name,ts,lat,lng,acc,pred,conf,method,truth) VALUES(:uid,:name,:ts,:lat,:lng,:acc,:pred,:conf,:method,:truth)",
        row,
    )
    hub.publish("test", row)
    return {"ok": True, "row": row}


@app.post("/api/ping")
def ping(b: PingIn):
    presence[b.uid] = {"name": clean_name(b.name), "mode": b.mode, "st": b.st[:12],
                       "spot": b.spot, "sensing": b.sensing, "seen": time.time()}
    push_presence()
    return {"ok": True, "now": time.time()}


@app.post("/api/reset")
def reset(b: ResetIn, x_admin_key: str = Header(default="")):
    if ADMIN_KEY and x_admin_key != ADMIN_KEY:
        raise HTTPException(403, "يتطلب مفتاح المشرفة")
    tables = {"reports": ["reports"], "fps": ["fps"], "tests": ["tests"], "all": ["reports", "fps", "tests"]}[b.scope]
    for t in tables:
        x(f"DELETE FROM {t}")
    hub.publish("reset", {"scope": b.scope})
    return {"ok": True}


@app.get("/api/stream")
async def stream(request: Request):
    qu = hub.subscribe()

    async def gen():
        try:
            yield "retry: 3000\n\n"
            yield f"data: {json.dumps({'t': 'hello', 'd': {'now': time.time()}})}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(qu.get(), timeout=15)
                except asyncio.TimeoutError:
                    yield ": ping\n\n"
                    continue
                yield f"data: {msg}\n\n"
        finally:
            hub.unsubscribe(qu)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/")
def index():
    return FileResponse(os.path.join(BASE, "static", "index.html"), headers={"Cache-Control": "no-cache"})


app.mount("/", StaticFiles(directory=os.path.join(BASE, "static")), name="static")
