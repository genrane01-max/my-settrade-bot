# ==============================================================================
# SETTRADE BOT v2 — Watchlist หลายหุ้น + Trailing % + เทรด MP-MTL
# - แต่ละหุ้นมีค่าเอง (บิดหาย%, trailing%) เก็บใน Firebase /watchlist/<SYMBOL>
# - ดึงจำนวนหุ้นจากพอร์ตอัตโนมัติ → เจอเหตุการณ์ขายหมดพอร์ต
# - ทดสอบ Sandbox ก่อน (SETTRADE_APP_CODE=SANDBOX) แล้วค่อยใช้ ALGO
#
# แก้ไข: เพิ่มระบบ "หา method อัตโนมัติ" (_resolve_method) เพราะ SDK settrade_v2
# ใช้ชื่อ method ไม่ตรงกับที่เอกสาร/ตัวอย่างเก่าบอกไว้เป๊ะๆ (เช่น subscribe_bids_offers
# vs subscribe_bid_offer, get_portfolio อาจไม่มีตรงๆ) ตัวช่วยนี้จะลองชื่อที่เป็นไปได้
# หลายแบบ ถ้าไม่เจอเลยจะ log รายชื่อ method จริงที่มีอยู่ใน object นั้นออกมาที่ Render
# logs ให้เห็นเลย จะได้แก้ให้ตรงเป๊ะได้ในทีเดียว
# ==============================================================================

import os
import time
import json
import logging
import threading
from functools import partial
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

import requests
import firebase_admin
from firebase_admin import credentials, db
from settrade_v2 import Investor

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ===================== สถานะ (ในหน่วยความจำ) =====================
lock = threading.Lock()
investor = None
equity = None
realtime = None

state = {
    "enabled": True,          # ปุ่มเปิด-ปิดบอทรวม
    "connected": False,
    "selected": "TTB",        # หุ้นที่กำลังดูจอบิด/ออฟเฟอร์
    "watchlist": {},          # { SYMBOL: {bid_drop_pct, trailing_pct, active} }
    "positions": {},          # { SYMBOL: จำนวนหุ้นที่ถือ }
    "symbols": {},            # { SYMBOL: {bids, offers, last_price, highest, stop, prev_bid1_vol, drop, last_action} }
    "pos_updated": 0,
}
subscribed = set()  # หุ้นที่สตรีมอยู่แล้ว

DEFAULT_CFG = {"bid_drop_pct": 60.0, "trailing_pct": 1.0, "active": True}

# ===================== ตัวช่วยหา method ของ SDK แบบยืดหยุ่น =====================
_warned_missing = set()  # กัน log ซ้ำรัวๆ ต่อ object/label เดิม

def _resolve_method(obj, candidates, label=""):
    """
    ลองหา method จากรายชื่อที่เป็นไปได้ (candidates) ใน obj
    ถ้าเจอ -> คืนค่า method (callable) กลับไปเลย
    ถ้าไม่เจอเลย -> log รายชื่อ method จริงทั้งหมดที่ obj มี (ไม่ขึ้นต้นด้วย _)
                    ออกไปที่ Render logs หนึ่งครั้ง แล้วคืนค่า None
    """
    for name in candidates:
        m = getattr(obj, name, None)
        if callable(m):
            return m
    key = (type(obj).__name__, label)
    if key not in _warned_missing:
        _warned_missing.add(key)
        available = sorted(m for m in dir(obj) if not m.startswith("_"))
        logger.error(
            f"❗ [{label}] ไม่พบ method ที่ลองใน {type(obj).__name__} "
            f"(ลองแล้ว: {candidates}) — method จริงที่มีอยู่คือ: {available}"
        )
    return None

def _call_flexible(method, *args):
    """เรียก method โดยลองแบบไม่มี argument ก่อน ถ้า TypeError ค่อยลองใส่ args"""
    try:
        return method()
    except TypeError:
        return method(*args)

# ===================== FIREBASE =====================
def init_firebase():
    try:
        if not firebase_admin._apps:
            cred = credentials.Certificate("/etc/secrets/firebase.json")
            db_url = os.getenv("FIREBASE_DATABASE_URL")
            firebase_admin.initialize_app(cred, {"databaseURL": db_url})
            logger.info("Firebase Realtime Database Connected!")
    except Exception as e:
        logger.error(f"Firebase Init Error: {e}")

init_firebase()

def load_watchlist():
    """ อ่าน /watchlist จาก Firebase → ถ้าว่าง ใส่ค่าเริ่มต้นจาก env """
    try:
        data = db.reference("watchlist").get() or {}
        wl = {}
        for sym, cfg in data.items():
            if isinstance(cfg, dict):
                wl[sym.upper()] = {
                    "bid_drop_pct": float(cfg.get("bid_drop_pct", DEFAULT_CFG["bid_drop_pct"])),
                    "trailing_pct": float(cfg.get("trailing_pct", DEFAULT_CFG["trailing_pct"])),
                    "active": bool(cfg.get("active", True)),
                }
        if not wl:
            sym = os.getenv("TARGET_SYMBOL", "TTB").upper().strip()
            wl[sym] = dict(DEFAULT_CFG)
            save_watchlist_item(sym, wl[sym])
        return wl
    except Exception as e:
        logger.error(f"load_watchlist error: {e}")
        return {os.getenv("TARGET_SYMBOL", "TTB").upper(): dict(DEFAULT_CFG)}

def save_watchlist_item(symbol, cfg):
    try:
        db.reference(f"watchlist/{symbol.upper()}").set(cfg)
        return True
    except Exception as e:
        logger.error(f"save_watchlist_item error: {e}")
        return False

def remove_watchlist_item(symbol):
    try:
        db.reference(f"watchlist/{symbol.upper()}").delete()
        return True
    except Exception as e:
        logger.error(f"remove_watchlist_item error: {e}")
        return False

# ===================== TELEGRAM =====================
def send_telegram(message):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message},
            timeout=5,
        )
    except Exception as e:
        logger.error(f"Telegram error: {e}")

# ===================== SETTRADE =====================
def init_settrade():
    global investor, equity
    app_id = os.getenv("SETTRADE_APP_ID")
    app_secret = os.getenv("SETTRADE_APP_SECRET")
    broker_id = os.getenv("SETTRADE_BROKER_ID")
    app_code = os.getenv("SETTRADE_APP_CODE", "SANDBOX")
    account_no = os.getenv("SETTRADE_ACCOUNT_N")
    if not app_id or not app_secret:
        logger.warning("ยังไม่มี SETTRADE_APP_ID/SECRET — ต้องรอ key จาก BLS ถึงจะเชื่อมตลาด")
        return False
    try:
        investor = Investor(app_id=app_id, app_secret=app_secret,
                            broker_id=broker_id, app_code=app_code)
        equity = investor.Equity(account_no=account_no)
        with lock:
            state["connected"] = True
        logger.info(f"✅ Settrade เชื่อมต่อ (broker={broker_id}, app_code={app_code})")
        return True
    except Exception as e:
        logger.error(f"Settrade เชื่อมต่อไม่สำเร็จ: {e}")
        return False

def refresh_positions(force=False):
    """ ดึงพอร์ตจาก Settrade ทุก 30 วิ — รู้ว่าถือหุ้นละกี่หุ้น """
    now = time.time()
    if not force and now - state["pos_updated"] < 30:
        return
    if equity is None:
        return
    try:
        get_port = _resolve_method(
            equity,
            ["get_portfolio", "portfolio", "get_port", "getPortfolio", "port"],
            "get_portfolio",
        )
        if get_port is None:
            return

        account_no = os.getenv("SETTRADE_ACCOUNT_N")
        raw = _call_flexible(get_port, account_no)

        # ผลลัพธ์อาจเป็น list ตรงๆ หรือ dict ที่ห่อ list ไว้อีกที — ลองทุกแบบ
        if isinstance(raw, dict):
            items = (
                raw.get("portfolio_list")
                or raw.get("portfolios")
                or raw.get("data")
                or raw.get("results")
                or []
            )
        else:
            items = raw or []

        pos = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            sym = (item.get("symbol") or item.get("security_symbol") or "").upper()
            vol = (
                item.get("volume")
                or item.get("total_volume")
                or item.get("hold_volume")
                or item.get("actual_volume")
                or item.get("start_volume")
                or 0
            )
            if sym:
                pos[sym] = int(vol or 0)

        if not pos and raw:
            # ดึงข้อมูลกลับมาได้ แต่ field ไม่ตรงกับที่เดาไว้ — log ตัวอย่างไว้ดู
            logger.warning(f"[get_portfolio] ได้ raw data กลับมาแต่แปลงเป็น position ไม่ได้: {str(raw)[:500]}")

        with lock:
            state["positions"] = pos
            state["pos_updated"] = now
    except Exception as e:
        logger.error(f"get_portfolio error: {e}")

def place_order(side, symbol, volume, pin, price_type="MP-MTL"):
    """ side='Buy'/'Sell' — MP-MTL เสมอ (กันราคาหลุดไกล) """
    if equity is None:
        return {"ok": False, "msg": "ยังไม่ได้เชื่อมต่อ Settrade"}
    try:
        resp = equity.place_order(
            side=side,
            symbol=symbol.upper().strip(),
            trustee_id_type=os.getenv("SETTRADE_TRUSTEE_ID", "Local"),
            volume=int(volume),
            price_type=price_type,
            price=0.0,
            validity_type="Day",
            pin=pin,
        )
        msg = f"📤 {side} {symbol} {volume} ({price_type})\nตอบ: {resp}"
        logger.info(msg)
        send_telegram(msg)
        return {"ok": True, "msg": str(resp)}
    except Exception as e:
        logger.error(f"place_order error: {e}")
        return {"ok": False, "msg": str(e)}

# ===================== สตรีมข้อมูล =====================
def normalize_book(data):
    rows = []
    for item in (data or [])[:5]:
        if isinstance(item, dict):
            rows.append([item.get("price", 0), item.get("volume", 0)])
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            rows.append([item[0], item[1]])
    return rows

def on_bids_offers(symbol, msg):
    try:
        with lock:
            s = state["symbols"].setdefault(symbol, {})
            s["bids"] = normalize_book(msg.get("bids"))
            s["offers"] = normalize_book(msg.get("offers"))
    except Exception as e:
        logger.error(f"on_bids_offers error: {e}")

def on_price_info(symbol, msg):
    try:
        with lock:
            s = state["symbols"].setdefault(symbol, {})
            s["last_price"] = msg.get("last", 0.0) or msg.get("price", 0.0)
    except Exception as e:
        logger.error(f"on_price_info error: {e}")

def ensure_subscribe(symbols):
    """ สมัครสตรีมหุ้นใหม่ (ถ้ายังไม่ได้สมัคร) — ลองหลายชื่อ method เพราะ SDK แต่ละเวอร์ชันตั้งชื่อไม่ตรงกัน """
    global realtime
    if investor is None or realtime is None:
        return
    for sym in symbols:
        if sym in subscribed:
            continue
        try:
            sub_bid_offer = _resolve_method(
                realtime,
                [
                    "subscribe_bid_offer",
                    "subscribe_bids_offers",
                    "subscribeBidOffer",
                    "subscribe_bidoffer",
                    "subscribe_bid_offers",
                ],
                "subscribe bid/offer",
            )
            sub_price = _resolve_method(
                realtime,
                [
                    "subscribe_price_info",
                    "subscribePriceInfo",
                    "subscribe_price",
                ],
                "subscribe price info",
            )
            ok_any = False
            if sub_bid_offer:
                sub_bid_offer(sym, partial(on_bids_offers, sym))
                ok_any = True
            if sub_price:
                sub_price(sym, partial(on_price_info, sym))
                ok_any = True
            if ok_any:
                subscribed.add(sym)
                logger.info(f"📡 สตรีม {sym} แล้ว")
        except Exception as e:
            logger.error(f"subscribe {sym} error: {e}")

def start_realtime():
    """
    SDK เวอร์ชันนี้ (settrade_v2) ไม่มี method run()/start()/connect() แบบ blocking —
    ยืนยันจาก log จริงว่า RealtimeDataConnection มีแค่ subscribe_bid_offer,
    subscribe_price_info, subscribe_candlestick ฯลฯ เท่านั้น ไม่มีตัวไหนไว้ block รอ event
    แปลว่าแต่ละ subscribe_* เปิด connection/thread เบื้องหลังให้เองตอนเรียกแล้ว
    เราแค่ต้อง keep thread นี้ให้มีชีวิตอยู่เฉยๆ กัน object โดน garbage collect
    """
    global realtime
    try:
        realtime = investor.RealtimeDataConnection()
        wl = load_watchlist()
        ensure_subscribe([s for s, c in wl.items() if c["active"]])
        logger.info("🔌 Realtime subscribe เรียบร้อย (SDK จัดการ connection เบื้องหลังเอง ไม่ต้อง run())")
        while True:
            time.sleep(3600)
    except Exception as e:
        logger.error(f"start_realtime error: {e}")

# ===================== ตรรกะเฝ้าหุ้น =====================
def check_and_autosell():
    with lock:
        if not state["enabled"] or not state["connected"]:
            return
        pin = os.getenv("SETTRADE_PIN")
        for symbol, cfg in state["watchlist"].items():
            if not cfg.get("active", True):
                continue
            s = state["symbols"].get(symbol)
            if not s:
                continue
            threshold = float(cfg.get("bid_drop_pct", 60.0))
            trailing_pct = float(cfg.get("trailing_pct", 1.0))
            held = int(state["positions"].get(symbol, 0) or 0)
            if held <= 0:
                continue  # ไม่ถือหุ้น → ไม่ขาย

            # --- 1) บิดชั้น 1 วอลุ่มหายเกิน % → ขายหมดพอร์ตทันที ---
            bids = s.get("bids") or []
            if bids:
                bid1_price, bid1_vol = bids[0]
                prev = s.get("prev_bid1_vol", 0.0)
                if prev > 0 and bid1_vol < prev:
                    drop = (prev - bid1_vol) / prev * 100
                    s["drop"] = round(drop, 1)
                    if drop >= threshold:
                        msg = (f"🚨 {symbol} บิด {bid1_price} วอลุ่มหาย {drop:.1f}% "
                               f"→ ขาย {held} หุ้น (หมดพอร์ต) ทันที!")
                        logger.warning(msg)
                        s["last_action"] = msg
                        s["prev_bid1_vol"] = 0  # ป้องกันขายซ้ำ
                        send_telegram(msg)
                        place_order("Sell", symbol, held, pin)  # MP-MTL
                        continue
                s["prev_bid1_vol"] = bid1_vol

            # --- 2) Trailing % — จุดขายคำนวณจากราคาสูงสุดแบบเรียลไทม์ ---
            last = s.get("last_price", 0.0)
            if last > 0:
                highest = s.get("highest", 0.0)
                if last > highest:
                    s["highest"] = last
                    s["stop"] = round(last * (1 - trailing_pct / 100.0), 2)
                stop = s.get("stop", 0.0)
                if stop > 0 and last <= stop:
                    msg = (f"🛑 {symbol} ราคา {last} ตกถึงจุดขาย {stop} "
                           f"(สูงสุด {s['highest']} -{trailing_pct}%) → ขาย {held} หุ้น")
                    logger.warning(msg)
                    s["last_action"] = msg
                    s["stop"] = 0  # ป้องกันขายซ้ำ
                    send_telegram(msg)
                    place_order("Sell", symbol, held, pin)  # MP-MTL

def bot_loop():
    while True:
        try:
            wl = load_watchlist()
            with lock:
                state["watchlist"] = wl
            ensure_subscribe([s for s, c in wl.items() if c["active"]])
            refresh_positions()
            check_and_autosell()
        except Exception as e:
            logger.error(f"Bot Loop Error: {e}")
        time.sleep(1)

# ===================== เว็บ DASHBOARD =====================
class DashboardHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def send_json(self, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/api/state":
            with lock:
                sel = state["selected"]
                sd = state["symbols"].get(sel, {})
                payload = {
                    "enabled": state["enabled"],
                    "connected": state["connected"],
                    "selected": sel,
                    "selected_data": {
                        "last_price": sd.get("last_price", 0),
                        "highest": sd.get("highest", 0),
                        "stop": sd.get("stop", 0),
                        "drop": sd.get("drop", 0),
                        "bids": sd.get("bids", []),
                        "offers": sd.get("offers", []),
                        "last_action": sd.get("last_action", "รอข้อมูล..."),
                    },
                    "watchlist": state["watchlist"],
                    "positions": state["positions"],
                }
            self.send_json(payload)
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(HTML.encode("utf-8"))

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length).decode("utf-8")
        try:
            data = json.loads(raw) if raw else {}
        except Exception:
            data = {}
        path = urlparse(self.path).path

        if path == "/api/toggle":
            with lock:
                state["enabled"] = not state["enabled"]
                enabled = state["enabled"]
            send_telegram(f"🤖 บอท {'เปิด' if enabled else 'ปิด'} แล้ว")
            self.send_json({"ok": True, "enabled": enabled})
            return

        if path == "/api/select":
            sym = data.get("symbol", "TTB").upper().strip()
            with lock:
                state["selected"] = sym
            self.send_json({"ok": True})
            return

        if path == "/api/order":
            side = data.get("side", "Buy")
            symbol = data.get("symbol", "TTB")
            volume = data.get("volume", 100)
            pin = data.get("pin", "")
            if not pin:
                self.send_json({"ok": False, "msg": "ต้องกรอก PIN"})
                return
            self.send_json(place_order(side, symbol, volume, pin))  # MP-MTL
            return

        if path == "/api/watchlist/add":
            sym = data.get("symbol", "").upper().strip()
            if not sym:
                self.send_json({"ok": False, "msg": "ใส่ชื่อหุ้น"})
                return
            cfg = {
                "bid_drop_pct": float(data.get("bid_drop_pct", 60)),
                "trailing_pct": float(data.get("trailing_pct", 1.0)),
                "active": True,
            }
            ok = save_watchlist_item(sym, cfg)
            if ok:
                with lock:
                    state["watchlist"][sym] = cfg
                ensure_subscribe([sym])
            self.send_json({"ok": ok})
            return

        if path == "/api/watchlist/update":
            sym = data.get("symbol", "").upper().strip()
            if sym not in state["watchlist"]:
                self.send_json({"ok": False, "msg": "ไม่พบหุ้นนี้"})
                return
            cfg = {
                "bid_drop_pct": float(data.get("bid_drop_pct", 60)),
                "trailing_pct": float(data.get("trailing_pct", 1.0)),
                "active": bool(data.get("active", True)),
            }
            ok = save_watchlist_item(sym, cfg)
            if ok:
                with lock:
                    state["watchlist"][sym] = cfg
            self.send_json({"ok": ok})
            return

        if path == "/api/watchlist/remove":
            sym = data.get("symbol", "").upper().strip()
            ok = remove_watchlist_item(sym)
            if ok:
                with lock:
                    state["watchlist"].pop(sym, None)
                    state["symbols"].pop(sym, None)
            self.send_json({"ok": ok})
            return

        self.send_json({"ok": False, "msg": "unknown path"})

HTML = """<!DOCTYPE html>
<html lang="th">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Settrade Bot v2</title>
<style>
  * { box-sizing:border-box; margin:0; padding:0; }
  body { font-family:-apple-system,'Segoe UI',Roboto,sans-serif; background:#0b1220; color:#e2e8f0; padding:12px; }
  .card { background:#151e2e; border:1px solid #263449; border-radius:14px; padding:14px; margin-bottom:12px; }
  .row { display:flex; gap:8px; align-items:center; }
  .grow { flex:1; }
  .badge { padding:6px 12px; border-radius:20px; font-weight:bold; font-size:14px; }
  .on { background:#064e3b; color:#34d399; } .off { background:#7f1d1d; color:#fca5a5; }
  .price { font-size:34px; font-weight:800; }
  .red { color:#f87171; } .green { color:#4ade80; } .yellow { color:#facc15; }
  button { border:none; border-radius:10px; padding:12px; font-size:16px; font-weight:bold; color:#fff; cursor:pointer; }
  .btn-buy { background:#059669; } .btn-sell { background:#dc2626; }
  .btn-toggle { background:#2563eb; } .btn-ghost { background:#334155; } .btn-danger { background:#991b1b; }
  input, select { width:100%; padding:10px; border-radius:8px; border:1px solid #334155; background:#0b1220; color:#fff; font-size:16px; margin-top:4px; }
  label { font-size:12px; color:#94a3b8; font-weight:600; }
  table { width:100%; border-collapse:collapse; font-size:13px; }
  th { color:#94a3b8; font-size:11px; padding:4px; text-align:center; }
  td { padding:5px; text-align:center; border-top:1px solid #1e293b; }
  .mono { font-variant-numeric:tabular-nums; }
  .log { background:#0b1220; border:1px solid #263449; border-radius:8px; padding:8px; font-size:12px; color:#93c5fd; min-height:20px; }
  .wl-row input { width:56px; padding:6px; font-size:13px; margin-top:0; }
  .wl-row button { padding:6px 8px; font-size:12px; width:auto; }
  .modal-bg { display:none; position:fixed; inset:0; background:rgba(0,0,0,.7); z-index:50; align-items:center; justify-content:center; }
  .modal { background:#1e293b; border:1px solid #475569; border-radius:14px; padding:20px; max-width:340px; width:92%; }
</style>
</head>
<body>
  <!-- โซน 1: สถานะ + ปิดฉุกเฉิน -->
  <div class="card">
    <div class="row">
      <div class="grow">
        <span id="statusBadge" class="badge on">🟢 บอททำงาน</span>
        <span id="connBadge" class="badge off">🔌 ยังไม่ต่อ</span>
      </div>
      <button id="toggleBtn" class="btn-toggle" style="width:110px;" onclick="toggleBot()">⏸ ปิดบอท</button>
    </div>
    <div class="log" id="actionLog" style="margin-top:10px;">รอข้อมูล...</div>
  </div>

  <!-- โซน 2: เลือกหุ้น + จอบิด/ออฟเฟอร์ -->
  <div class="card">
    <div class="row">
      <div class="grow">
        <label>เลือกหุ้นดูจอ</label>
        <select id="selSymbol" onchange="selectSymbol()"></select>
      </div>
      <div style="text-align:right;font-size:13px;">
        <div>ถือ: <span id="posTxt" class="yellow mono">0</span> หุ้น</div>
        <div>สูงสุด: <span id="highestTxt" class="yellow mono">--</span></div>
        <div>จุดขาย: <span id="stopTxt" class="green mono">--</span></div>
        <div>บิดหาย: <span id="dropTxt" class="red mono">0%</span></div>
      </div>
    </div>
    <div class="price red mono" id="priceTxt" style="margin:8px 0;">--</div>
    <table>
      <tr><th>วอลุ่ม</th><th>บิด</th><th style="width:30px;"></th><th>ออฟเฟอร์</th><th>วอลุ่ม</th></tr>
      <tbody id="bookBody"><tr><td colspan="5" style="color:#64748b;">กำลังโหลด...</td></tr></tbody>
    </table>
  </div>

  <!-- โซน 3: เทรดด่วน -->
  <div class="card">
    <div style="font-weight:bold;margin-bottom:8px;">⚡ เทรดด่วน (MP-MTL)</div>
    <div class="row">
      <div class="grow"><label>หุ้น</label><input id="tradeSymbol" value="TTB"></div>
      <div class="grow"><label>จำนวน</label><input id="tradeVol" type="number" value="100" inputmode="numeric"></div>
    </div>
    <label>PIN</label>
    <input id="tradePin" type="password" inputmode="numeric">
    <div class="row" style="margin-top:12px;">
      <button class="btn-buy grow" onclick="askOrder('Buy')">🟢 ซื้อ</button>
      <button class="btn-sell grow" onclick="askOrder('Sell')">🔴 ขาย</button>
    </div>
  </div>

  <!-- โซน 4: Watchlist -->
  <div class="card">
    <div style="font-weight:bold;margin-bottom:8px;">📋 รายการเฝ้า (Watchlist)</div>
    <div id="wlBody"></div>
    <div style="border-top:1px solid #263449;margin:10px 0;"></div>
    <div style="font-size:12px;color:#94a3b8;margin-bottom:6px;">➕ เพิ่มหุ้นใหม่</div>
    <div class="row">
      <div class="grow"><input id="newSym" placeholder="เช่น AOT" style="text-transform:uppercase;"></div>
      <div style="width:70px;"><input id="newDrop" type="number" value="60" title="บิดหาย%"></div>
      <div style="width:70px;"><input id="newTrail" type="number" step="0.1" value="1.0" title="trailing%"></div>
      <button class="btn-buy" onclick="addSymbol()">➕</button>
    </div>
  </div>

  <div class="modal-bg" id="modalBg">
    <div class="modal">
      <div style="font-size:16px;font-weight:bold;margin-bottom:8px;" id="modalTitle">ยืนยัน</div>
      <div id="modalBody" style="font-size:14px;color:#cbd5e1;margin-bottom:14px;"></div>
      <div class="row">
        <button class="btn-ghost grow" onclick="closeModal()">ยกเลิก</button>
        <button id="modalOk" class="btn-sell grow" onclick="doOrder()">ยืนยัน</button>
      </div>
    </div>
  </div>

<script>
let pending=null;
const fmt=n=>n==null||n===0?'--':Number(n).toLocaleString('en-US');
async function refresh(){
  try{
    const s = await (await fetch('/api/state')).json();
    // badges
    const sb=document.getElementById('statusBadge');
    sb.className='badge '+(s.enabled?'on':'off');
    sb.textContent=s.enabled?'🟢 บอททำงาน':'⏸ บอทหยุด';
    const cb=document.getElementById('connBadge');
    cb.className='badge '+(s.connected?'on':'off');
    cb.textContent=s.connected?'🔌 เชื่อมต่อ':'🔌 ยังไม่ต่อ';
    document.getElementById('toggleBtn').textContent=s.enabled?'⏸ ปิดบอท':'▶ เปิดบอท';
    // select
    const sel=document.getElementById('selSymbol');
    const keys=Object.keys(s.watchlist||{});
    if(sel.options.length!==keys.length){
      sel.innerHTML=keys.map(k=>`<option value="${k}" ${k===s.selected?'selected':''}>${k}</option>`).join('')||'<option>--</option>';
    }
    // selected data
    const d=s.selected_data||{};
    document.getElementById('tradeSymbol').value=s.selected;
    document.getElementById('priceTxt').textContent=d.last_price?fmt(d.last_price):'--';
    document.getElementById('posTxt').textContent=(s.positions&&s.positions[s.selected])||0;
    document.getElementById('highestTxt').textContent=d.highest?fmt(d.highest):'--';
    document.getElementById('stopTxt').textContent=d.stop?fmt(d.stop):'--';
    document.getElementById('dropTxt').textContent=(d.drop||0)+'%';
    document.getElementById('actionLog').textContent=d.last_action||'รอข้อมูล...';
    // order book
    const tbody=document.getElementById('bookBody');
    let html='';
    const bids=d.bids||[], offers=d.offers||[];
    const n=Math.max(bids.length,offers.length);
    for(let i=0;i<n;i++){
      const b=bids[i]||[],o=offers[i]||[];
      html+=`<tr><td class="yellow mono">${b[1]?fmt(b[1]):''}</td><td class="red mono">${b[0]?fmt(b[0]):''}</td><td></td><td class="green mono">${o[0]?fmt(o[0]):''}</td><td class="yellow mono">${o[1]?fmt(o[1]):''}</td></tr>`;
    }
    tbody.innerHTML=html||'<tr><td colspan="5" style="color:#64748b;">รอข้อมูล...</td></tr>';
    // watchlist rows
    const wl=document.getElementById('wlBody');
    let whtml='<table><tr><th>หุ้น</th><th>ถือ</th><th>บิดหาย%</th><th>Trail%</th><th>บน/ปิด</th><th></th></tr>';
    for(const k of keys){
      const c=s.watchlist[k]||{};
      whtml+=`<tr class="wl-row">
        <td><b>${k}</b></td>
        <td class="yellow mono">${(s.positions&&s.positions[k])||0}</td>
        <td><input id="d_${k}" type="number" value="${c.bid_drop_pct}" onchange="updateRow('${k}')"></td>
        <td><input id="t_${k}" type="number" step="0.1" value="${c.trailing_pct}" onchange="updateRow('${k}')"></td>
        <td><button class="${c.active?'btn-buy':'btn-ghost'}" onclick="toggleActive('${k}')">${c.active?'🟢':'⚪'}</button></td>
        <td><button class="btn-danger" onclick="removeSym('${k}')">🗑</button></td>
      </tr>`;
    }
    whtml+='</table>';
    wl.innerHTML=whtml;
  }catch(e){}
}
async function toggleBot(){ await fetch('/api/toggle',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'}); }
async function selectSymbol(){ await fetch('/api/select',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({symbol:document.getElementById('selSymbol').value})}); }
function askOrder(side){
  const symbol=document.getElementById('tradeSymbol').value.trim().toUpperCase();
  const vol=document.getElementById('tradeVol').value;
  const pin=document.getElementById('tradePin').value;
  if(!pin){ alert('กรอก PIN ก่อน'); return; }
  pending={side,symbol,volume:vol,pin};
  document.getElementById('modalTitle').textContent=(side==='Buy'?'🟢 ซื้อ':'🔴 ขาย')+' '+symbol+' '+vol+' หุ้น';
  document.getElementById('modalBody').textContent='ราคาตลาด (MP-MTL) — ยืนยัน?';
  document.getElementById('modalOk').className='btn-'+(side==='Buy'?'buy':'sell')+' grow';
  document.getElementById('modalBg').style.display='flex';
}
function closeModal(){ pending=null; document.getElementById('modalBg').style.display='none'; }
async function doOrder(){
  if(!pending) return;
  const r=await fetch('/api/order',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(pending)});
  const res=await r.json();
  alert(res.ok?'✅ ส่งแล้ว: '+res.msg:'❌ '+res.msg);
  closeModal();
}
async function addSymbol(){
  const symbol=document.getElementById('newSym').value.trim().toUpperCase();
  if(!symbol){ alert('ใส่ชื่อหุ้น'); return; }
  const body={symbol,bid_drop_pct:document.getElementById('newDrop').value,trailing_pct:document.getElementById('newTrail').value};
  const res=await (await fetch('/api/watchlist/add',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)})).json();
  alert(res.ok?'✅ เพิ่ม '+symbol+' แล้ว':'❌ '+res.msg);
}
async function updateRow(sym){
  await fetch('/api/watchlist/update',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({
    symbol:sym,bid_drop_pct:document.getElementById('d_'+sym).value,trailing_pct:document.getElementById('t_'+sym).value,active:true
  })});
}
async function toggleActive(sym){
  const r=await (await fetch('/api/state')).json();
  const c=r.watchlist[sym]||{};
  await fetch('/api/watchlist/update',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({
    symbol:sym,bid_drop_pct:c.bid_drop_pct,trailing_pct:c.trailing_pct,active:!c.active
  })});
}
async function removeSym(sym){
  if(!confirm('ลบ '+sym+' ออกจากรายการเฝ้า?')) return;
  await fetch('/api/watchlist/remove',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({symbol:sym})});
}
refresh();
setInterval(refresh,1000);
</script>
</body>
</html>"""

# ===================== STARTUP =====================
def main():
    init_settrade()
    wl = load_watchlist()
    with lock:
        state["watchlist"] = wl
        state["selected"] = list(wl.keys())[0]
    if state["connected"]:
        threading.Thread(target=start_realtime, daemon=True).start()
    threading.Thread(target=bot_loop, daemon=True).start()
    port = int(os.getenv("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), DashboardHandler)
    logger.info(f"🌐 Dashboard: http://0.0.0.0:{port}")
    server.serve_forever()

if __name__ == "__main__":
    main()
