# ==============================================================================
# SETTRADE BOT v2.16 — Watchlist หลายหุ้น + Trailing % + เทรด MP-MTL/Limit + Chase-sell
#   + Cost-basis stop-loss + Manual order-status/cancel test tool
# (ดู comment เวอร์ชันก่อนหน้าในไฟล์ v2.13/v2.14/v2.15 สำหรับรายละเอียดทั้งหมด)
#
# v2.16 — แก้ตาม "เอกสาร SDK ทางการ" ที่ยืนยัน signature จริงแล้ว (ไม่ต้องเดาอีกต่อไป):
#   1) cancel_order: เอกสารยืนยันชัดเจนว่าเรียกด้วย keyword args เท่านั้น
#        equity.cancel_order(order_no="AB123456", pin="Your PIN")
#      → ไม่ต้องส่ง account_no และไม่ต้องมี attempts-list เดา 6 แบบเหมือนเดิมอีกแล้ว
#      (v2.14/v2.15 ยังเดาอยู่เพราะตอนนั้นแค่ "ยืนยันจาก log สำเร็จ" ไม่ใช่จากเอกสาร)
#      ตัด _call_cancel_flexible() ออก เหลือแค่เรียก signature เดียวตรงๆ
#      ยังคง retry เฉพาะ error "Invalid Order state"/SEOSGW-01 ไว้เหมือน v2.14 (ของเดิมถูกแล้ว)
#   2) get_order (เอกพจน์!): เอกสารมี equity.get_order(order_no="...") ที่คืน dict ออเดอร์
#      เดียวตรงๆ ครบทุก field ที่ chase-sell ต้องใช้ (matched/balance/canCancel/status/
#      showOrderStatusMeaning) → เปลี่ยน _get_order_snapshot ให้ใช้ get_order ก่อนเป็นทางหลัก
#      (เร็วกว่า ชัวร์กว่า ไม่ต้องดึง get_orders() ทั้งลิสต์มากรองเอาเหมือนเดิม)
#      ยังคง fallback ไป get_orders() (ทั้งลิสต์) ไว้เผื่อ SDK เวอร์ชันเก่าไม่มี get_order
#   3) _cancel_order_manual_test ก็ปรับตาม — ลอง signature ตามเอกสารก่อนเป็นอันดับแรก
#      เครื่องมือนี้ยังมีประโยชน์อยู่ (เช็คว่า SDK เวอร์ชันที่ deploy จริงตรงเอกสารไหม / เช็ค
#      business-logic error เช่น canCancel=False) แต่ไม่ต้องเดา positional-order เยอะแบบเดิมแล้ว
# ==============================================================================

import os
import time
import json
import logging
import datetime
import threading
import concurrent.futures
from functools import partial
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

import requests
import firebase_admin
from firebase_admin import credentials, db
from settrade_v2 import Investor

# ===================== RATE LIMITER (SETTRADE API) =====================
class TokenBucket:
    def __init__(self, rate, capacity):
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_fill = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self, tokens=1):
        while True:
            with self.lock:
                now = time.monotonic()
                elapsed = now - self.last_fill
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                self.last_fill = now
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return
                wait_time = (tokens - self.tokens) / self.rate
            time.sleep(wait_time)  # sleep นอก lock

Q_RATE = float(os.getenv("SETTRADE_QUERY_RATE", "4.0"))
O_RATE = float(os.getenv("SETTRADE_ORDER_RATE", "1.0"))
query_bucket = TokenBucket(rate=Q_RATE, capacity=5)
order_bucket = TokenBucket(rate=O_RATE, capacity=5)

def api_call_with_retry(bucket, func, *args, **kwargs):
    max_retries = 3
    backoff = 1.0
    for attempt in range(max_retries):
        bucket.acquire()
        try:
            resp = func(*args, **kwargs)
            # เช็คกรณี SDK คืนค่าเป็น dict แจ้ง error 401 / session ตาย
            if isinstance(resp, dict) and (resp.get("status_code") == 401 or str(resp.get("success")).lower() == "false"):
                if attempt < max_retries - 1:
                    logger.warning("⚠️ Session หมดอายุ (เจอ 401) กำลัง Login ใหม่...")
                    reconnect_settrade()
                    continue
            return resp
        except Exception as e:
            err = str(e).lower()
            if "429" in err or "rate" in err or "too many" in err:
                if attempt < max_retries - 1:
                    time.sleep(backoff)
                    backoff *= 2
                    continue
            elif "401" in err or "session" in err or "unauthorized" in err:
                if attempt < max_retries - 1:
                    logger.warning(f"⚠️ Session หมดอายุ ({e}) กำลัง Login ใหม่...")
                    reconnect_settrade()
                    continue
            raise

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ===================== สถานะ (ในหน่วยความจำ) =====================
lock = threading.Lock()
investor = None
equity = None
realtime = None

state = {
    "enabled": True,
    "connected": False,
    "selected": "",
    "watchlist": {},
    "positions": {},
    "avg_cost": {},
    "symbols": {},
    "pos_updated": 0,
    "order_log": [],
}
subscribed = set()
_last_trading_date = None
_last_watchlist_load = 0
WATCHLIST_LOAD_INTERVAL = 10  # วิ — throttle การอ่าน watchlist จาก Firebase

DEFAULT_CFG = {
    "bid_drop_pct": 60.0, "trailing_pct": 1.0, "price_drop_pct": 1.0,
    "cost_stop_pct": 1.0,
    "active": True, "pinned": False,
}

STALE_POSITION_SECONDS = 180
SELL_ORDER_TIMEOUT = 10
MARKET_OPEN_HHMM = (9, 55)
MARKET_CLOSE_HHMM = (16, 40)
MARKET_LUNCH_START_HHMM = (12, 30)
MARKET_LUNCH_END_HHMM = (14, 30)
BOT_LOOP_INTERVAL = 2
ORDER_LOG_MAX = 20

TICK_LOG_ENABLED = os.getenv("TICK_LOG_ENABLED", "0") == "1"

CHASE_MAX_ROUNDS = 3
CHASE_POLL_INTERVAL = 0.25
CHASE_POLL_TIMEOUT_PER_ROUND = 3.0

# v2.14: retry เฉพาะกรณี cancel เจอ "Invalid Order state" — ดูคอมเมนต์หัวไฟล์
CANCEL_STATE_RETRY_MAX = 2
CANCEL_STATE_RETRY_DELAY = 0.25  # วิ

_order_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="order")
_io_executor = concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="io")

# ===================== ตัวช่วยหา method ของ SDK แบบยืดหยุ่น =====================
_warned_missing = set()

def _resolve_method(obj, candidates, label=""):
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
    try:
        return method()
    except TypeError:
        return method(*args)

# ===================== เวลา / วันเทรด (Asia/Bangkok, UTC+7) =====================
from datetime import datetime, timezone, timedelta

def get_bkk_now():
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=7)

def get_bkk_date():
    return get_bkk_now().date()

def is_market_hours():
    now = get_bkk_now()
    if now.weekday() >= 5:
        return False
    hm = now.hour * 60 + now.minute
    start = MARKET_OPEN_HHMM[0] * 60 + MARKET_OPEN_HHMM[1]
    end = MARKET_CLOSE_HHMM[0] * 60 + MARKET_CLOSE_HHMM[1]
    lunch_start = MARKET_LUNCH_START_HHMM[0] * 60 + MARKET_LUNCH_START_HHMM[1]
    lunch_end = MARKET_LUNCH_END_HHMM[0] * 60 + MARKET_LUNCH_END_HHMM[1]
    if lunch_start <= hm < lunch_end:
        return False
    return start <= hm <= end

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
    try:
        data = db.reference("watchlist").get() or {}
        wl = {}
        for sym, cfg in data.items():
            if isinstance(cfg, dict):
                wl[sym.upper()] = {
                    "bid_drop_pct": float(cfg.get("bid_drop_pct", DEFAULT_CFG["bid_drop_pct"])),
                    "trailing_pct": float(cfg.get("trailing_pct", DEFAULT_CFG["trailing_pct"])),
                    "price_drop_pct": float(cfg.get("price_drop_pct", DEFAULT_CFG["price_drop_pct"])),
                    "cost_stop_pct": float(cfg.get("cost_stop_pct", DEFAULT_CFG["cost_stop_pct"])),
                    "active": bool(cfg.get("active", True)),
                    "pinned": bool(cfg.get("pinned", False)),
                }
        return wl
    except Exception as e:
        logger.error(f"load_watchlist error: {e}")
        return None

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

def save_trailing(symbol, highest, stop):
    try:
        db.reference(f"trailing/{symbol.upper()}").set({
            "highest": highest,
            "stop": stop,
            "date": str(get_bkk_date()),
        })
    except Exception as e:
        logger.error(f"save_trailing error: {e}")

def load_trailing(symbol):
    try:
        d = db.reference(f"trailing/{symbol.upper()}").get() or {}
        if str(d.get("date", "")) != str(get_bkk_date()):
            return 0.0, 0.0
        return float(d.get("highest", 0) or 0), float(d.get("stop", 0) or 0)
    except Exception as e:
        logger.error(f"load_trailing error: {e}")
        return 0.0, 0.0

def save_trailing_async(symbol, highest, stop):
    _io_executor.submit(save_trailing, symbol, highest, stop)

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

def send_telegram_async(message):
    _io_executor.submit(send_telegram, message)

# ===================== SETTRADE =====================
def init_settrade():
    global investor, equity
    app_id = os.getenv("SETTRADE_APP_ID")
    app_secret = os.getenv("SETTRADE_APP_SECRET")
    broker_id = os.getenv("SETTRADE_BROKER_ID")
    app_code = os.getenv("SETTRADE_APP_CODE", "SANDBOX")
    account_no = os.getenv("SETTRADE_ACCOUNT_N")
    if not app_id or not app_secret:
        logger.warning("ยังไม่มี SETTRADE_APP_ID/SECRET — ต้องรอ key จากโบรกเกอร์ถึงจะเชื่อมตลาด")
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
        
def reconnect_settrade():
    logger.info("🔄 กำลังเชื่อมต่อ Settrade และ Realtime ใหม่...")
    if init_settrade():
        global realtime
        try:
            realtime = investor.RealtimeDataConnection()
            with lock:
                subs = [s for s, c in state["watchlist"].items() if c.get("active", True)]
            subscribed.clear()
            ensure_subscribe(subs)
            logger.info("✅ Reconnect และ Subscribe Realtime ใหม่สำเร็จ")
        except Exception as e:
            logger.error(f"Reconnect Realtime error: {e}")

def refresh_positions(force=False):
    now = time.time()
    if not force and now - state["pos_updated"] < 30:
        return
    if equity is None:
        return
    try:
        get_port = _resolve_method(
            equity,
            ["get_portfolios", "get_portfolio", "portfolio", "get_port", "getPortfolio", "port"],
            "get_portfolio",
        )
        if get_port is None:
            return

        account_no = os.getenv("SETTRADE_ACCOUNT_N")
        raw = api_call_with_retry(query_bucket, _call_flexible, get_port, account_no)

        portfolio_list_key_used = None
        if isinstance(raw, dict):
            for key in ("portfolioList", "portfolio_list", "portfolios", "data", "results"):
                if key in raw:
                    portfolio_list_key_used = key
                    break
            items = raw.get(portfolio_list_key_used, []) if portfolio_list_key_used else []
        else:
            items = raw or []

        pos = {}
        avg_cost = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            sym = (item.get("symbol") or item.get("security_symbol") or "").upper()
            if not sym or sym == "_TOTAL":
                continue
            vol = (
                item.get("amount")
                or item.get("actualVolume")
                or item.get("currentVolume")
                or item.get("startVolume")
                or item.get("volume")
                or item.get("total_volume")
                or item.get("hold_volume")
                or 0
            )
            if sym:
                pos[sym] = int(vol or 0)

            cost = (
                item.get("averagePrice")
                if item.get("averagePrice") not in (None, 0)
                else item.get("average_price")
                if item.get("average_price") not in (None, 0)
                else item.get("avgPrice")
                if item.get("avgPrice") not in (None, 0)
                else item.get("costPrice")
            )
            if sym and cost not in (None, 0):
                try:
                    avg_cost[sym] = float(cost)
                except (TypeError, ValueError):
                    pass

        if not pos and raw:
            list_is_genuinely_empty = (
                isinstance(raw, dict) and portfolio_list_key_used is not None and items == []
            )
            if not list_is_genuinely_empty:
                logger.warning(f"[get_portfolio] ได้ raw data กลับมาแต่แปลงเป็น position ไม่ได้: {str(raw)[:500]}")

        with lock:
            state["positions"] = pos
            state["avg_cost"] = avg_cost
            state["pos_updated"] = now
    except Exception as e:
        logger.error(f"get_portfolio error: {e}")

def sync_watchlist_with_portfolio():
    with lock:
        positions = dict(state["positions"])
        watchlist = dict(state["watchlist"])

    to_add = {sym: vol for sym, vol in positions.items() if vol and vol > 0 and sym not in watchlist}
    to_remove = [
        sym for sym, cfg in watchlist.items()
        if not cfg.get("pinned", False) and int(positions.get(sym, 0) or 0) <= 0
    ]

    if not to_add and not to_remove:
        return

    for sym, vol in to_add.items():
        cfg = dict(DEFAULT_CFG)
        cfg["pinned"] = False
        if save_watchlist_item(sym, cfg):
            watchlist[sym] = cfg
            msg = f"📌 พบหุ้น {sym} ในพอร์ต ({vol} หุ้น) → เพิ่มเข้า watchlist อัตโนมัติ"
            logger.info(msg)
            send_telegram(msg)

    for sym in to_remove:
        if remove_watchlist_item(sym):
            watchlist.pop(sym, None)
            logger.info(f"📭 {sym} ขายหมดพอร์ตแล้ว → เอาออกจาก watchlist อัตโนมัติ")

    with lock:
        state["watchlist"] = watchlist
        for sym in to_remove:
            state["symbols"].pop(sym, None)
        for sym in watchlist:
            if sym not in state["symbols"]:
                h, st = load_trailing(sym)
                state["symbols"][sym] = {"highest": h, "stop": st}

    ensure_subscribe([s for s, c in watchlist.items() if c.get("active", True)])

def _record_order(entry):
    with lock:
        state["order_log"].insert(0, entry)
        state["order_log"] = state["order_log"][:ORDER_LOG_MAX]

def _extract_order_no(resp):
    candidates = ("orderNo", "order_no", "orderId", "order_id", "id", "orderNumber", "no")
    if isinstance(resp, dict):
        for k in candidates:
            v = resp.get(k)
            if v:
                return v
    else:
        for k in candidates:
            v = getattr(resp, k, None)
            if v:
                return v
    return None

def place_order(side, symbol, volume, pin, price_type="MP-MTL", price=0.0):
    price_type = price_type or "MP-MTL"
    use_price = float(price) if price_type == "Limit" else 0.0

    entry = {
        "time": get_bkk_now().strftime("%H:%M:%S"),
        "side": side,
        "symbol": symbol.upper().strip(),
        "volume": volume,
        "price_type": price_type,
        "price": use_price if price_type == "Limit" else None,
        "ok": None,
        "msg": "",
        "order_no": None,
    }

    if equity is None:
        entry["ok"] = False
        entry["msg"] = "ยังไม่ได้เชื่อมต่อ Settrade"
        _record_order(entry)
        return {"ok": False, "msg": entry["msg"], "order_no": None}

    try:
        resp = api_call_with_retry(
            order_bucket,
            equity.place_order,
            side=side,
            symbol=symbol.upper().strip(),
            trustee_id_type=os.getenv("SETTRADE_TRUSTEE_ID", "Local"),
            volume=int(volume),
            price_type=price_type,
            price=use_price,
            validity_type="Day",
            pin=pin,
        )
        order_no = _extract_order_no(resp)
        price_note = f" @{use_price}" if price_type == "Limit" else ""
        msg = f"📤 {side} {symbol} {volume}{price_note} ({price_type})\nตอบ: {resp}"
        logger.info(msg)
        send_telegram(msg)

        entry["ok"] = True
        entry["msg"] = str(resp)
        entry["order_no"] = order_no
        _record_order(entry)
        return {"ok": True, "msg": str(resp), "order_no": order_no}
    except Exception as e:
        logger.error(f"place_order error: {e}")
        entry["ok"] = False
        entry["msg"] = str(e)
        _record_order(entry)
        return {"ok": False, "msg": str(e), "order_no": None}

def _log_late_order_result(side, symbol, volume, future):
    try:
        res = future.result()
        logger.info(f"📬 ผลคำสั่งที่มาช้า: {side} {symbol} {volume} → {res}")
        send_telegram(f"📬 ผลคำสั่งที่ส่งไปก่อนหน้า (ตอบช้ากว่า {SELL_ORDER_TIMEOUT}s): {side} {symbol} {volume}\n{res.get('msg', res)}")
    except Exception as e:
        logger.error(f"late order result error: {e}")

def place_order_async(side, symbol, volume, pin, price_type="MP-MTL", price=0.0, timeout=SELL_ORDER_TIMEOUT):
    future = _order_executor.submit(place_order, side, symbol, volume, pin, price_type, price)
    try:
        return future.result(timeout=timeout)
    except concurrent.futures.TimeoutError:
        logger.warning(
            f"⏱️ {side} {symbol} {volume} ไม่ได้รับคำตอบภายใน {timeout}s — ไปเช็คหุ้นตัวอื่นต่อก่อน "
            f"(คำสั่งอาจกำลังทำงานอยู่เบื้องหลัง ผลจริงจะตามมาใน log/Telegram ทีหลัง)"
        )
        future.add_done_callback(lambda f: _log_late_order_result(side, symbol, volume, f))
        return {"ok": None, "msg": f"timeout {timeout}s — รอผลจริงทีหลัง"}

# ===================== ไล่ราคาขายอัตโนมัติ (chase-sell) =====================

def _resolve_single_order_method():
    """
    v2.16: เอกสาร SDK ยืนยันว่ามี equity.get_order(order_no="...") คืน dict ออเดอร์เดียว
    ตรงๆ (มี matched/balance/canCancel/status/showOrderStatusMeaning ครบ) — ใช้เป็นทางหลัก
    เร็วกว่าและชัวร์กว่าการดึง get_orders() ทั้งลิสต์มากรองเอาแบบเดิม
    """
    return _resolve_method(
        equity,
        ["get_order", "get_order_info", "getOrder"],
        "get_order (chase-sell)",
    )

def _resolve_orders_method():
    """สำรอง — ใช้เฉพาะกรณี SDK เวอร์ชันนั้นไม่มี get_order (เอกพจน์)"""
    return _resolve_method(
        equity,
        ["get_orders", "get_order_list", "list_orders", "orders"],
        "get_orders (chase-sell fallback)",
    )

def _snapshot_from_item(item, order_no):
    matched = 0
    for k in ("matched", "matchedVolume", "matched_volume", "filledVolume", "filled_volume", "cumExecQty", "matchQty"):
        if item.get(k) is not None:
            matched = int(item.get(k) or 0)
            break
    balance = None
    for k in ("balance", "balanceVolume", "balance_volume", "remainingVolume", "remaining_volume", "leavesQty", "leaveVolume"):
        if item.get(k) is not None:
            balance = int(item.get(k) or 0)
            break
    if balance is None:
        total_vol = item.get("vol") or item.get("volume") or item.get("totalVolume") or item.get("qty") or 0
        balance = max(0, int(total_vol or 0) - matched)
    status = item.get("status") or item.get("orderStatus") or ""
    status_meaning = item.get("showOrderStatusMeaning") or item.get("showOrderStatus") or ""
    can_cancel = item.get("canCancel")
    logger.info(
        f"[chase-sell] #{order_no} snapshot: matched={matched} balance={balance} "
        f"status={status} ({status_meaning}) canCancel={can_cancel}"
    )
    return {
        "matched": matched, "balance": balance, "status": status,
        "status_meaning": status_meaning, "can_cancel": can_cancel, "raw": item,
    }

def _get_order_snapshot(order_no):
    if equity is None:
        return None

    # ทางหลัก (v2.16): get_order(order_no=...) ตามเอกสาร SDK — คืนออเดอร์เดียวตรงๆ ไม่ต้องกรอง
    get_order = _resolve_single_order_method()
    if get_order is not None:
        item = None
        try:
            item = api_call_with_retry(query_bucket, get_order, order_no=order_no)
        except TypeError:
            try:
                item = get_order(order_no)
            except Exception as e:
                logger.error(f"[chase-sell] get_order({order_no}) error: {e}")
        except Exception as e:
            logger.error(f"[chase-sell] get_order({order_no}) error: {e}")

        if isinstance(item, dict) and item:
            return _snapshot_from_item(item, order_no)

    # สำรอง: SDK เวอร์ชันเก่า/ไม่มี get_order → ดึง get_orders() ทั้งลิสต์มากรองแทน
    get_orders = _resolve_orders_method()
    if get_orders is None:
        return None

    try:
        raw = api_call_with_retry(query_bucket, _call_flexible, get_orders, os.getenv("SETTRADE_ACCOUNT_N"))
    except Exception as e:
        logger.error(f"[chase-sell] get_orders error: {e}")
        return None

    if isinstance(raw, dict):
        items = None
        for key in ("orders", "orderList", "order_list", "data", "results"):
            if key in raw:
                items = raw.get(key)
                break
        if items is None:
            items = [raw]
    else:
        items = raw or []

    for item in items:
        if not isinstance(item, dict):
            continue
        item_no = None
        for k in ("orderNo", "order_no", "orderId", "order_id", "id", "orderNumber", "no"):
            if item.get(k):
                item_no = item.get(k)
                break
        if item_no is None or str(item_no) != str(order_no):
            continue
        return _snapshot_from_item(item, order_no)

    logger.warning(
        f"[chase-sell] หา order_no={order_no} ไม่เจอ ทั้งจาก get_order และ get_orders — "
        f"ตัวอย่าง raw แรกจาก get_orders: {str(items[0])[:300] if items else '(ว่าง)'}"
    )
    return None

def check_order_status(order_no):
    snap = _get_order_snapshot(order_no)
    if snap is None:
        return {
            "ok": False,
            "msg": "หา order นี้ไม่เจอ หรือ resolve method/field ไม่ได้ — ดู Render log "
                   "บรรทัด [chase-sell] เพื่อดูรายละเอียด",
        }
    return {
        "ok": True,
        "matched": snap["matched"],
        "balance": snap["balance"],
        "status": snap["status"],
        "status_meaning": snap.get("status_meaning", ""),
        "can_cancel": snap["can_cancel"],
    }

def _poll_order_until_settled(order_no, timeout, interval):
    elapsed = 0.0
    last = None
    while elapsed < timeout:
        snap = _get_order_snapshot(order_no)
        if snap is None:
            return None
        last = snap
        if snap["balance"] <= 0:
            return snap
        time.sleep(interval)
        elapsed += interval
    return last

def _cancel_order(order_no, symbol, pin):
    """
    v2.16: ใช้ signature ที่เอกสาร SDK ยืนยันตรงๆ — equity.cancel_order(order_no=..., pin=...)
    (keyword args เท่านั้น ไม่ต้องส่ง account_no และไม่ต้องเดา positional/แบบอื่นอีกต่อไป)
    ยังคง retry เฉพาะกรณี error "Invalid Order state" (SEOSGW-01) เหมือน v2.14 — ดูคอมเมนต์หัวไฟล์
    (canCancel มักเป็น False ทันทีหลังส่งคำสั่งใหม่ เพราะยังรอ SETTRADE ประมวลผลอยู่)
    """
    if equity is None:
        return False
    cancel_fn = _resolve_method(equity, ["cancel_order", "cancelOrder"], "cancel_order (chase-sell)")
    if cancel_fn is None:
        return False

    attempt = 0
    while True:
        attempt += 1
        try:
            resp = api_call_with_retry(order_bucket, cancel_fn, order_no=order_no, pin=pin)
            logger.info(f"🚫 [chase-sell] ยกเลิกคำสั่ง {symbol} #{order_no} → {resp}")
            return True
        except Exception as e:
            err_str = str(e)
            is_state_error = "Invalid Order state" in err_str or "SEOSGW-01" in err_str
            if is_state_error and attempt <= CANCEL_STATE_RETRY_MAX:
                logger.warning(
                    f"[chase-sell] cancel_order {symbol} #{order_no} รอบลอง {attempt}: "
                    f"'Invalid Order state' — น่าจะยังอยู่ในช่วงสั้นๆ ที่ยังยกเลิกไม่ได้ "
                    f"(canCancel มักเป็น False ทันทีหลังส่งคำสั่งใหม่) ลองใหม่ใน {CANCEL_STATE_RETRY_DELAY}s"
                )
                time.sleep(CANCEL_STATE_RETRY_DELAY)
                continue
            logger.error(f"[chase-sell] cancel_order {symbol} #{order_no} error (รอบ {attempt}): {e}")
            return False

def _cancel_order_manual_test(order_no, pin, symbol=""):
    """
    v2.16: เอกสาร SDK ยืนยัน signature ที่ถูกต้องแล้วคือ equity.cancel_order(order_no=..., pin=...)
    เครื่องมือนี้จึงลองแบบนั้นก่อนเป็นหลัก แล้วค่อย fallback แบบ positional เผื่อ SDK เวอร์ชันเก่า
    ยังมีประโยชน์อยู่สำหรับเช็ค business-logic error เช่น canCancel=False / PIN ผิดรูปแบบ
    """
    cancel_fn = _resolve_method(equity, ["cancel_order", "cancelOrder"], "cancel_order (manual test)")
    if cancel_fn is None or equity is None:
        return {
            "ok": False,
            "msg": "ไม่พบ method cancel_order ใน SDK (ดู Render log หา '❗ [cancel_order (manual test)]' "
                   "เพื่อดูรายชื่อ method จริงที่มีอยู่)",
        }

    attempts = [
        ("cancel_order(order_no=order_no, pin=pin) [ตามเอกสาร SDK]", lambda: api_call_with_retry(order_bucket, cancel_fn, order_no=order_no, pin=pin)),
        ("cancel_order(order_no, pin) [positional สำรอง]", lambda: api_call_with_retry(order_bucket, cancel_fn, order_no, pin)),
    ]

    tried = []
    for desc, fn in attempts:
        logger.info(f"🧪 [manual-cancel] {symbol} #{order_no} กำลังลอง: {desc}")
        try:
            resp = fn()
            msg = f"✅ [manual-cancel] {symbol} #{order_no} สำเร็จด้วย: {desc}\nตอบ: {resp}"
            logger.info(msg)
            send_telegram(msg)
            return {"ok": True, "signature": desc, "response": str(resp), "tried_before": tried}
        except TypeError as e:
            err = f"TypeError: {e}"
            logger.info(f"🧪 [manual-cancel] {desc} → signature ไม่ตรง ({err}) ลองแบบถัดไป")
            tried.append({"signature": desc, "error": err})
            continue
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            msg = (
                f"🧪 [manual-cancel] {symbol} #{order_no} ลองแล้ว: {desc}\n"
                f"SDK รับ signature นี้ (ไม่ใช่ TypeError) แต่ error ระดับ business logic: {err}\n"
                f"↳ นี่คือ signature ที่ถูกต้องตามเอกสาร แค่ order นี้ยกเลิกไม่ได้ด้วยเหตุผลอื่น "
                f"(เช่น canCancel=False ตอนนี้, PIN ผิด, order match ไปแล้ว) — ลองกด "
                f"'🔍 เช็คสถานะ' ก่อนเพื่อดู canCancel/status จริงของออเดอร์นี้"
            )
            logger.error(msg)
            send_telegram(msg)
            return {
                "ok": False,
                "signature": desc,
                "response": err,
                "tried_before": tried,
                "likely_correct_signature": True,
            }

    msg = f"❌ [manual-cancel] {symbol} #{order_no} ลองทุก signature แล้วเป็น TypeError หมด — SDK อาจใช้ชื่อ parameter อื่นที่ไม่ได้ลอง"
    logger.error(msg)
    send_telegram(msg)
    return {"ok": False, "msg": "ลองทุกแบบแล้วไม่มี signature ไหนถูกต้อง (TypeError หมด)", "tried_before": tried}

def _sell_chase_worker(symbol, initial_held, pin, tick_ts):
    remaining = initial_held
    round_num = 0
    while remaining > 0 and round_num < CHASE_MAX_ROUNDS:
        round_num += 1
        result = place_order("Sell", symbol, remaining, pin, "MP-MTL", 0.0)
        order_no = result.get("order_no")
        if not result.get("ok") or not order_no:
            msg = (f"⚠️ [chase-sell] {symbol} รอบ {round_num}/{CHASE_MAX_ROUNDS}: ส่งคำสั่งไม่สำเร็จ "
                   f"หรือไม่ได้ order_no กลับมา — หยุดไล่ราคาอัตโนมัติ ตรวจพอร์ต/คำสั่งด้วยตัวเองด่วน "
                   f"({str(result.get('msg', ''))[:150]})")
            logger.error(msg)
            send_telegram_async(msg)
            break

        snap = _poll_order_until_settled(order_no, CHASE_POLL_TIMEOUT_PER_ROUND, CHASE_POLL_INTERVAL)
        if snap is None:
            msg = (f"⚠️ [chase-sell] {symbol} รอบ {round_num}/{CHASE_MAX_ROUNDS}: เช็คสถานะคำสั่ง #{order_no} "
                   f"ไม่ได้ (SDK method หา order ไม่เจอ/ชื่อไม่ตรง ดู Render log) — หยุดไล่ราคาอัตโนมัติ "
                   f"รอ refresh_positions ปรับพอร์ตให้ถูกภายใน 30 วิ หรือตรวจสอบ/ขายส่วนที่เหลือเอง")
            logger.error(msg)
            send_telegram_async(msg)
            break

        matched = snap["matched"]
        balance = snap["balance"]
        with lock:
            cur = int(state["positions"].get(symbol, 0) or 0)
            state["positions"][symbol] = max(0, cur - matched)
        remaining = balance

        if remaining > 0:
            # v2.14: log canCancel ชัดๆ ก่อนตัดสินใจยกเลิกทุกครั้ง (ดูคอมเมนต์หัวไฟล์)
            logger.info(
                f"[chase-sell] {symbol} รอบ {round_num}: เหลือค้าง {remaining} หุ้น "
                f"(canCancel={snap.get('can_cancel')}) → จะลองยกเลิก"
            )
            cancelled = _cancel_order(order_no, symbol, pin)
            if not cancelled:
                msg = (f"⚠️ [chase-sell] {symbol} รอบ {round_num}/{CHASE_MAX_ROUNDS}: ยกเลิกคำสั่งค้าง "
                       f"{remaining} หุ้น (#{order_no}) ไม่สำเร็จ — หยุดไล่ราคา อาจมีคำสั่งซ้อนค้างอยู่ "
                       f"ตรวจสอบด้วยตัวเองด่วน (ลองกด '🔍 เช็คสถานะ' หรือ '🧪 ทดสอบยกเลิก' บนหน้าเว็บ "
                       f"ด้วย order_no นี้เพื่อดูรายละเอียดเพิ่ม)")
                logger.error(msg)
                send_telegram_async(msg)
                break

    if remaining > 0:
        msg = (f"🛑 [chase-sell] {symbol} ไล่ราคาครบ {round_num} รอบแล้วยังเหลือ {remaining} หุ้น "
               f"ขายไม่หมด — ตลาดอาจผันผวนหนัก/สภาพคล่องต่ำมาก กรุณาตรวจสอบและจัดการเองด่วน")
        logger.warning(msg)
        send_telegram_async(msg)
    else:
        t1 = time.time()
        logger.info(
            f"✅ [chase-sell] {symbol} ขายหมดสำเร็จหลังไล่ราคา {round_num} รอบ "
            f"(รวมเวลา tick→ขายหมด: {(t1 - tick_ts) * 1000:.0f}ms)"
        )

    with lock:
        s = state["symbols"].get(symbol)
        if s:
            s["selling"] = False

def _global_guards_ok():
    with lock:
        if not state["enabled"] or not state["connected"]:
            return False
    if not is_market_hours():
        return False

    now_ts = time.time()
    with lock:
        stale = state["pos_updated"] == 0 or (now_ts - state["pos_updated"] > STALE_POSITION_SECONDS)
        was_alerted = state.get("_stale_alerted", False)
    if stale:
        if not was_alerted:
            msg = f"⚠️ ดึงข้อมูลพอร์ตไม่สำเร็จเกิน {STALE_POSITION_SECONDS // 60} นาที — หยุดเทรดอัตโนมัติชั่วคราวเพื่อความปลอดภัย"
            logger.warning(msg)
            send_telegram_async(msg)
            with lock:
                state["_stale_alerted"] = True
        return False
    elif was_alerted:
        send_telegram_async("✅ ดึงข้อมูลพอร์ตกลับมาปกติแล้ว เทรดต่อได้")
        with lock:
            state["_stale_alerted"] = False
    return True

def _check_symbol_and_maybe_sell(symbol, tick_ts=None):
    if tick_ts is None:
        tick_ts = time.time()

    if not _global_guards_ok():
        return

    pin = os.getenv("SETTRADE_PIN")
    sell_action = None
    trailing_to_persist = None

    with lock:
        cfg = state["watchlist"].get(symbol)
        if not cfg or not cfg.get("active", True):
            return
        s = state["symbols"].get(symbol)
        if not s:
            return
        if s.get("selling"):
            return
        threshold = float(cfg.get("bid_drop_pct", 60.0))
        trailing_pct = float(cfg.get("trailing_pct", 1.0))
        held = int(state["positions"].get(symbol, 0) or 0)
        if held <= 0:
            return

        bids = s.get("bids") or []
        if bids:
            bid1_price, bid1_vol = bids[0]
            prev_vol = s.get("prev_bid1_vol", 0.0)
            prev_price = s.get("prev_bid1_price", 0.0)
            price_threshold = float(cfg.get("price_drop_pct", 1.0))

            drop_vol_pct = 0.0
            if prev_vol > 0 and bid1_vol < prev_vol:
                drop_vol_pct = (prev_vol - bid1_vol) / prev_vol * 100

            drop_price_pct = 0.0
            if prev_price > 0 and bid1_price < prev_price:
                drop_price_pct = (prev_price - bid1_price) / prev_price * 100

            s["drop"] = round(max(drop_vol_pct, drop_price_pct), 2)

            vol_triggered = drop_vol_pct >= threshold
            price_triggered = drop_price_pct >= price_threshold
            if vol_triggered or price_triggered:
                reasons = []
                if vol_triggered:
                    reasons.append(f"วอลุ่มหาย {drop_vol_pct:.1f}%")
                if price_triggered:
                    reasons.append(f"ราคาตก {drop_price_pct:.2f}%")
                msg = (f"🚨 {symbol} บิด {bid1_price} ({' + '.join(reasons)}) "
                       f"→ ขาย {held} หุ้น (ไล่ราคาจนหมด) ทันที!")
                logger.warning(msg)
                s["last_action"] = msg
                s["prev_bid1_vol"] = 0
                s["prev_bid1_price"] = 0
                s["selling"] = True
                sell_action = (msg, held)
            else:
                s["prev_bid1_vol"] = bid1_vol
                s["prev_bid1_price"] = bid1_price

        last = s.get("last_price", 0.0)

        if sell_action is None and last > 0:
            cost_stop_pct = float(cfg.get("cost_stop_pct", DEFAULT_CFG["cost_stop_pct"]))
            avg_cost = float(state["avg_cost"].get(symbol, 0) or 0)
            if avg_cost > 0:
                if last < avg_cost:
                    cost_drop_pct = (avg_cost - last) / avg_cost * 100
                    if cost_drop_pct >= cost_stop_pct:
                        msg = (f"🩸 {symbol} ราคา {last} ขาดทุนจากต้นทุน {avg_cost:.2f} "
                               f"({cost_drop_pct:.2f}%) เกิน {cost_stop_pct}% → ตัดขาดทุน "
                               f"ขาย {held} หุ้น (ไล่ราคาจนหมด)")
                        logger.warning(msg)
                        s["last_action"] = msg
                        s["selling"] = True
                        sell_action = (msg, held)
            elif not s.get("_cost_stop_warned"):
                logger.warning(
                    f"[cost-stop] {symbol} ไม่มีข้อมูลต้นทุนเฉลี่ย (avg_cost) — cost-basis "
                    f"stop-loss จะไม่ทำงานกับหุ้นนี้ ใช้ price_drop_pct/trailing แทนไปก่อน"
                )
                s["_cost_stop_warned"] = True

        if sell_action is None:
            if last > 0:
                highest = s.get("highest", 0.0)
                if last > highest:
                    s["highest"] = last
                    s["stop"] = round(last * (1 - trailing_pct / 100.0), 2)
                    trailing_to_persist = (symbol, s["highest"], s["stop"])
                stop = s.get("stop", 0.0)
                if stop > 0 and last <= stop:
                    msg = (f"🛑 {symbol} ราคา {last} ตกถึงจุดขาย {stop} "
                           f"(สูงสุด {s['highest']} -{trailing_pct}%) → ขาย {held} หุ้น (ไล่ราคาจนหมด)")
                    logger.warning(msg)
                    s["last_action"] = msg
                    s["stop"] = 0
                    trailing_to_persist = (symbol, s["highest"], 0)
                    s["selling"] = True
                    sell_action = (msg, held)

    if trailing_to_persist:
        save_trailing_async(*trailing_to_persist)

    if sell_action:
        msg, held = sell_action
        _order_executor.submit(_sell_chase_worker, symbol, held, pin, tick_ts)
        t_decide = time.time()
        logger.info(
            f"⏱️ {symbol} tick→ตัดสินใจขาย+เริ่มไล่ราคา (chase-sell): {(t_decide - tick_ts) * 1000:.1f}ms "
            f"(ผลแต่ละรอบดู log ที่ขึ้นต้นด้วย '[chase-sell]')"
        )
        send_telegram_async(msg)

def check_and_autosell_all():
    with lock:
        symbols = list(state["watchlist"].keys())
    for symbol in symbols:
        _check_symbol_and_maybe_sell(symbol)

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
    tick_ts = time.time()
    try:
        if TICK_LOG_ENABLED:
            logger.info(f"📥 tick bid/offer เข้า {symbol}: {msg}")
        with lock:
            s = state["symbols"].setdefault(symbol, {})
            s["bids"] = normalize_book(msg.get("bids"))
            s["offers"] = normalize_book(msg.get("offers"))
        _check_symbol_and_maybe_sell(symbol, tick_ts=tick_ts)
    except Exception as e:
        logger.error(f"on_bids_offers error: {e}")

def on_price_info(symbol, msg):
    tick_ts = time.time()
    try:
        if TICK_LOG_ENABLED:
            logger.info(f"📥 tick price เข้า {symbol}: {msg}")
        with lock:
            s = state["symbols"].setdefault(symbol, {})
            s["last_price"] = msg.get("last", 0.0) or msg.get("price", 0.0)
        _check_symbol_and_maybe_sell(symbol, tick_ts=tick_ts)
    except Exception as e:
        logger.error(f"on_price_info error: {e}")

def ensure_subscribe(symbols):
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
    global realtime
    try:
        realtime = investor.RealtimeDataConnection()
        wl = load_watchlist() or {}
        ensure_subscribe([s for s, c in wl.items() if c.get("active", True)])
        logger.info("🔌 Realtime subscribe เรียบร้อย (SDK จัดการ connection เบื้องหลังเอง ไม่ต้อง run())")
        while True:
            time.sleep(3600)
    except Exception as e:
        logger.error(f"start_realtime error: {e}")

# ===================== รีเซ็ตข้ามวันเทรด =====================
def new_trading_day_reset():
    with lock:
        for sym, s in state["symbols"].items():
            s["prev_bid1_vol"] = 0
            s["prev_bid1_price"] = 0
            s["selling"] = False
        subscribed.clear()
    logger.info("📅 เข้าสู่วันเทรดใหม่ → รีเซ็ต baseline บิดหาย%/ราคาตก% และบังคับ subscribe ใหม่")
    
def session_keeper():
    last = None
    while True:
        try:
            now = get_bkk_now()
            today = now.date()
            if now.hour == 5 and now.minute < 5 and last != today:
                logger.info("🌅 ตี 5 แล้ว ต่ออายุ Session ประจำวัน...")
                reconnect_settrade()
                last = today
        except Exception as e:
            logger.error(f"session_keeper error: {e}")
        time.sleep(30)

def bot_loop():
    global _last_trading_date, _last_watchlist_load
    while True:
        try:
            today = get_bkk_date()
            if _last_trading_date != today:
                new_trading_day_reset()
                _last_trading_date = today

            now_ts = time.time()
            if now_ts - _last_watchlist_load >= WATCHLIST_LOAD_INTERVAL:
                wl = load_watchlist()
                if wl is not None:
                    with lock:
                        state["watchlist"] = wl
                        for sym in wl:
                            if sym not in state["symbols"]:
                                h, st = load_trailing(sym)
                                state["symbols"][sym] = {"highest": h, "stop": st}
                _last_watchlist_load = now_ts

            with lock:
                active_syms = [s for s, c in state["watchlist"].items() if c.get("active", True)]
            ensure_subscribe(active_syms)
            refresh_positions()
            sync_watchlist_with_portfolio()
            check_and_autosell_all()
        except Exception as e:
            logger.error(f"Bot Loop Error: {e}")
        time.sleep(BOT_LOOP_INTERVAL)

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
                        "selling": sd.get("selling", False),
                        "avg_cost": state["avg_cost"].get(sel, 0),
                    },
                    "watchlist": state["watchlist"],
                    "positions": state["positions"],
                    "avg_cost": state["avg_cost"],
                    "order_log": state["order_log"],
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
            sym = data.get("symbol", "").upper().strip()
            with lock:
                state["selected"] = sym
            self.send_json({"ok": True})
            return

        if path == "/api/order":
            side = data.get("side", "Buy")
            symbol = data.get("symbol", "TTB")
            volume = data.get("volume", 100)
            pin = data.get("pin", "")
            price_type = data.get("price_type", "MP-MTL")
            price_raw = data.get("price", 0)
            if not pin:
                self.send_json({"ok": False, "msg": "ต้องกรอก PIN"})
                return

            if price_type == "Limit":
                try:
                    price = float(price_raw)
                except (TypeError, ValueError):
                    price = 0.0
                if price <= 0:
                    self.send_json({"ok": False, "msg": "ระบุราคาที่ต้องการก่อน (ต้องมากกว่า 0)"})
                    return
            else:
                price_type = "MP-MTL"
                price = 0.0

            if not is_market_hours():
                now = get_bkk_now()
                hm = now.hour * 60 + now.minute
                lunch_start = MARKET_LUNCH_START_HHMM[0] * 60 + MARKET_LUNCH_START_HHMM[1]
                lunch_end = MARKET_LUNCH_END_HHMM[0] * 60 + MARKET_LUNCH_END_HHMM[1]
                if now.weekday() >= 5:
                    reason = "วันหยุดสุดสัปดาห์"
                elif lunch_start <= hm < lunch_end:
                    reason = "ช่วงพักเที่ยง (12:30-14:30 น.)"
                else:
                    reason = "นอกเวลาทำการตลาด"
                msg = f"ตลาดปิดอยู่ตอนนี้ ({reason}) — ส่งคำสั่งไม่ได้"
                entry = {
                    "time": now.strftime("%H:%M:%S"), "side": side,
                    "symbol": symbol.upper().strip(), "volume": volume,
                    "price_type": price_type, "price": price if price_type == "Limit" else None,
                    "ok": False, "msg": msg, "order_no": None,
                }
                _record_order(entry)
                self.send_json({"ok": False, "msg": msg})
                return
            self.send_json(place_order(side, symbol, volume, pin, price_type=price_type, price=price))
            return

        if path == "/api/manual_cancel":
            order_no = str(data.get("order_no", "")).strip()
            pin = data.get("pin", "")
            symbol = data.get("symbol", "").strip().upper()
            if not order_no:
                self.send_json({"ok": False, "msg": "ใส่ Order No ก่อน"})
                return
            if not pin:
                self.send_json({"ok": False, "msg": "ต้องกรอก PIN"})
                return
            self.send_json(_cancel_order_manual_test(order_no, pin, symbol))
            return

        if path == "/api/order_status":
            order_no = str(data.get("order_no", "")).strip()
            if not order_no:
                self.send_json({"ok": False, "msg": "ใส่ Order No ก่อน"})
                return
            self.send_json(check_order_status(order_no))
            return

        if path == "/api/watchlist/add":
            sym = data.get("symbol", "").upper().strip()
            if not sym:
                self.send_json({"ok": False, "msg": "ใส่ชื่อหุ้น"})
                return
            cfg = {
                "bid_drop_pct": float(data.get("bid_drop_pct", 60)),
                "trailing_pct": float(data.get("trailing_pct", 1.0)),
                "price_drop_pct": float(data.get("price_drop_pct", 1.0)),
                "cost_stop_pct": float(data.get("cost_stop_pct", DEFAULT_CFG["cost_stop_pct"])),
                "active": True,
                "pinned": True,
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
            with lock:
                existing = state["watchlist"].get(sym)
            if existing is None:
                self.send_json({"ok": False, "msg": "ไม่พบหุ้นนี้"})
                return
            cfg = {
                "bid_drop_pct": float(data.get("bid_drop_pct", 60)),
                "trailing_pct": float(data.get("trailing_pct", 1.0)),
                "price_drop_pct": float(data.get("price_drop_pct", 1.0)),
                "cost_stop_pct": float(data.get("cost_stop_pct", DEFAULT_CFG["cost_stop_pct"])),
                "active": bool(data.get("active", True)),
                "pinned": bool(existing.get("pinned", False)),
            }
            ok = save_watchlist_item(sym, cfg)
            if ok:
                with lock:
                    state["watchlist"][sym] = cfg
            self.send_json({"ok": ok})
            return

        if path == "/api/watchlist/remove":
            sym = data.get("symbol", "").upper().strip()
            with lock:
                existed_in_state = sym in state["watchlist"]
            ok = remove_watchlist_item(sym)
            logger.info(f"🗑 ขอลบ {sym} ออกจาก watchlist (มีอยู่ใน state ก่อนลบ={existed_in_state}) → firebase_delete_ok={ok}")
            if ok:
                with lock:
                    state["watchlist"].pop(sym, None)
                    state["symbols"].pop(sym, None)
            self.send_json({"ok": ok, "msg": "" if ok else "ลบใน Firebase ไม่สำเร็จ ดู Render logs"})
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
  .card { background:#151e2e; border:1px solid #263449; border-radius:14px; padding:14px; margin-bottom:12px; overflow:hidden; }
  .row { display:flex; gap:8px; align-items:center; }
  .grow { flex:1; }
  .badge { padding:6px 12px; border-radius:20px; font-weight:bold; font-size:14px; }
  .on { background:#064e3b; color:#34d399; } .off { background:#7f1d1d; color:#fca5a5; }
  .warn { background:#78350f; color:#fbbf24; }
  .price { font-size:34px; font-weight:800; }
  .red { color:#f87171; } .green { color:#4ade80; } .yellow { color:#facc15; }
  button { border:none; border-radius:10px; padding:12px; font-size:16px; font-weight:bold; color:#fff; cursor:pointer; }
  .btn-buy { background:#059669; } .btn-sell { background:#dc2626; }
  .btn-toggle { background:#2563eb; } .btn-ghost { background:#334155; } .btn-danger { background:#991b1b; }
  .btn-info { background:#0369a1; }
  input, select { width:100%; padding:10px; border-radius:8px; border:1px solid #334155; background:#0b1220; color:#fff; font-size:16px; margin-top:4px; }
  label { font-size:12px; color:#94a3b8; font-weight:600; }
  table { border-collapse:collapse; font-size:13px; }
  th { color:#94a3b8; font-size:11px; padding:4px; text-align:center; white-space:nowrap; }
  td { padding:5px; text-align:center; border-top:1px solid #1e293b; white-space:nowrap; }
  .mono { font-variant-numeric:tabular-nums; }
  .log { background:#0b1220; border:1px solid #263449; border-radius:8px; padding:8px; font-size:12px; color:#93c5fd; min-height:20px; white-space:pre-wrap; word-break:break-word; }
  .wl-scroll { overflow-x:auto; -webkit-overflow-scrolling:touch; margin:0 -14px; padding:0 14px; }
  .wl-scroll table { min-width:100%; }
  .wl-row input { width:50px; padding:6px; font-size:13px; margin-top:0; }
  .wl-row button { padding:6px 8px; font-size:12px; width:auto; white-space:nowrap; }
  .modal-bg { display:none; position:fixed; inset:0; background:rgba(0,0,0,.7); z-index:50; align-items:center; justify-content:center; }
  .modal { background:#1e293b; border:1px solid #475569; border-radius:14px; padding:20px; max-width:340px; width:92%; }
  .order-row { display:flex; justify-content:space-between; gap:8px; font-size:12px; padding:6px 0; border-top:1px solid #1e293b; align-items:flex-start; }
  .order-row:first-child { border-top:none; }
  .ok-yes { color:#4ade80; } .ok-no { color:#f87171; } .ok-pending { color:#facc15; }
  .btn-cancel-mini { padding:3px 7px; font-size:10px; margin-left:6px; background:#991b1b; border-radius:6px; }
</style>
</head>
<body>
  <!-- โซน 1: สถานะ + ปิดฉุกเฉิน -->
  <div class="card">
    <div class="row">
      <div class="grow">
        <span id="statusBadge" class="badge on">🟢 บอททำงาน</span>
        <span id="connBadge" class="badge off">🔌 ยังไม่ต่อ</span>
        <span id="sellingBadge" class="badge warn" style="display:none;">🏃 กำลังไล่ราคาขาย...</span>
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
    <table style="width:100%;">
      <tr><th>วอลุ่ม</th><th>บิด</th><th style="width:30px;"></th><th>ออฟเฟอร์</th><th>วอลุ่ม</th></tr>
      <tbody id="bookBody"><tr><td colspan="5" style="color:#64748b;">กำลังโหลด...</td></tr></tbody>
    </table>
  </div>

  <!-- โซน 3: เทรดด่วน -->
  <div class="card">
    <div style="font-weight:bold;margin-bottom:8px;">⚡ เทรดด่วน</div>
    <div class="row">
      <div class="grow"><label>หุ้น</label><input id="tradeSymbol" value="TTB"></div>
      <div class="grow"><label>จำนวน</label><input id="tradeVol" type="number" value="100" inputmode="numeric"></div>
    </div>
    <div class="row" style="margin-top:8px;">
      <div class="grow">
        <label>ประเภทคำสั่ง</label>
        <select id="tradePriceType" onchange="togglePriceField()">
          <option value="MP-MTL">ราคาตลาด (MP-MTL)</option>
          <option value="Limit">จำกัดราคา (Limit)</option>
        </select>
      </div>
      <div class="grow" id="tradePriceWrap" style="display:none;">
        <label>ราคาที่ต้องการ (บาท)</label>
        <input id="tradePrice" type="number" step="0.01" inputmode="decimal" placeholder="พิมพ์ราคาที่ต้องการเอง">
      </div>
    </div>
    <label>PIN</label>
    <input id="tradePin" type="password" inputmode="numeric">
    <div class="row" style="margin-top:12px;">
      <button class="btn-buy grow" onclick="askOrder('Buy')">🟢 ซื้อ</button>
      <button class="btn-sell grow" onclick="askOrder('Sell')">🔴 ขาย</button>
    </div>
  </div>

  <!-- โซน 3.5: ประวัติคำสั่งซื้อขาย -->
  <div class="card">
    <div style="font-weight:bold;margin-bottom:8px;">🧾 ประวัติคำสั่ง (ล่าสุด 20 รายการ)</div>
    <div style="font-size:11px;color:#64748b;margin-bottom:6px;">ตอนไล่ราคาขายอัตโนมัติ (chase-sell) แต่ละรอบจะขึ้นเป็นคนละแถวในนี้ — กด 🚫 เพื่อดึง Order No ไปกรอกในช่องทดสอบยกเลิก/เช็คสถานะด้านล่าง</div>
    <div id="orderLogBody" style="font-size:12px;color:#64748b;">ยังไม่มีคำสั่ง</div>
  </div>

  <!-- โซน 3.6: เช็คสถานะ / ทดสอบยกเลิกคำสั่ง -->
  <div class="card">
    <div style="font-weight:bold;margin-bottom:8px;">🔍 เช็คสถานะ / 🧪 ทดสอบยกเลิกคำสั่ง</div>
    <div style="font-size:11px;color:#64748b;margin-bottom:6px;">
      ใช้หา signature ที่ถูกต้องของ SDK และตรวจสอบสถานะออเดอร์ — ไม่ผูกกับ chase-sell อัตโนมัติ
      (ของเดิมใช้ signature ที่ยืนยันแล้วอยู่แล้ว) พิมพ์ Order No เองหรือกด 🚫 จากตารางประวัติ
      คำสั่งด้านบนก็ได้ "🔍 เช็คสถานะ" ไม่มีผลข้างเคียงและไม่ต้องใช้ PIN แนะนำให้กดก่อนเสมอ
      ก่อนจะลอง "🧪 ทดสอบยกเลิก" ซึ่งต้องใช้ PIN และมีผลจริงกับออเดอร์ — ใส่ PIN เป็นตัวเลขล้วนๆ
      เท่านั้น (ห้ามมีช่องว่าง/ตัวอักษร ไม่งั้นจะได้ error "Invalid Pin Format")
    </div>
    <div class="row">
      <div class="grow"><label>Order No</label><input id="cancelOrderNo" placeholder="เช่น 64UJS0PUXL"></div>
      <div class="grow"><label>หุ้น (แค่ label ไม่บังคับ)</label><input id="cancelSymbol" placeholder="เช่น UKEM"></div>
    </div>
    <label>PIN (ใช้เฉพาะตอนกดทดสอบยกเลิก)</label>
    <input id="cancelPin" type="password" inputmode="numeric">
    <div class="row" style="margin-top:10px;">
      <button class="btn-info grow" onclick="checkStatus()">🔍 เช็คสถานะ</button>
      <button class="btn-sell grow" onclick="testCancel()">🧪 ทดสอบยกเลิก</button>
    </div>
    <div id="cancelResult" class="log" style="margin-top:10px;display:none;"></div>
  </div>

  <!-- โซน 3.7: พอร์ตปัจจุบัน (ดิบๆ ไม่ผ่านการกรอง) -->
  <div class="card">
    <div style="font-weight:bold;margin-bottom:8px;">💼 พอร์ตปัจจุบัน (จาก Settrade ตรงๆ)</div>
    <div id="portBody" style="font-size:13px;color:#94a3b8;">กำลังโหลด...</div>
  </div>

  <!-- โซน 4: Watchlist -->
  <div class="card">
    <div style="font-weight:bold;margin-bottom:8px;">📋 รายการเฝ้า (Watchlist)</div>
    <div style="font-size:11px;color:#64748b;margin-bottom:4px;">บิดหาย% หรือ ราคาตก% หรือ ขาดทุนจากต้นทุน% (แล้วแต่อันไหนถึงก่อน) → ไล่ราคาขายหมดพอร์ตด้วย MP-MTL (cancel+ส่งใหม่อัตโนมัติถ้าขายไม่หมดในรอบเดียว)</div>
    <div style="font-size:11px;color:#64748b;margin-bottom:6px;">🩸 = จุดตัดขาดทุนอ้างอิงต้นทุนจริง (คงที่ ไม่เลื่อนตามราคา) — ต้องมีข้อมูลต้นทุนจากพอร์ตก่อนถึงจะทำงาน ดูคอลัมน์ "ต้นทุน" ถ้าว่างแปลว่าคอลัมน์นี้ยังใช้ไม่ได้กับหุ้นนั้น</div>
    <div style="font-size:11px;color:#64748b;margin-bottom:6px;">🔒 = หุ้นที่ถืออยู่จริง ระบบเพิ่มให้อัตโนมัติ ลบแล้วจะเพิ่มกลับถ้ายังถือของอยู่ (ขายหมดจะหายเอง) — ถ้าอยากหยุดเฝ้าโดยไม่ลบ ใช้ปุ่ม 🟢/⚪ แทน ส่วนหุ้นที่กด + เพิ่มเอง ลบได้อิสระ ไว้ทดสอบ</div>
    <div style="font-size:11px;color:#64748b;margin-bottom:6px;">↔️ เลื่อนซ้าย-ขวาในตารางได้ถ้าจอแคบ — ปุ่ม 🟢/⚪ และ 🗑 จะไม่ล้นออกนอกการ์ดแล้ว</div>
    <div id="wlBody"></div>
    <div style="border-top:1px solid #263449;margin:10px 0;"></div>
    <div style="font-size:12px;color:#94a3b8;margin-bottom:6px;">➕ เพิ่มหุ้นใหม่ (ไว้ทดสอบ/เฝ้าก่อนซื้อ)</div>
    <div class="row">
      <div class="grow"><input id="newSym" placeholder="เช่น AOT" style="text-transform:uppercase;"></div>
      <div style="width:54px;"><input id="newDrop" type="number" value="60" title="บิดหาย%"></div>
      <div style="width:54px;"><input id="newPriceDrop" type="number" step="0.1" value="1.0" title="ราคาตก%"></div>
      <div style="width:54px;"><input id="newTrail" type="number" step="0.1" value="1.0" title="trailing%"></div>
      <div style="width:54px;"><input id="newCostStop" type="number" step="0.1" value="1.0" title="ขาดทุนจากต้นทุน%"></div>
      <button class="btn-buy" onclick="addSymbol()">➕</button>
    </div>
  </div>

  <div class="modal-bg" id="modalBg">
    <div class="modal">
      <div style="font-size:16px;font-weight:bold;margin-bottom:8px;" id="modalTitle">ยืนยัน</div>
      <div id="modalBody" style="font-size:14px;color:#cbd5e1;margin-bottom:14px;"></div>
      <div class="row">
        <button class="btn-ghost grow" onclick="closeModal()">ยกเลิก</button>
        <button id="modalOk" class="btn-sell grow" onclick="modalConfirmFn && modalConfirmFn()">ยืนยัน</button>
      </div>
    </div>
  </div>

<script>
let pending=null;
let modalConfirmFn=null;
let pendingRemoveSym=null;
let userTypingSymbol=false;
let wlFocusedId=null;
let wlEditedAt={};
let wlActiveCache={};
const fmt=n=>n==null||n===0?'--':Number(n).toLocaleString('en-US');
function wlVal(id, fallback){
  if(wlEditedAt[id] && (Date.now()-wlEditedAt[id] < 4000)){
    const el=document.getElementById(id);
    if(el) return el.value;
  }
  return fallback;
}

document.addEventListener('DOMContentLoaded', ()=>{
  const el = document.getElementById('tradeSymbol');
  el.addEventListener('focus', ()=>{ userTypingSymbol=true; });
  el.addEventListener('blur', ()=>{ userTypingSymbol=false; });
});

document.addEventListener('focusin', e=>{
  if(e.target && e.target.closest && e.target.closest('#wlBody')) wlFocusedId = e.target.id;
});
document.addEventListener('focusout', e=>{
  if(e.target && e.target.id === wlFocusedId) wlFocusedId = null;
});

function togglePriceField(){
  const isLimit = document.getElementById('tradePriceType').value === 'Limit';
  document.getElementById('tradePriceWrap').style.display = isLimit ? '' : 'none';
}

function fillCancelForm(orderNo, symbol){
  document.getElementById('cancelOrderNo').value = orderNo || '';
  document.getElementById('cancelSymbol').value = symbol || '';
  document.getElementById('cancelOrderNo').scrollIntoView({behavior:'smooth', block:'center'});
}

async function checkStatus(){
  const order_no = document.getElementById('cancelOrderNo').value.trim();
  if(!order_no){ alert('ใส่ Order No ก่อน'); return; }
  const box = document.getElementById('cancelResult');
  box.style.display='';
  box.textContent = 'กำลังเช็คสถานะ...';
  try{
    const res = await (await fetch('/api/order_status',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({order_no})})).json();
    if(res.ok){
      box.textContent = `📦 สถานะ #${order_no}\\nจับคู่แล้ว (matched): ${res.matched}\\nเหลือค้าง (balance): ${res.balance}\\nstatus: ${res.status} ${res.status_meaning?('('+res.status_meaning+')'):''}\\ncanCancel: ${res.can_cancel}`;
    }else{
      box.textContent = '❌ '+(res.msg||'เช็คสถานะไม่ได้');
    }
  }catch(e){
    box.textContent = '❌ ส่งคำขอไม่สำเร็จ: '+e;
  }
}

async function testCancel(){
  const order_no = document.getElementById('cancelOrderNo').value.trim();
  const symbol = document.getElementById('cancelSymbol').value.trim();
  const pin = document.getElementById('cancelPin').value;
  if(!order_no){ alert('ใส่ Order No ก่อน'); return; }
  if(!pin){ alert('กรอก PIN ก่อน'); return; }
  const box = document.getElementById('cancelResult');
  box.style.display='';
  box.textContent = 'กำลังทดสอบ...';
  try{
    const res = await (await fetch('/api/manual_cancel',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({order_no,symbol,pin})})).json();
    let txt;
    if(res.ok){
      txt = '✅ สำเร็จด้วย: '+res.signature+'\\nตอบ: '+res.response;
    } else if(res.likely_correct_signature){
      txt = '⚠️ น่าจะเจอ signature ถูกแล้ว: '+res.signature+'\\nแต่ error: '+res.response;
    } else {
      txt = '❌ '+(res.msg||res.response||'ไม่ทราบสาเหตุ');
    }
    if(res.tried_before && res.tried_before.length){
      txt += '\\n\\nลองไปแล้ว '+res.tried_before.length+' แบบก่อนหน้า (ดูรายละเอียดที่ Render log บรรทัด [manual-cancel])';
    }
    box.textContent = txt;
  }catch(e){
    box.textContent = '❌ ส่งคำขอไม่สำเร็จ: '+e;
  }
}

async function refresh(){
  try{
    const s = await (await fetch('/api/state')).json();
    const sb=document.getElementById('statusBadge');
    sb.className='badge '+(s.enabled?'on':'off');
    sb.textContent=s.enabled?'🟢 บอททำงาน':'⏸ บอทหยุด';
    const cb=document.getElementById('connBadge');
    cb.className='badge '+(s.connected?'on':'off');
    cb.textContent=s.connected?'🔌 เชื่อมต่อ':'🔌 ยังไม่ต่อ';
    document.getElementById('toggleBtn').textContent=s.enabled?'⏸ ปิดบอท':'▶ เปิดบอท';
    const sel=document.getElementById('selSymbol');
    const keys=Object.keys(s.watchlist||{});
    if(sel.options.length!==keys.length){
      sel.innerHTML=keys.map(k=>`<option value="${k}" ${k===s.selected?'selected':''}>${k}</option>`).join('')||'<option>--</option>';
    }
    const d=s.selected_data||{};
    document.getElementById('priceTxt').textContent=d.last_price?fmt(d.last_price):'--';
    document.getElementById('posTxt').textContent=(s.positions&&s.positions[s.selected])||0;
    document.getElementById('highestTxt').textContent=d.highest?fmt(d.highest):'--';
    document.getElementById('stopTxt').textContent=d.stop?fmt(d.stop):'--';
    document.getElementById('dropTxt').textContent=(d.drop||0)+'%';
    document.getElementById('actionLog').textContent=d.last_action||'รอข้อมูล...';
    document.getElementById('sellingBadge').style.display = d.selling ? '' : 'none';
    const tbody=document.getElementById('bookBody');
    let html='';
    const bids=d.bids||[], offers=d.offers||[];
    const n=Math.max(bids.length,offers.length);
    for(let i=0;i<n;i++){
      const b=bids[i]||[],o=offers[i]||[];
      html+=`<tr><td class="yellow mono">${b[1]?fmt(b[1]):''}</td><td class="red mono">${b[0]?fmt(b[0]):''}</td><td></td><td class="green mono">${o[0]?fmt(o[0]):''}</td><td class="yellow mono">${o[1]?fmt(o[1]):''}</td></tr>`;
    }
    tbody.innerHTML=html||'<tr><td colspan="5" style="color:#64748b;">รอข้อมูล...</td></tr>';

    const olb=document.getElementById('orderLogBody');
    const orders=s.order_log||[];
    if(orders.length===0){
      olb.innerHTML='ยังไม่มีคำสั่ง';
    }else{
      olb.innerHTML=orders.map(o=>{
        const cls=o.ok===true?'ok-yes':(o.ok===false?'ok-no':'ok-pending');
        const icon=o.ok===true?'✅':(o.ok===false?'❌':'⏳');
        const sideTh=o.side==='Buy'?'ซื้อ':'ขาย';
        const priceTxt = (o.price_type==='Limit' && o.price) ? ` @${o.price}` : '';
        const cancelBtn = o.order_no ? `<button class="btn-cancel-mini" onclick="fillCancelForm('${o.order_no}','${o.symbol}')">🚫</button>` : '';
        return `<div class="order-row"><span>${o.time} ${icon} ${sideTh} ${o.symbol} ${o.volume}${priceTxt}${cancelBtn}</span><span class="${cls}" style="text-align:right;max-width:55%;overflow-wrap:anywhere;">${(o.msg||'').slice(0,80)}</span></div>`;
      }).join('');
    }

    const pb=document.getElementById('portBody');
    const pk=Object.keys(s.positions||{});
    pb.innerHTML = pk.length
      ? pk.map(k=>`<div>${k}: <span class="yellow mono">${s.positions[k]}</span> หุ้น</div>`).join('')
      : '<div>ไม่มีหุ้นในพอร์ต</div>';

    if(!wlFocusedId){
      const wl=document.getElementById('wlBody');
      if(keys.length===0){
        wl.innerHTML='<div style="color:#64748b;font-size:13px;padding:4px 0;">ยังไม่มีหุ้นในรายการเฝ้า — ถ้าถือหุ้นอยู่จะเพิ่มให้อัตโนมัติ หรือกด + เพิ่มเองด้านล่างเพื่อทดสอบ</div>';
      } else {
        let whtml='<div class="wl-scroll"><table><tr><th>หุ้น</th><th>ถือ</th><th>ต้นทุน</th><th>บิดหาย%</th><th>ราคาตก%</th><th>Trail%</th><th>ขาดทุน%</th><th>บน/ปิด</th><th></th></tr>';
        for(const k of keys){
          const c=s.watchlist[k]||{};
          const held=(s.positions&&s.positions[k])||0;
          const cost=(s.avg_cost&&s.avg_cost[k])||0;
          const autoTag=(!c.pinned && held>0)?' <span style="font-size:10px;color:#64748b;">🔒</span>':'';
          wlActiveCache[k]=!!c.active;
          whtml+=`<tr class="wl-row">
            <td><b>${k}</b>${autoTag}</td>
            <td class="yellow mono">${held}</td>
            <td class="mono" style="color:#64748b;">${cost?fmt(cost):'--'}</td>
            <td><input id="d_${k}" type="number" value="${wlVal('d_'+k, c.bid_drop_pct)}" onchange="updateRow('${k}')"></td>
            <td><input id="p_${k}" type="number" step="0.1" value="${wlVal('p_'+k, c.price_drop_pct!=null?c.price_drop_pct:1.0)}" onchange="updateRow('${k}')"></td>
            <td><input id="t_${k}" type="number" step="0.1" value="${wlVal('t_'+k, c.trailing_pct)}" onchange="updateRow('${k}')"></td>
            <td><input id="c_${k}" type="number" step="0.1" value="${wlVal('c_'+k, c.cost_stop_pct!=null?c.cost_stop_pct:1.0)}" onchange="updateRow('${k}')"></td>
            <td><button class="${c.active?'btn-buy':'btn-ghost'}" onclick="toggleActive('${k}')">${c.active?'🟢':'⚪'}</button></td>
            <td><button class="btn-danger" onclick="askRemove('${k}')">🗑</button></td>
          </tr>`;
        }
        whtml+='</table></div>';
        wl.innerHTML=whtml;
      }
    }
  }catch(e){}
}
async function toggleBot(){ await fetch('/api/toggle',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'}); }
async function selectSymbol(){
  const sym = document.getElementById('selSymbol').value;
  document.getElementById('tradeSymbol').value = sym;
  await fetch('/api/select',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({symbol:sym})});
}
function askOrder(side){
  const symbol=document.getElementById('tradeSymbol').value.trim().toUpperCase();
  const vol=document.getElementById('tradeVol').value;
  const pin=document.getElementById('tradePin').value;
  const priceType=document.getElementById('tradePriceType').value;
  const priceVal=document.getElementById('tradePrice').value;
  if(!pin){ alert('กรอก PIN ก่อน'); return; }
  if(priceType==='Limit' && (!priceVal || Number(priceVal)<=0)){ alert('ระบุราคาที่ต้องการก่อน'); return; }
  pending={side,symbol,volume:vol,pin,price_type:priceType,price:priceType==='Limit'?priceVal:0};
  modalConfirmFn=doOrder;
  const priceTxt = priceType==='Limit' ? (' ที่ราคา '+priceVal+' บาท') : ' (ราคาตลาด MP-MTL)';
  document.getElementById('modalTitle').textContent=(side==='Buy'?'🟢 ซื้อ':'🔴 ขาย')+' '+symbol+' '+vol+' หุ้น';
  document.getElementById('modalBody').textContent='ยืนยันคำสั่ง'+priceTxt+' ?';
  document.getElementById('modalOk').className='btn-'+(side==='Buy'?'buy':'sell')+' grow';
  document.getElementById('modalBg').style.display='flex';
}
function closeModal(){ pending=null; pendingRemoveSym=null; modalConfirmFn=null; document.getElementById('modalBg').style.display='none'; }
async function doOrder(){
  if(!pending) return;
  const r=await fetch('/api/order',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(pending)});
  const res=await r.json();
  alert(res.ok?'✅ ส่งแล้ว: '+res.msg:'❌ '+res.msg);
  closeModal();
  refresh();
}
async function addSymbol(){
  const symbol=document.getElementById('newSym').value.trim().toUpperCase();
  if(!symbol){ alert('ใส่ชื่อหุ้น'); return; }
  const body={symbol,bid_drop_pct:document.getElementById('newDrop').value,price_drop_pct:document.getElementById('newPriceDrop').value,trailing_pct:document.getElementById('newTrail').value,cost_stop_pct:document.getElementById('newCostStop').value};
  const res=await (await fetch('/api/watchlist/add',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)})).json();
  alert(res.ok?'✅ เพิ่ม '+symbol+' แล้ว':'❌ '+res.msg);
}
async function updateRow(sym){
  const now=Date.now();
  ['d_','p_','t_','c_'].forEach(pfx=>{ wlEditedAt[pfx+sym]=now; });
  const activeState = wlActiveCache[sym]!==false;
  try{
    const res = await (await fetch('/api/watchlist/update',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({
      symbol:sym,bid_drop_pct:document.getElementById('d_'+sym).value,price_drop_pct:document.getElementById('p_'+sym).value,trailing_pct:document.getElementById('t_'+sym).value,cost_stop_pct:document.getElementById('c_'+sym).value,active:activeState
    })})).json();
    if(!res.ok){
      alert('❌ บันทึกไม่สำเร็จ: '+(res.msg||'ไม่ทราบสาเหตุ — ลองใหม่อีกครั้ง หรือดู Render logs'));
    }
  }catch(e){
    alert('❌ ส่งคำขอบันทึกไม่สำเร็จ (เน็ตหลุด/เซิร์ฟเวอร์ไม่ตอบ): '+e);
  }
}
async function toggleActive(sym){
  const r=await (await fetch('/api/state')).json();
  const c=r.watchlist[sym]||{};
  await fetch('/api/watchlist/update',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({
    symbol:sym,bid_drop_pct:c.bid_drop_pct,price_drop_pct:c.price_drop_pct,trailing_pct:c.trailing_pct,cost_stop_pct:c.cost_stop_pct,active:!c.active
  })});
}
function askRemove(sym){
  pendingRemoveSym=sym;
  modalConfirmFn=doRemove;
  document.getElementById('modalTitle').textContent='🗑 ลบ '+sym;
  document.getElementById('modalBody').textContent='ลบ '+sym+' ออกจากรายการเฝ้า — ยืนยัน?';
  document.getElementById('modalOk').className='btn-sell grow';
  document.getElementById('modalBg').style.display='flex';
}
async function doRemove(){
  const sym=pendingRemoveSym;
  if(!sym) return;
  try{
    const r=await fetch('/api/watchlist/remove',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({symbol:sym})});
    const res=await r.json();
    closeModal();
    if(!res.ok){ alert('❌ ลบไม่สำเร็จ: '+(res.msg||'ไม่ทราบสาเหตุ')); return; }
    refresh();
  }catch(e){
    closeModal();
    alert('❌ ส่งคำขอลบไม่สำเร็จ (เน็ตหลุด/เซิร์ฟเวอร์ไม่ตอบ): '+e);
  }
}
refresh();
setInterval(refresh,1000);
</script>
</body>
</html>"""

# ===================== STARTUP =====================
def main():
    global _last_trading_date
    init_settrade()
    wl = load_watchlist() or {}
    with lock:
        state["watchlist"] = wl
        state["selected"] = next(iter(wl), "")
        for sym in wl:
            h, st = load_trailing(sym)
            state["symbols"][sym] = {"highest": h, "stop": st}
    _last_trading_date = get_bkk_date()
    if state["connected"]:
        threading.Thread(target=start_realtime, daemon=True).start()
    threading.Thread(target=bot_loop, daemon=True).start()
    port = int(os.getenv("PORT", 10000))
    server = ThreadingHTTPServer(("0.0.0.0", port), DashboardHandler)
    logger.info(f"🌐 Dashboard: http://0.0.0.0:{port}")
    server.serve_forever()

if __name__ == "__main__":
    main()