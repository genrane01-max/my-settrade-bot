# ==============================================================================
# SETTRADE BOT v2.18 — Watchlist หลายหุ้น + Trailing % + เทรด MP-MTL/Limit + Chase-sell
#   + Cost-basis stop-loss + Manual order-status/cancel test tool + ซื้ออัตโนมัติ (auto-buy)
# (ดู comment เวอร์ชันก่อนหน้าในไฟล์ v2.13-v2.17 สำหรับรายละเอียดฟีเจอร์ทั้งหมด — โค้ด
#  ฝั่ง backend/ตรรกะเทรดในไฟล์นี้เหมือน v2.17 เป๊ะทุกบรรทัด ไม่มีการแก้ตรรกะใดๆ เลย)
#
# v2.18 — จัดหน้าเว็บใหม่ทั้งหมด (ฝั่ง frontend เท่านั้น ไม่แตะ backend):
#   ปัญหาเดิม: เพิ่มฟีเจอร์ทีละอย่างมาเรื่อยๆ (v2.9 Limit order, v2.12 cost-stop,
#   v2.13 order log, v2.17 auto-buy) ทำให้หน้าเว็บกลายเป็นตารางกว้างเกะกะ 12 คอลัมน์
#   ต้องเลื่อนซ้าย-ขวา และช่องตั้งค่า "ซื้ออัตโนมัติ" ตอนเพิ่มหุ้นใหม่หลุดไปอยู่คนละจุดกับ
#   ช่องเพิ่มหุ้นหลัก ดูเหมือนหายไปทั้งที่มีอยู่จริง
#
#   แก้โดย:
#   1) ตาราง Watchlist 12 คอลัมน์ → เปลี่ยนเป็นการ์ดแยกรายหุ้น อ่านง่ายบนมือถือ ไม่ต้อง
#      เลื่อนจอ แบ่งเป็น 2 โซนชัดเจนในการ์ดเดียวกัน: "🔴 เงื่อนไขขาย" กับ "🚀 เงื่อนไขซื้อ"
#   2) "เพิ่มหุ้นใหม่" รวมเป็นการ์ดเดียวจบ มีทั้งชื่อหุ้น + เงื่อนไขขาย + เงื่อนไขซื้อ
#      อยู่ในที่เดียวกันเห็นครบทุกอย่างตั้งแต่แรก ไม่ต้องเลื่อนหาอีกจุดนึง
#   3) จัดลำดับการ์ดใหม่ตามลำดับที่ใช้งานจริงบ่อยที่สุด: สถานะ/พอร์ต → Watchlist (ฟีเจอร์
#      หลัก) → เพิ่มหุ้นใหม่ → เทรดด่วน → ประวัติคำสั่ง → เครื่องมือ debug (เช็คสถานะ/
#      ทดสอบยกเลิก, พอร์ตดิบ) ไว้ล่างสุด เพราะใช้ไม่บ่อย
#   4) element id ทั้งหมด (d_/p_/t_/c_/bv_/oe_ ต่อท้ายชื่อหุ้น, #wlBody ฯลฯ) เหมือนเดิม
#      ทุกตัว กัน JS เดิม (wlVal/updateRow/focusin-focusout กัน polling ทับตอนพิมพ์)
#      พังจากการจัดหน้าใหม่ — ทดสอบแล้วว่ายังทำงานเหมือนเดิมทุกจุด
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
from settrade_v2.errors import SettradeError

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


def _is_waf_block(payload) -> bool:
    """
    ตรวจจับกรณีโดน WAF/Incapsula บล็อกก่อนที่ request จะไปถึง Settrade จริง —
    สังเกตได้จากคำพวกนี้ที่ปนมาในข้อความ error/response แทนที่จะเป็นข้อมูลปกติ
    """
    text = str(payload).lower()
    markers = (
        "incapsula",
        "request unsuccessful",
        "<!doctype html",
        "<html",
        "_incapsula_resource",
        "incident_id",
    )
    return any(m in text for m in markers)


_last_waf_alert = 0
WAF_ALERT_COOLDOWN = 300  # วิ — แจ้ง Telegram ไม่เกิน 1 ครั้งทุก 5 นาที กันสแปม


def _alert_waf_block_throttled(msg):
    global _last_waf_alert
    now = time.time()
    if now - _last_waf_alert > WAF_ALERT_COOLDOWN:
        _last_waf_alert = now
        send_telegram_async(msg)


def api_call_with_retry(bucket, func, *args, **kwargs):
    max_retries = 3
    backoff = 1.0
    for attempt in range(max_retries):
        bucket.acquire()
        try:
            resp = func(*args, **kwargs)

            if _is_waf_block(resp):
                if attempt < max_retries - 1:
                    logger.warning(
                        f"⚠️ [waf-block] โดน WAF/Incapsula บล็อก (ไม่ถึง Settrade จริง) — "
                        f"รอ {backoff:.1f}s แล้วลองใหม่ (รอบ {attempt + 1}/{max_retries})"
                    )
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                _alert_waf_block_throttled(
                    f"❌ [waf-block] โดน WAF บล็อกซ้ำจนครบ {max_retries} รอบ — "
                    f"ต้องตรวจ IP/ช่วงเวลาที่ยิง request"
                )
                raise Exception(f"WAF block ซ้ำจนครบ retry: {str(resp)[:200]}")

            if isinstance(resp, dict):
                sc = resp.get("status_code")
                if sc == 401:
                    if attempt < max_retries - 1:
                        logger.warning("⚠️ Session หมดอายุ(401) กำลัง Login ใหม่...")
                        reconnect_settrade()
                        continue
                    return resp
                if sc in (429, 503, 504, 509) or "bandwidth" in str(resp).lower():
                    if attempt < max_retries - 1:
                        time.sleep(backoff)
                        backoff *= 2
                        continue
                    return resp
                return resp
            return resp

        except SettradeError as e:
            if _is_waf_block(e):
                if attempt < max_retries - 1:
                    logger.warning(f"⚠️ [waf-block] SettradeError ก็โดนบล็อกด้วย — retry รอบ {attempt + 1}")
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                _alert_waf_block_throttled(f"❌ [waf-block] โดนบล็อกซ้ำจนครบรอบ (SettradeError)")
                raise
            sc = getattr(e, "status_code", None)
            msg = str(e).lower()
            if sc == 401 or "401" in msg or "session" in msg or "unauthorized" in msg:
                if attempt < max_retries - 1:
                    logger.warning(f"⚠️ Session หมดอายุ(SettradeError {getattr(e, 'code', '')}) กำลัง Login ใหม่...")
                    reconnect_settrade()
                    continue
                raise
            if sc in (429, 503, 504, 509) or "rate" in msg or "bandwidth" in msg or "too many" in msg or "timeout" in msg:
                if attempt < max_retries - 1:
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                raise
            raise

        except Exception as e:
            if _is_waf_block(e):
                if attempt < max_retries - 1:
                    logger.warning(f"⚠️ [waf-block] โดนบล็อก (generic exception) — retry รอบ {attempt + 1}")
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                _alert_waf_block_throttled(f"❌ [waf-block] โดนบล็อกซ้ำจนครบรอบ (generic exception)")
                raise
            err = str(e).lower()
            if "429" in err or "509" in err or "bandwidth" in err or \
               "503" in err or "504" in err or "rate" in err or \
               "too many" in err or "unavailable" in err or "timeout" in err:
                if attempt < max_retries - 1:
                    time.sleep(backoff)
                    backoff *= 2
                    continue
            elif "401" in err or "session" in err or "unauthorized" in err:
                if attempt < max_retries - 1:
                    logger.warning(f"⚠️ Session หมดอายุ({e}) กำลัง Login ใหม่...")
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
    # v2.17: ฝั่งซื้ออัตโนมัติ (auto-buy) — ดูคอมเมนต์หัวไฟล์
    "buy_active": False, "buy_volume": 0, "offer_eat_pct": 50.0,
}

STALE_POSITION_SECONDS = 180
SELL_ORDER_TIMEOUT = 10
MARKET_OPEN_HHMM = (9, 55)
MARKET_CLOSE_HHMM = (16, 30)
MARKET_LUNCH_START_HHMM = (12, 30)
MARKET_LUNCH_END_HHMM = (14, 30)
BOT_LOOP_INTERVAL = 2
ORDER_LOG_MAX = 20

TICK_LOG_ENABLED = os.getenv("TICK_LOG_ENABLED", "0") == "1"

CHASE_MAX_ROUNDS = 3
CHASE_POLL_INTERVAL = 0.25
CHASE_POLL_TIMEOUT_PER_ROUND = 4.0
CHASE_MIN_ROUND_INTERVAL = 10  # วิ — เว้นระหว่างรอบ chase ตามแนวทาง SET

# v2.14: retry เฉพาะกรณี cancel เจอ "Invalid Order state" — ดูคอมเมนต์หัวไฟล์
CANCEL_STATE_RETRY_MAX = 3
CANCEL_STATE_RETRY_DELAY = 0.50  # วิ

# v2.17: ซื้ออัตโนมัติ (auto-buy) — ดักจังหวะออฟเฟอร์ถูกกินเร็ว (แรงซื้อเข้า)
BUY_MAX_ATTEMPTS_PER_DAY = 3     # ลองซื้อซ้ำได้ไม่เกินกี่ครั้ง/หุ้น/วัน ถ้ารอบก่อนส่งไม่สำเร็จ (กันสัญญาณหลอกยิงรัว)
BUY_MIN_RETRY_INTERVAL = 5       # วิ — เว้นก่อนลองซื้อใหม่ถ้ารอบก่อนล้มเหลว

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
                    "buy_active": bool(cfg.get("buy_active", False)),
                    "buy_volume": int(cfg.get("buy_volume", 0) or 0),
                    "offer_eat_pct": float(cfg.get("offer_eat_pct", DEFAULT_CFG["offer_eat_pct"])),
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
    # สร้าง config ของ SDK ให้อัตโนมัติ (สำคัญ: บอก environment ตลาดจริง/จำลอง)
    try:
        import pathlib
        cfg = pathlib.Path.home() / "settradesdkv2_config.txt"
        sdk_env = os.getenv("SETTRADE_ENV", "uat")  # uat=จำลอง (ปลอดภัย) / prod=จริง
        cfg.write_text(f"environment={sdk_env}\nclear_log=30\n", encoding="utf-8")
    except Exception as e:
        logger.warning(f"config SDK เขียนไม่สำเร็จ: {e}")

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
                            broker_id=broker_id, app_code=app_code,
                            is_auto_queue=True)
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
                item.get("actualVolume")
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

        with lock:                                      # เยื้อง 8
            # v2.20 แก้ race condition: ไม่เขียนทับตัวเลขหุ้นที่ "กำลังไล่ราคาขาย" (selling=
            # True) อยู่ ด้วยค่าจาก Settrade รอบนี้ — เพราะ API ของ Settrade อาจยังอัปเดต
            # ไม่ทันการขายที่ chase-sell เพิ่งหักไปเองแบบสดๆ (ดู _sell_chase_worker) ถ้าปล่อย
            # ให้เขียนทับ อาจได้ตัวเลขเก่า(สูงกว่าจริง)กลับมาแทนที่ตัวเลขที่หักไปแล้วถูกต้อง
            # แล้ว ระหว่าง selling=True เราเชื่อค่าที่ chase-sell หักเองมากกว่า เพราะมันตาม
            # ติดสถานะคำสั่งแบบสดๆ อยู่แล้ว รอบ refresh ปกติ (30 วิ) จะกลับมาซิงก์ให้ถูกต้อง
            # เองหลัง selling กลับเป็น False แล้ว (ไม่มีอะไรค้างตลอดไป)
            for sym, s in state["symbols"].items():
                if s.get("selling") and sym in state["positions"]:
                    pos[sym] = state["positions"][sym]
            state["positions"] = pos                    # เยื้อง 12
            state["avg_cost"] = avg_cost                # เยื้อง 12
            state["pos_updated"] = now                  # เยื้อง 12
        # v2.19: ส่ง raw ที่เพิ่งดึงมาสดๆ ต่อให้เลย ไม่ต้องให้ refresh_account_summary()
        # ไปยิง get_portfolios ซ้ำอีกรอบ (ดูเหตุผลเต็มๆ ที่คอมเมนต์ในฟังก์ชันนั้น)
        refresh_account_summary(portfolio_raw=raw)      # เยื้อง 8 ← ต้องอยู่ตรงนี้!
    except Exception as e:                              # เยื้อง 4
        logger.error(f"get_portfolio error: {e}")       # เยื้อง 8
        
def refresh_account_summary(portfolio_raw=None):
    """
    ดึงเงินสด + กำไร/ขาดทุนรวม + มูลค่าพอร์ต มาเก็บใน state (สำหรับแสดงบน dashboard)
    v2.19: เดิมฟังก์ชันนี้ยิง equity.get_portfolios ของตัวเอง ทั้งที่ refresh_positions()
    ซึ่งเป็นคนเรียกฟังก์ชันนี้ ก็เพิ่งดึงพอร์ตก้อนเดียวกันเป๊ะๆ มาหมาดๆ ก่อนหน้านี้เอง — เป็นการ
    ยิง request ซ้ำซ้อนโดยไม่จำเป็นทุกๆ 30 วิ ถ้ามี portfolio_raw ส่งมาให้ (จาก refresh_positions)
    จะใช้อันนั้นแทนเลย ไม่ยิงซ้ำ ช่วยลดโอกาสโดน rate-limit/WAF บล็อกที่เคยเจอมาก่อน (ดู
    TokenBucket ด้านบนของไฟล์) ยังคง fallback ไปดึงเองได้เหมือนเดิม เผื่อถูกเรียกจากที่อื่น
    ที่ไม่มี raw ส่งมาให้ (ไม่มีจุดเรียกแบบนั้นในโค้ดตอนนี้ แต่กันไว้เผื่ออนาคต)
    """
    try:
        if equity is None:
            return
        acct = api_call_with_retry(query_bucket, equity.get_account_info)
        cash = 0.0
        if isinstance(acct, dict):
            d = acct.get("data", acct)
            cash = d.get("cashBalance") or d.get("cash") or 0.0
        if portfolio_raw is not None:
            port = portfolio_raw
        else:
            port = api_call_with_retry(query_bucket, equity.get_portfolios)
        pnl = 0.0
        mv = 0.0
        if isinstance(port, dict):
            tot = port.get("totalPortfolio", {})
            if tot:
                pnl = tot.get("profit") or 0.0
                mv = tot.get("marketValue") or 0.0
            else:
                for it in port.get("portfolioList", []):
                    pnl += it.get("profit") or 0.0
                    mv += it.get("marketValue") or 0.0
        with lock:
            state["cash"] = cash
            state["pnl"] = pnl
            state["market_value"] = mv
            state["acct_updated"] = time.time()
    except Exception as e:
        logger.error(f"refresh_account_summary error: {e}")

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

        # ถ้า SDK คืน success=False แต่ไม่ throw exception → ถือว่าไม่สำเร็จ
        if isinstance(resp, dict) and str(resp.get("success")).lower() == "false":
            raise Exception(f"API Error: {resp.get('message', resp)}")

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
            # ถ้า SDK คืน success=False (ไม่ throw) → ถือว่ายกเลิกไม่สำเร็จ
            if isinstance(resp, dict) and str(resp.get("success")).lower() == "false":
                raise Exception(f"Cancel API Error: {resp.get('message', resp)}")
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
        if round_num >= 1:
            # SET แนะนำเว้นอย่างน้อย 10 วิ ระหว่างคำสั่งซื้อขายต่อเนื่อง (PTRM)
            time.sleep(CHASE_MIN_ROUND_INTERVAL)
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

# ===================== ซื้ออัตโนมัติ (auto-buy) =====================
# v2.17: สัญญาณซื้อ = ฝั่ง Offer (คนขาย) ถูก "กิน" อย่างรวดเร็วจากแรงซื้อ
#   → วอลุ่มออฟเฟอร์เดิมหายไปเยอะในช่วงเวลาสั้นๆ ในรอบราคาเดียวกัน (คนไล่ซื้อชน offer)
#   → หรือราคาออฟเฟอร์ขยับขึ้นไปเลย (offer เดิมโดนกินหมด ตลาดขึ้นไปยืนราคาถัดไป)
# เมื่อเจอ → ส่งคำสั่งซื้อแบบ Limit ที่ "ราคาออฟเฟอร์ ณ ตอนนั้น" จำนวนตามที่ตั้งไว้ (buy_volume)
# ซื้อได้แค่ 1 ไม้ต่อหุ้นต่อวัน (ตามที่ตกลงกันไว้) แล้วปล่อยให้ trailing/cost-stop เดิมดูแลการขายต่อ

def _buy_worker(symbol, volume, price, pin, tick_ts):
    result = place_order("Buy", symbol, volume, pin, "Limit", price)
    with lock:
        s = state["symbols"].get(symbol)
        if s:
            s["buying"] = False
            if result.get("ok"):
                s["bought_today"] = True
                s["last_action"] = (f"✅ ซื้อ {symbol} {volume} หุ้น @{price} สำเร็จ "
                                     f"(order_no={result.get('order_no')})")
            else:
                s["bought_today"] = False  # ให้ลองใหม่ได้ถ้ายังไม่เกิน BUY_MAX_ATTEMPTS_PER_DAY
                s["buy_attempts"] = s.get("buy_attempts", 0) + 1
                s["last_buy_attempt_ts"] = time.time()
                s["last_action"] = (f"❌ ซื้อ {symbol} {volume} หุ้น @{price} ไม่สำเร็จ: "
                                     f"{str(result.get('msg',''))[:150]}")

    if result.get("ok"):
        t1 = time.time()
        logger.info(
            f"✅ [auto-buy] {symbol} ซื้อสำเร็จ {volume} หุ้น @{price} "
            f"(tick→ซื้อ: {(t1 - tick_ts) * 1000:.0f}ms) order_no={result.get('order_no')}"
        )
        send_telegram_async(
            f"🟢 [auto-buy] {symbol} ซื้อ {volume} หุ้น @{price} สำเร็จ "
            f"(order_no={result.get('order_no')})"
        )
    else:
        logger.warning(f"⚠️ [auto-buy] {symbol} ซื้อไม่สำเร็จ: {result.get('msg')}")
        send_telegram_async(
            f"⚠️ [auto-buy] {symbol} ซื้อ {volume} หุ้น @{price} ไม่สำเร็จ: "
            f"{str(result.get('msg',''))[:200]}"
        )

def _check_symbol_and_maybe_buy(symbol, tick_ts=None):
    if tick_ts is None:
        tick_ts = time.time()

    if not _global_guards_ok():
        return

    pin = os.getenv("SETTRADE_PIN")
    buy_action = None

    with lock:
        cfg = state["watchlist"].get(symbol)
        if not cfg or not cfg.get("buy_active", False):
            return
        buy_volume = int(cfg.get("buy_volume", 0) or 0)
        if buy_volume <= 0:
            return

        s = state["symbols"].get(symbol)
        if not s:
            return
        # v2.19 แก้บั๊ก: เดิมเช็คแค่ buying/bought_today ไม่ได้เช็ค "selling" — ถ้าหุ้นตัวนี้
        # เพิ่งโดน stop-loss ขายทิ้งไป (chase-sell กำลังทำงานอยู่ held จะเหลือ 0 ระหว่างทาง
        # ก่อน worker จบงานจริง) แล้วเงื่อนไขซื้อดันทริกเกอร์พอดีในช่วงนั้น บอทจะซื้อกลับเข้าไป
        # ทันทีทั้งที่ยังขายไม่เสร็จ (มีโอกาสส่งคำสั่งซื้อ-ขายชนกันได้) → เพิ่มเช็ค selling ด้วย
        if s.get("buying") or s.get("bought_today") or s.get("selling"):
            return
        if s.get("buy_attempts", 0) >= BUY_MAX_ATTEMPTS_PER_DAY:
            return
        last_try = s.get("last_buy_attempt_ts", 0)
        if last_try and (time.time() - last_try) < BUY_MIN_RETRY_INTERVAL:
            return

        # กันซื้อซ้อนถ้าถือหุ้นตัวนี้อยู่แล้ว (ไม้เก่ายังไม่ขายออก)
        held = int(state["positions"].get(symbol, 0) or 0)
        if held > 0:
            return

        offers = s.get("offers") or []
        if not offers:
            return
        offer1_price, offer1_vol = offers[0]
        prev_vol = s.get("prev_offer1_vol", 0.0)
        prev_price = s.get("prev_offer1_price", 0.0)
        threshold = float(cfg.get("offer_eat_pct", 50.0))

        eaten_pct = 0.0
        price_moved_up = prev_price > 0 and offer1_price > prev_price
        if prev_vol > 0 and offer1_vol < prev_vol and offer1_price == prev_price:
            eaten_pct = (prev_vol - offer1_vol) / prev_vol * 100

        triggered = price_moved_up or eaten_pct >= threshold

        if triggered:
            # เช็คเงินสดพอไหมก่อนยิงคำสั่งจริง (กันคำสั่ง reject เพราะเงินไม่พอ)
            # v2.19 แก้บั๊ก "fail open": เดิมเช็คแค่ "cash > 0 and est_cost > cash" — ถ้า
            # cash ยังเป็น 0 เพราะ refresh_account_summary() ยังไม่เคยดึงสำเร็จเลย (ไม่ใช่
            # เพราะบัญชีมีเงิน 0 จริง) เงื่อนไขนี้จะเป็นเท็จทันที แล้วปล่อยให้ซื้อผ่านไปเลย
            # ทั้งที่ไม่รู้เลยว่าเงินพอไหม → ตอนนี้เช็คจาก acct_updated (มีค่า timestamp
            # ก็ต่อเมื่อเคยดึงสำเร็จแล้วเท่านั้น) แทน ถ้ายังไม่เคยดึงสำเร็จ ให้บล็อกไว้ก่อน
            # เป็นค่าเริ่มต้น (fail closed) ไม่ใช่ปล่อยผ่าน
            est_cost = offer1_price * buy_volume
            cash = float(state.get("cash", 0) or 0)
            acct_updated = state.get("acct_updated", 0)
            if not acct_updated:
                msg = (f"⚠️ {symbol} เจอสัญญาณซื้อ แต่ยังไม่เคยดึงยอดเงินสดสำเร็จเลย "
                       f"(ไม่รู้ว่าเงินพอไหม) → ข้ามไปก่อนเพื่อความปลอดภัย")
                logger.warning(msg)
                s["last_action"] = msg
                s["prev_offer1_vol"] = offer1_vol
                s["prev_offer1_price"] = offer1_price
            elif est_cost > cash:
                msg = (f"⚠️ {symbol} เจอสัญญาณซื้อ แต่เงินสดไม่พอ "
                       f"(ต้องการ ~{est_cost:,.0f} มี {cash:,.0f}) → ข้าม")
                logger.warning(msg)
                s["last_action"] = msg
                s["prev_offer1_vol"] = offer1_vol
                s["prev_offer1_price"] = offer1_price
            else:
                reason = ("ราคาขยับขึ้น (ออฟเฟอร์เดิมถูกซื้อหมด)" if price_moved_up
                           else f"ออฟเฟอร์หาย {eaten_pct:.1f}%")
                msg = (f"🚀 {symbol} ออฟเฟอร์ {offer1_price} ({reason}) "
                       f"→ ซื้อ {buy_volume} หุ้น @{offer1_price} ทันที!")
                logger.warning(msg)
                s["last_action"] = msg
                s["buying"] = True
                buy_action = (buy_volume, offer1_price)
                s["prev_offer1_vol"] = 0
                s["prev_offer1_price"] = 0
        else:
            s["prev_offer1_vol"] = offer1_vol
            s["prev_offer1_price"] = offer1_price

    if buy_action:
        volume, price = buy_action
        _order_executor.submit(_buy_worker, symbol, volume, price, pin, tick_ts)
        t_decide = time.time()
        logger.info(
            f"⏱️ {symbol} tick→ตัดสินใจซื้อ (auto-buy): {(t_decide - tick_ts) * 1000:.1f}ms"
        )
        send_telegram_async(f"🚀 [auto-buy] {symbol} กำลังส่งคำสั่งซื้อ {volume} หุ้น @{price}")

def check_and_autobuy_all():
    with lock:
        symbols = list(state["watchlist"].keys())
    for symbol in symbols:
        _check_symbol_and_maybe_buy(symbol)

# ===================== สตรีมข้อมูล =====================
def normalize_book(data):
    rows = []
    for item in (data or [])[:5]:
        if isinstance(item, dict):
            rows.append([item.get("price", 0), item.get("volume", 0)])
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            rows.append([item[0], item[1]])
    return rows

def _parse_book_levels(msg, side):
    data = msg.get("data") if isinstance(msg, dict) else {}
    rows = []
    if isinstance(data, dict) and f"{side}_price1" in data:
        for i in range(1, 6):
            price = data.get(f"{side}_price{i}")
            vol = data.get(f"{side}_volume{i}")
            if price is not None:
                rows.append([price, vol or 0])
        return rows
    items = msg.get(f"{side}s") if isinstance(msg, dict) else msg
    for item in (items or [])[:5]:
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
        if _is_stream_msg_ok(msg):
            _mark_stream_tick(tick_ts)
        with lock:
            s = state["symbols"].setdefault(symbol, {})
            s["bids"] = _parse_book_levels(msg, "bid")
            s["offers"] = _parse_book_levels(msg, "ask")
        _check_symbol_and_maybe_sell(symbol, tick_ts=tick_ts)
        _check_symbol_and_maybe_buy(symbol, tick_ts=tick_ts)
    except Exception as e:
        logger.error(f"on_bids_offers error: {e}")

def on_price_info(symbol, msg):
    tick_ts = time.time()
    try:
        if TICK_LOG_ENABLED:
            logger.info(f"📥 tick price เข้า {symbol}: {msg}")
        if _is_stream_msg_ok(msg):
            _mark_stream_tick(tick_ts)
        data = msg.get("data") if isinstance(msg, dict) else {}
        last = 0.0
        if isinstance(data, dict):
            last = data.get("last") or data.get("price") or 0.0
        if not last:
            last = msg.get("last", 0.0) or msg.get("price", 0.0)
        with lock:
            s = state["symbols"].setdefault(symbol, {})
            s["last_price"] = last
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
                ["subscribe_bid_offer", "subscribe_bids_offers", "subscribeBidOffer", "subscribe_bidoffer", "subscribe_bid_offers"],
                "subscribe bid/offer",
            )
            sub_price = _resolve_method(
                realtime,
                ["subscribe_price_info", "subscribePriceInfo", "subscribe_price"],
                "subscribe price info",
            )
            ok_any = False
            if sub_bid_offer:
                sub = sub_bid_offer(sym, partial(on_bids_offers, sym))
                if sub is not None:
                    _active_subs.append(sub)
                    if hasattr(sub, "start"):
                        sub.start()
                ok_any = True
            if sub_price:
                sub = sub_price(sym, partial(on_price_info, sym))
                if sub is not None:
                    _active_subs.append(sub)
                    if hasattr(sub, "start"):
                        sub.start()
                ok_any = True
            if ok_any:
                subscribed.add(sym)
                logger.info(f"📡 สตรีม {sym} แล้ว")
        except Exception as e:
            logger.error(f"subscribe {sym} error: {e}")
            
# ===================== STREAM WATCHDOG (reconnect อัตโนมัติ) =====================
STREAM_WATCHDOG_CHECK = float(os.getenv("STREAM_WATCHDOG_CHECK", "30"))       # วิ — เช็คทุกกี่วิ
STREAM_WATCHDOG_TIMEOUT = float(os.getenv("STREAM_WATCHDOG_TIMEOUT", "120"))  # วิ — ไม่มี tick กี่วิ = ถือว่าตาย

_last_stream_tick = time.time()   # เวลาล่าสุดที่ได้รับข้อมูลจริง
_active_subs = []                 # เก็บ subscription object ไว้ stop ตอน reconnect

def _mark_stream_tick(tick_ts):
    global _last_stream_tick
    _last_stream_tick = tick_ts

def _is_stream_msg_ok(msg):
    """ข้อมูลจากสตรีมใช้ได้ไหม (is_success != False)"""
    if isinstance(msg, dict):
        return str(msg.get("is_success", "true")).lower() != "false"
    return True

def _is_market_time():
    """จ-ศ 09:30-16:45 ตามเวลาไทย (เผื่อ pre-close) — นอกเวลานี้ไม่ reconnect ให้วุ่น"""
    try:
        from datetime import datetime, timedelta
        now = datetime.utcnow() + timedelta(hours=7)
        if now.weekday() >= 5:
            return False
        hm = now.hour * 60 + now.minute
        return 9 * 60 + 30 <= hm <= 16 * 60 + 45
    except Exception:
        return True

def _reconnect_stream():
    """สร้าง RealtimeDataConnection ใหม่ + subscribe ใหม่ทั้งหมด"""
    global realtime, _last_stream_tick
    logger.warning("🔄 สตรีมเงียบเกินกำหนด — กำลัง reconnect ใหม่...")
    try:
        for sub in list(_active_subs):
            try:
                if hasattr(sub, "stop"):
                    sub.stop()
            except Exception:
                pass
        _active_subs.clear()
        subscribed.clear()  # ให้ ensure_subscribe รู้ว่าต้อง subscribe ใหม่
        realtime = investor.RealtimeDataConnection()
        with lock:
            syms = [s for s, c in state["watchlist"].items() if c.get("active", True)]
        ensure_subscribe(syms)
        _last_stream_tick = time.time()
        logger.info("✅ Reconnect สตรีมเรียบร้อย")
    except Exception as e:
        logger.error(f"reconnect สตรีมไม่สำเร็จ: {e}")

def stream_watchdog():
    while True:
        time.sleep(STREAM_WATCHDOG_CHECK)
        try:
            if investor is None:
                continue
            if not _is_market_time():
                continue
            with lock:
                if not state["watchlist"]:
                    continue
            if realtime is None or time.time() - _last_stream_tick > STREAM_WATCHDOG_TIMEOUT:
                _reconnect_stream()
        except Exception as e:
            logger.error(f"stream_watchdog error: {e}")

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
            # v2.17: รีเซ็ต baseline + สิทธิ์ซื้อของฝั่ง auto-buy ทุกวันเทรดใหม่
            s["prev_offer1_vol"] = 0
            s["prev_offer1_price"] = 0
            s["buying"] = False
            s["bought_today"] = False
            s["buy_attempts"] = 0
            s["last_buy_attempt_ts"] = 0
        subscribed.clear()
    logger.info("📅 เข้าสู่วันเทรดใหม่ → รีเซ็ต baseline บิดหาย%/ราคาตก%/สัญญาณซื้อ และบังคับ subscribe ใหม่")
    
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

def _is_pre_market_query_window():
    """
    อนุญาตให้ query พอร์ต/บัญชีได้ตั้งแต่ 9:00 เป็นต้นไป (ก่อนตลาดเปิดจริง ~55 นาที
    พอสำหรับเตรียมข้อมูล) — ตัดการยิง query รัวๆ ช่วงตี 5-9 โมงที่ไม่จำเป็นออกไป
    ลดความถี่ที่ทำให้ WAF สงสัยว่าเป็นบอท
    """
    now = get_bkk_now()
    if now.weekday() >= 5:
        return False
    hm = now.hour * 60 + now.minute
    return hm >= 9 * 60  # ตั้งแต่ 9:00

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

            if _is_pre_market_query_window():
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
        
    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()

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
                        "buying": sd.get("buying", False),
                        "bought_today": sd.get("bought_today", False),
                        "avg_cost": state["avg_cost"].get(sel, 0),
                    },
                    "watchlist": state["watchlist"],
                    "positions": state["positions"],
                    "avg_cost": state["avg_cost"],
                    "order_log": state["order_log"],
                    "cash": state.get("cash", 0),
                    "pnl": state.get("pnl", 0),
                    "market_value": state.get("market_value", 0),
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
                "buy_active": bool(data.get("buy_active", False)),
                "buy_volume": int(data.get("buy_volume", 0) or 0),
                "offer_eat_pct": float(data.get("offer_eat_pct", DEFAULT_CFG["offer_eat_pct"])),
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
                "buy_active": bool(data.get("buy_active", False)),
                "buy_volume": int(data.get("buy_volume", 0) or 0),
                "offer_eat_pct": float(data.get("offer_eat_pct", DEFAULT_CFG["offer_eat_pct"])),
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
  .card-title { font-weight:bold; margin-bottom:10px; font-size:15px; }
  .card-hint { font-size:11px; color:#64748b; margin-bottom:8px; line-height:1.5; }
  .row { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
  .grow { flex:1; min-width:0; }
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
  table { border-collapse:collapse; font-size:13px; width:100%; }
  th { color:#94a3b8; font-size:11px; padding:4px; text-align:center; white-space:nowrap; }
  td { padding:5px; text-align:center; border-top:1px solid #1e293b; }
  .mono { font-variant-numeric:tabular-nums; }
  .log { background:#0b1220; border:1px solid #263449; border-radius:8px; padding:8px; font-size:12px; color:#93c5fd; min-height:20px; white-space:pre-wrap; word-break:break-word; }
  .modal-bg { display:none; position:fixed; inset:0; background:rgba(0,0,0,.7); z-index:50; align-items:center; justify-content:center; }
  .modal { background:#1e293b; border:1px solid #475569; border-radius:14px; padding:20px; max-width:340px; width:92%; }
  .order-row { display:flex; justify-content:space-between; gap:8px; font-size:12px; padding:6px 0; border-top:1px solid #1e293b; align-items:flex-start; }
  .order-row:first-child { border-top:none; }
  .ok-yes { color:#4ade80; } .ok-no { color:#f87171; } .ok-pending { color:#facc15; }
  .btn-cancel-mini { padding:3px 7px; font-size:10px; margin-left:6px; background:#991b1b; border-radius:6px; }

  /* v2.18: การ์ดรายหุ้นแทนตารางกว้าง */
  .stock-card { background:#0e1729; border:1px solid #223049; border-radius:12px; padding:12px; margin-bottom:10px; }
  .stock-card:last-child { margin-bottom:0; }
  .stock-card-header { display:flex; justify-content:space-between; align-items:flex-start; gap:8px; flex-wrap:wrap; }
  .stock-card-title { font-size:17px; font-weight:800; }
  .stock-card-actions { display:flex; gap:6px; flex-wrap:wrap; }
  .chip { padding:6px 10px; font-size:12px; border-radius:20px; width:auto; }
  .chip-watch-on { background:#064e3b; color:#34d399; }
  .chip-watch-off { background:#334155; color:#94a3b8; }
  .chip-buy-on { background:#1e3a8a; color:#93c5fd; }
  .chip-buy-off { background:#334155; color:#64748b; }
  .chip-del { background:#7f1d1d; color:#fca5a5; padding:6px 9px; }
  .stock-card-meta { font-size:12px; color:#94a3b8; margin:6px 0 10px; }
  .section-label { font-size:10.5px; color:#64748b; font-weight:700; margin:10px 0 6px; text-transform:uppercase; letter-spacing:.04em; }
  .section-label:first-of-type { margin-top:0; }
  .field-grid { display:grid; grid-template-columns:repeat(2, 1fr); gap:8px; }
  .field-grid label { font-size:11px; color:#94a3b8; display:block; }
  .field-grid input { margin-top:3px; padding:8px; font-size:14px; }
  .empty-hint { color:#64748b; font-size:13px; padding:8px 0; }
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
        <span id="buyingBadge" class="badge on" style="display:none;">🚀 กำลังส่งคำสั่งซื้อ...</span>
      </div>
      <button id="toggleBtn" class="btn-toggle" style="width:110px;" onclick="toggleBot()">⏸ ปิดบอท</button>
    </div>
    <div class="log" id="actionLog" style="margin-top:10px;">รอข้อมูล...</div>
  </div>

   <!-- โซน 6: จอบิด/ออฟเฟอร์ของหุ้นที่เลือก -->
  <div class="card">
    <div class="row" style="margin-bottom:8px;">
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

  <!-- โซน 3: Watchlist — ฟีเจอร์หลัก ย้ายขึ้นมาไว้บนสุด (เดิมอยู่ล่างสุด) -->
  <div class="card">
    <div class="card-title">📋 รายการเฝ้า (Watchlist)</div>
    <div class="card-hint">
      🔴 <b>เงื่อนไขขาย</b> — บิดหาย% หรือ ราคาตก% หรือ ขาดทุนจากต้นทุน% (ถึงอันไหนก่อนขายอันนั้น) → ไล่ราคาขายหมดพอร์ตด้วย MP-MTL<br>
      🚀 <b>เงื่อนไขซื้อ</b> — ออฟเฟอร์หายเร็ว%เกินที่ตั้ง (หรือราคาขยับขึ้น) → ซื้อ Limit ที่ราคาออฟเฟอร์ตอนนั้น ซื้อได้ 1 ไม้/หุ้น/วัน ต้องกดปุ่ม 🚀 เปิดก่อนถึงจะทำงาน<br>
      🔒 = ถืออยู่จริง ระบบเพิ่ม/ลบให้อัตโนมัติตามพอร์ต (ลบเองแล้วจะเพิ่มกลับถ้ายังถือของอยู่ — ใช้ปุ่มเฝ้า⚪ ถ้าอยากหยุดเฝ้าโดยไม่ลบ) — หุ้นที่กด➕เพิ่มเอง ลบได้อิสระ
    </div>
    <div id="wlBody"></div>
  </div>

  <!-- โซน 9: พอร์ตปัจจุบัน (ดิบๆ ไม่ผ่านการกรอง — ไว้อ้างอิง) -->
  <div class="card">
    <div class="card-title">💼 พอร์ตปัจจุบัน (จาก Settrade ตรงๆ)</div>
    <div id="portBody" style="font-size:13px;color:#94a3b8;">กำลังโหลด...</div>
  </div>

  <!-- โซน 2: สรุปพอร์ต -->
  <div class="card">
    <div class="card-title">💰 สรุปพอร์ต</div>
    <div style="display:flex;gap:8px;margin-bottom:8px;">
      <div style="flex:1;background:#1e293b;border-radius:8px;padding:10px;">
        <div style="font-size:12px;color:#94a3b8;">เงินสด</div>
        <div id="cashVal" style="font-size:17px;font-weight:bold;">--</div>
      </div>
      <div style="flex:1;background:#1e293b;border-radius:8px;padding:10px;">
        <div style="font-size:12px;color:#94a3b8;">มูลค่าพอร์ต</div>
        <div id="mvVal" style="font-size:17px;font-weight:bold;">--</div>
      </div>
    </div>
    <div style="background:#1e293b;border-radius:8px;padding:10px;">
      <div style="font-size:12px;color:#94a3b8;">กำไร/ขาดทุน</div>
      <div id="pnlVal" style="font-size:19px;font-weight:bold;">--</div>
    </div>
  </div>

  <!-- โซน 4: เพิ่มหุ้นใหม่ — รวมเป็นการ์ดเดียว เห็นครบทั้งเงื่อนไขขาย/ซื้อในที่เดียว -->
  <div class="card">
    <div class="card-title">➕ เพิ่มหุ้นใหม่</div>
    <div class="card-hint">ไว้ทดสอบ/เฝ้าก่อนซื้อ หรือเปิดซื้ออัตโนมัติทันทีจากที่นี่เลยก็ได้</div>
    <label>ชื่อหุ้น</label>
    <input id="newSym" placeholder="เช่น AOT" style="text-transform:uppercase;margin-bottom:12px;">

    <div class="section-label">🔴 เงื่อนไขขาย</div>
    <div class="field-grid">
      <label>บิดหาย% <input id="newDrop" type="number" value="60"></label>
      <label>ราคาตก% <input id="newPriceDrop" type="number" step="0.1" value="1.0"></label>
      <label>Trail% <input id="newTrail" type="number" step="0.1" value="1.0"></label>
      <label>ขาดทุนจากต้นทุน% <input id="newCostStop" type="number" step="0.1" value="1.0"></label>
    </div>

    <div class="section-label">🚀 เงื่อนไขซื้อ (ไม่บังคับ — เว้น 0 คือปิดไว้ก่อน)</div>
    <div class="field-grid">
      <label>จำนวนซื้อ (หุ้น) <input id="newBuyVol" type="number" value="0" placeholder="0 = ปิด"></label>
      <label>ออฟเฟอร์หาย% ถึงซื้อ <input id="newOfferEat" type="number" step="1" value="50"></label>
    </div>

    <button class="btn-buy" style="width:100%;margin-top:14px;" onclick="addSymbol()">➕ เพิ่มหุ้นนี้เข้า Watchlist</button>
  </div>

  <!-- โซน 5: เทรดด่วน -->
  <div class="card">
    <div class="card-title">⚡ เทรดด่วน</div>
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
    <div class="row" style="margin-top:12px;flex-wrap:nowrap;">
      <button class="btn-buy grow" onclick="askOrder('Buy')">🟢 ซื้อ</button>
      <button class="btn-sell grow" onclick="askOrder('Sell')">🔴 ขาย</button>
    </div>
  </div>

  <!-- โซน 8: เครื่องมือ debug — เช็คสถานะ/ทดสอบยกเลิก (ใช้ไม่บ่อย ไว้ล่างๆ) -->
  <div class="card">
    <div class="card-title">🔍 เช็คสถานะ / 🧪 ทดสอบยกเลิกคำสั่ง</div>
    <div class="card-hint">
      เครื่องมือ debug — ไม่ผูกกับ chase-sell อัตโนมัติ (ของเดิมใช้ signature ที่ยืนยันแล้วอยู่แล้ว)
      พิมพ์ Order No เองหรือกด 🚫 จากตารางประวัติคำสั่งด้านบนก็ได้ "🔍 เช็คสถานะ" ไม่มีผลข้างเคียง
      และไม่ต้องใช้ PIN แนะนำให้กดก่อนเสมอก่อนจะลอง "🧪 ทดสอบยกเลิก" ซึ่งต้องใช้ PIN และมีผลจริง
      กับออเดอร์ — ใส่ PIN เป็นตัวเลขล้วนๆ เท่านั้น
    </div>
    <div class="row">
      <div class="grow"><label>Order No</label><input id="cancelOrderNo" placeholder="เช่น 64UJS0PUXL"></div>
      <div class="grow"><label>หุ้น (ไม่บังคับ)</label><input id="cancelSymbol" placeholder="เช่น UKEM"></div>
    </div>
    <label>PIN (ใช้เฉพาะตอนกดทดสอบยกเลิก)</label>
    <input id="cancelPin" type="password" inputmode="numeric">
    <div class="row" style="margin-top:10px;flex-wrap:nowrap;">
      <button class="btn-info grow" onclick="checkStatus()">🔍 เช็คสถานะ</button>
      <button class="btn-sell grow" onclick="testCancel()">🧪 ทดสอบยกเลิก</button>
    </div>
    <div id="cancelResult" class="log" style="margin-top:10px;display:none;"></div>
  </div>

  <!-- โซน 7: ประวัติคำสั่งซื้อขาย -->
  <div class="card">
    <div class="card-title">🧾 ประวัติคำสั่ง (ล่าสุด 20 รายการ)</div>
    <div class="card-hint">ตอนไล่ราคาขายอัตโนมัติ (chase-sell) แต่ละรอบจะขึ้นเป็นคนละแถวในนี้ — กด 🚫 เพื่อดึง Order No ไปกรอกในช่องทดสอบยกเลิก/เช็คสถานะด้านล่าง</div>
    <div id="orderLogBody" style="font-size:12px;color:#64748b;">ยังไม่มีคำสั่ง</div>
  </div>

  <div class="modal-bg" id="modalBg">
    <div class="modal">
      <div style="font-size:16px;font-weight:bold;margin-bottom:8px;" id="modalTitle">ยืนยัน</div>
      <div id="modalBody" style="font-size:14px;color:#cbd5e1;margin-bottom:14px;"></div>
      <div class="row" style="flex-wrap:nowrap;">
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
let wlBuyActiveCache={};
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

// กันบั๊ก: polling ทุก 1 วิ เคย rebuild การ์ด watchlist ทับช่องที่กำลังพิมพ์อยู่จนพิมพ์ไม่ติด
// ใช้ focusin/focusout แบบ event delegation เพราะการ์ดในนี้เพิ่ม/หายเองได้จากการ sync พอร์ต
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

    document.getElementById('cashVal').textContent = s.cash?fmt(s.cash):'--';
    document.getElementById('mvVal').textContent = s.market_value?fmt(s.market_value):'--';
    const pnlEl = document.getElementById('pnlVal');
    pnlEl.textContent = (s.pnl!=null)?fmt(s.pnl):'--';
    pnlEl.className = (s.pnl>0?'green':(s.pnl<0?'red':''));

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
    document.getElementById('buyingBadge').style.display = d.buying ? '' : 'none';
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
        wl.innerHTML='<div class="empty-hint">ยังไม่มีหุ้นในรายการเฝ้า — ถ้าถือหุ้นอยู่จะเพิ่มให้อัตโนมัติ หรือกด ➕ เพิ่มเองด้านล่างเพื่อทดสอบ</div>';
      } else {
        let whtml='';
        for(const k of keys){
          const c=s.watchlist[k]||{};
          const held=(s.positions&&s.positions[k])||0;
          const cost=(s.avg_cost&&s.avg_cost[k])||0;
          const autoTag=(!c.pinned && held>0)?' 🔒':'';
          wlActiveCache[k]=!!c.active;
          wlBuyActiveCache[k]=!!c.buy_active;
          whtml+=`
          <div class="stock-card">
            <div class="stock-card-header">
              <div class="stock-card-title">${k}${autoTag}</div>
              <div class="stock-card-actions">
                <button class="chip ${c.active?'chip-watch-on':'chip-watch-off'}" onclick="toggleActive('${k}')">${c.active?'🟢 เฝ้าอยู่':'⚪ ปิดเฝ้า'}</button>
                <button class="chip ${c.buy_active?'chip-buy-on':'chip-buy-off'}" onclick="toggleBuyActive('${k}')">${c.buy_active?'🚀 ซื้ออัตโนมัติ':'⚪ ปิดซื้อ'}</button>
                <button class="chip chip-del" onclick="askRemove('${k}')">🗑</button>
              </div>
            </div>
            <div class="stock-card-meta">ถือ ${held} หุ้น${cost?(' • ต้นทุน '+fmt(cost)):''}</div>

            <div class="section-label">🔴 เงื่อนไขขาย</div>
            <div class="field-grid">
              <label>บิดหาย%<input id="d_${k}" type="number" value="${wlVal('d_'+k, c.bid_drop_pct)}" onchange="updateRow('${k}')"></label>
              <label>ราคาตก%<input id="p_${k}" type="number" step="0.1" value="${wlVal('p_'+k, c.price_drop_pct!=null?c.price_drop_pct:1.0)}" onchange="updateRow('${k}')"></label>
              <label>Trail%<input id="t_${k}" type="number" step="0.1" value="${wlVal('t_'+k, c.trailing_pct)}" onchange="updateRow('${k}')"></label>
              <label>ขาดทุนจากต้นทุน%<input id="c_${k}" type="number" step="0.1" value="${wlVal('c_'+k, c.cost_stop_pct!=null?c.cost_stop_pct:1.0)}" onchange="updateRow('${k}')"></label>
            </div>

            <div class="section-label">🚀 เงื่อนไขซื้อ</div>
            <div class="field-grid">
              <label>จำนวนซื้อ (หุ้น)<input id="bv_${k}" type="number" value="${wlVal('bv_'+k, c.buy_volume||0)}" onchange="updateRow('${k}')"></label>
              <label>ออฟเฟอร์หาย%<input id="oe_${k}" type="number" step="1" value="${wlVal('oe_'+k, c.offer_eat_pct!=null?c.offer_eat_pct:50)}" onchange="updateRow('${k}')"></label>
            </div>
          </div>`;
        }
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
  const buyVol=Number(document.getElementById('newBuyVol').value||0);
  const body={symbol,bid_drop_pct:document.getElementById('newDrop').value,price_drop_pct:document.getElementById('newPriceDrop').value,trailing_pct:document.getElementById('newTrail').value,cost_stop_pct:document.getElementById('newCostStop').value,buy_volume:buyVol,offer_eat_pct:document.getElementById('newOfferEat').value,buy_active:buyVol>0};
  const res=await (await fetch('/api/watchlist/add',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)})).json();
  alert(res.ok?'✅ เพิ่ม '+symbol+' แล้ว':'❌ '+res.msg);
}
async function updateRow(sym){
  const now=Date.now();
  ['d_','p_','t_','c_','bv_','oe_'].forEach(pfx=>{ wlEditedAt[pfx+sym]=now; });
  const activeState = wlActiveCache[sym]!==false;
  const buyActiveState = !!wlBuyActiveCache[sym];
  try{
    const res = await (await fetch('/api/watchlist/update',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({
      symbol:sym,bid_drop_pct:document.getElementById('d_'+sym).value,price_drop_pct:document.getElementById('p_'+sym).value,trailing_pct:document.getElementById('t_'+sym).value,cost_stop_pct:document.getElementById('c_'+sym).value,buy_volume:document.getElementById('bv_'+sym).value,offer_eat_pct:document.getElementById('oe_'+sym).value,active:activeState,buy_active:buyActiveState
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
    symbol:sym,bid_drop_pct:c.bid_drop_pct,price_drop_pct:c.price_drop_pct,trailing_pct:c.trailing_pct,cost_stop_pct:c.cost_stop_pct,buy_volume:c.buy_volume,offer_eat_pct:c.offer_eat_pct,active:!c.active,buy_active:c.buy_active
  })});
}
async function toggleBuyActive(sym){
  const r=await (await fetch('/api/state')).json();
  const c=r.watchlist[sym]||{};
  if(!c.buy_active && (!c.buy_volume || Number(c.buy_volume)<=0)){
    alert('ตั้ง "จำนวนซื้อ" ให้มากกว่า 0 ก่อนเปิดซื้ออัตโนมัติ');
    return;
  }
  await fetch('/api/watchlist/update',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({
    symbol:sym,bid_drop_pct:c.bid_drop_pct,price_drop_pct:c.price_drop_pct,trailing_pct:c.trailing_pct,cost_stop_pct:c.cost_stop_pct,buy_volume:c.buy_volume,offer_eat_pct:c.offer_eat_pct,active:c.active,buy_active:!c.buy_active
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
    threading.Thread(target=session_keeper, daemon=True).start()   # ← เพิ่มบรรทัดนี้
    threading.Thread(target=stream_watchdog, daemon=True).start()
    port = int(os.getenv("PORT", 10000))
    server = ThreadingHTTPServer(("0.0.0.0", port), DashboardHandler)
    logger.info(f"🌐 Dashboard: http://0.0.0.0:{port}")
    server.serve_forever()

if __name__ == "__main__":
    main()