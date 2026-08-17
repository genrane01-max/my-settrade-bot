# ==============================================================================
# SETTRADE BOT v2.9 — Watchlist หลายหุ้น + Trailing % + เทรด MP-MTL/Limit
# - แต่ละหุ้นมีค่าเอง (บิดหาย%, trailing%) เก็บใน Firebase /watchlist/<SYMBOL>
# - ดึงจำนวนหุ้นจากพอร์ตอัตโนมัติ → เจอเหตุการณ์ขายหมดพอร์ต
# - ทดสอบ Sandbox ก่อน (SETTRADE_APP_CODE=SANDBOX) แล้วค่อยใช้ ALGO
# - รองรับหลายโบรกเกอร์ผ่าน SETTRADE_BROKER_ID (env var) — ไม่ผูกกับโบรกใดโบรกหนึ่ง
#
# แก้ไข: เพิ่มระบบ "หา method อัตโนมัติ" (_resolve_method) เพราะ SDK settrade_v2
# ใช้ชื่อ method ไม่ตรงกับที่เอกสาร/ตัวอย่างเก่าบอกไว้เป๊ะๆ (เช่น subscribe_bids_offers
# vs subscribe_bid_offer, get_portfolio อาจไม่มีตรงๆ) ตัวช่วยนี้จะลองชื่อที่เป็นไปได้
# หลายแบบ ถ้าไม่เจอเลยจะ log รายชื่อ method จริงที่มีอยู่ใน object นั้นออกมาที่ Render
# logs ให้เห็นเลย จะได้แก้ให้ตรงเป๊ะได้ในทีเดียว
#
# v2.1-v2.7: ดูรายละเอียดในไฟล์เวอร์ชันก่อนหน้า (รีเซ็ตข้ามวัน, trailing persist,
# input polling fix, watchlist auto-sync กับพอร์ต, event-driven check, order log,
# lunch break, modal confirm แทน native confirm())
#
# v2.8: แก้คอขวดความเร็ว — save_trailing/send_telegram/place_order (auto-sell) เป็น
# fire-and-forget ผ่าน thread pool แยก ไม่ block thread ที่กำลังเช็คหุ้นตัวอื่น
#
# v2.9 เพิ่มตัวเลือก "จำกัดราคา (Limit)" ในปุ่มเทรดด่วนของหน้าเว็บ
#   - เดิม /api/order ยิง MP-MTL (ราคาตลาด) เสมอ ไม่มีทางระบุราคาเอง ถ้าหุ้นตัวนั้น
#     กระดานบางมาก ซื้อ 100 หุ้นอาจไล่ราคาขึ้นหลายช่วงราคา ต้นทุนจริงสูงกว่าที่คิดไว้มาก
#   - เพิ่ม dropdown "ประเภทคำสั่ง" (ตลาด/จำกัดราคา) — เลือก "จำกัดราคา" แล้วจะโชว์
#     ช่องกรอกราคา บังคับกรอกก่อนยืนยันได้ (ห้ามเป็น 0/ว่าง)
#   - place_order() เพิ่ม parameter price รับค่าจริงจากหน้าเว็บ (เดิม hardcode 0.0
#     เสมอ ทำให้ Limit ไม่มีทางทำงานถูกได้เลยแม้จะส่ง price_type="Limit" ไปก็ตาม)
#   - สำคัญ: ตรรกะ auto-sell (_check_symbol_and_maybe_sell → place_order_fire_and_forget)
#     ยังคงยิง MP-MTL เท่านั้นเหมือนเดิมทุกประการ ไม่เปิดให้เลือกราคาเด็ดขาด เพราะเป็น
#     กลไกความปลอดภัยที่ต้องการ "รับประกันว่าขายได้" ตอนเกิดเหตุฉุกเฉิน การใส่ราคาจำกัด
#     เข้าไปจะขัดกับจุดประสงค์เดิม (อาจขายไม่ออกเพราะราคาที่ตั้งไว้ไม่มีคนรับซื้อ)
#     Limit ใช้ได้เฉพาะเทรดด่วนที่คุณกดเองในหน้าเว็บเท่านั้น
#
# v2.10 แก้: v2.9 เคย auto-fill ช่องราคาด้วยราคาล่าสุดที่ "เลือกหุ้นดูจอ" (dropdown บนสุด)
#   กำลังแสดงอยู่ — แต่ช่อง "หุ้น" ในเทรดด่วนกับ dropdown นั้นเป็นคนละตัวแปรกัน (ตั้งใจแยก
#   กันไว้ตั้งแต่แรกกันพิมพ์โดน overwrite) ถ้าผู้ใช้พิมพ์ชื่อหุ้นเองในช่องเทรดด่วนโดยไม่ได้
#   เลือกจาก dropdown ก่อน ราคาที่ auto-fill มาอาจเป็นของหุ้นคนละตัว เสี่ยงกดซื้อผิดราคาไป
#   โดยไม่รู้ตัว → เอา auto-fill ออกทั้งหมด ช่องราคาว่างเปล่าเสมอ บังคับพิมพ์เองทุกครั้ง
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
    "selected": "",            # หุ้นที่กำลังดูจอบิด/ออฟเฟอร์
    "watchlist": {},          # { SYMBOL: {bid_drop_pct, trailing_pct, price_drop_pct, active, pinned} }
    "positions": {},          # { SYMBOL: จำนวนหุ้นที่ถือ } — ดิบๆ จาก get_portfolios() ตรงๆ
    "symbols": {},            # { SYMBOL: {bids, offers, last_price, highest, stop, prev_bid1_vol, drop, last_action} }
    "pos_updated": 0,
    "order_log": [],          # ประวัติคำสั่งซื้อ/ขาย 20 รายการล่าสุด
}
subscribed = set()  # หุ้นที่สตรีมอยู่แล้ว
_last_trading_date = None  # วันเทรดล่าสุดที่เคยรีเซ็ต baseline ไปแล้ว (เวลาไทย)

# pinned=False หมายถึงหุ้นที่ระบบเพิ่มเข้ามาเองเพราะเจอในพอร์ต (auto)
# pinned=True หมายถึงหุ้นที่กด + เพิ่มเองผ่านหน้าเว็บ (manual/ไว้ทดสอบ)
DEFAULT_CFG = {"bid_drop_pct": 60.0, "trailing_pct": 1.0, "price_drop_pct": 1.0, "active": True, "pinned": False}

# ---- ค่าตั้งความปลอดภัย (แก้ตามคุยกัน) ----
STALE_POSITION_SECONDS = 180   # ถ้าดึงพอร์ตไม่สำเร็จเกิน 3 นาที → หยุดเทรดอัตโนมัติชั่วคราว
SELL_ORDER_TIMEOUT = 2         # วินาที — รอคำตอบคำสั่งขายไม่เกินนี้ ไม่บล็อก loop หลัก (ไม่ยิงซ้ำอัตโนมัติ)
MARKET_OPEN_HHMM = (9, 55)     # เผื่อช่วง pre-open/ATO ก่อนตลาดเปิดจริง 10:00
MARKET_CLOSE_HHMM = (16, 40)   # เผื่อช่วง ATC/หลังปิด
MARKET_LUNCH_START_HHMM = (12, 30)
MARKET_LUNCH_END_HHMM = (14, 30)
BOT_LOOP_INTERVAL = 2          # วิ — งานพื้นหลัง (sync watchlist/positions) ไม่ต้องไวเท่าเช็คขาย
ORDER_LOG_MAX = 20             # เก็บประวัติคำสั่งซื้อ/ขายไว้กี่รายการล่าสุด
TICK_LOG_ENABLED = os.getenv("TICK_LOG_ENABLED", "0") == "1"

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
def get_bkk_now():
    return datetime.datetime.utcnow() + datetime.timedelta(hours=7)

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
        raw = _call_flexible(get_port, account_no)

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

        if not pos and raw:
            list_is_genuinely_empty = (
                isinstance(raw, dict) and portfolio_list_key_used is not None and items == []
            )
            if not list_is_genuinely_empty:
                logger.warning(f"[get_portfolio] ได้ raw data กลับมาแต่แปลงเป็น position ไม่ได้: {str(raw)[:500]}")

        with lock:
            state["positions"] = pos
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

def place_order(side, symbol, volume, pin, price_type="MP-MTL", price=0.0):
    """
    side='Buy'/'Sell'
    price_type="MP-MTL" (ค่าเริ่มต้น) = ราคาตลาด, price ไม่มีผลใดๆ ส่งเป็น 0.0 เสมอ
    price_type="Limit" = จำกัดราคาเอง ต้องส่ง price เป็นราคาที่ต้องการ (บาท) มาด้วย
    v2.9: เพิ่ม parameter price จริงๆ (เดิม hardcode 0.0 ทุกครั้งไม่ว่า price_type จะเป็นอะไร
    ทำให้ "Limit" ไม่เคยทำงานถูกได้เลยแม้โค้ดจะรับ price_type มาเป็นพารามิเตอร์ก็ตาม)
    """
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
    }
    if equity is None:
        entry["ok"] = False
        entry["msg"] = "ยังไม่ได้เชื่อมต่อ Settrade"
        _record_order(entry)
        return {"ok": False, "msg": entry["msg"]}
    try:
        resp = equity.place_order(
            side=side,
            symbol=symbol.upper().strip(),
            trustee_id_type=os.getenv("SETTRADE_TRUSTEE_ID", "Local"),
            volume=int(volume),
            price_type=price_type,
            price=use_price,
            validity_type="Day",
            pin=pin,
        )
        price_note = f" @{use_price}" if price_type == "Limit" else ""
        msg = f"📤 {side} {symbol} {volume}{price_note} ({price_type})\nตอบ: {resp}"
        logger.info(msg)
        send_telegram(msg)
        entry["ok"] = True
        entry["msg"] = str(resp)
        _record_order(entry)
        return {"ok": True, "msg": str(resp)}
    except Exception as e:
        logger.error(f"place_order error: {e}")
        entry["ok"] = False
        entry["msg"] = str(e)
        _record_order(entry)
        return {"ok": False, "msg": str(e)}

def _log_late_order_result(side, symbol, volume, future):
    try:
        res = future.result()
        logger.info(f"📬 ผลคำสั่งที่มาช้า: {side} {symbol} {volume} → {res}")
        send_telegram(f"📬 ผลคำสั่งที่ส่งไปก่อนหน้า (ตอบช้ากว่า {SELL_ORDER_TIMEOUT}s): {side} {symbol} {volume}\n{res.get('msg', res)}")
    except Exception as e:
        logger.error(f"late order result error: {e}")

def place_order_async(side, symbol, volume, pin, price_type="MP-MTL", price=0.0, timeout=SELL_ORDER_TIMEOUT):
    """ ใช้ path ที่ต้องการผลลัพธ์ทันที (รอไม่เกิน timeout วิ) — ไม่ได้ใช้ในตรรกะ auto-sell """
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

def place_order_fire_and_forget(side, symbol, volume, pin, price_type="MP-MTL", price=0.0, tick_ts=None):
    """
    ใช้เฉพาะ path auto-sell ที่ยิงมาจาก websocket callback — ยิง MP-MTL เท่านั้นเสมอ
    (ไม่มีทางเรียกด้วย price_type="Limit" จากตรรกะ auto-sell เลย เพราะเป็นกลไกความ
    ปลอดภัยที่ต้องการรับประกันว่าขายได้ตอนเกิดเหตุฉุกเฉิน ใส่ราคาจำกัดจะขัดจุดประสงค์)
    ไม่รอผลเลยแม้แต่เสี้ยววิ ผล/log/telegram เกิดขึ้นเบื้องหลังทั้งหมดผ่าน place_order()
    """
    def _run():
        t0 = time.time()
        result = place_order(side, symbol, volume, pin, price_type, price)
        t1 = time.time()
        if tick_ts is not None:
            logger.info(
                f"⏱️ {symbol} {side} เวลารวม tick→ได้ผลคำสั่งจริงจาก Settrade: "
                f"{(t1 - tick_ts) * 1000:.0f}ms (ในนั้นรอ Settrade API ตอบ {(t1 - t0) * 1000:.0f}ms) "
                f"→ {str(result.get('msg', ''))[:100]}"
            )
    _order_executor.submit(_run)

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
                       f"→ ขาย {held} หุ้น (หมดพอร์ต) ทันที!")
                logger.warning(msg)
                s["last_action"] = msg
                s["prev_bid1_vol"] = 0
                s["prev_bid1_price"] = 0
                state["positions"][symbol] = 0
                sell_action = (msg, held)
            else:
                s["prev_bid1_vol"] = bid1_vol
                s["prev_bid1_price"] = bid1_price

        if sell_action is None:
            last = s.get("last_price", 0.0)
            if last > 0:
                highest = s.get("highest", 0.0)
                if last > highest:
                    s["highest"] = last
                    s["stop"] = round(last * (1 - trailing_pct / 100.0), 2)
                    trailing_to_persist = (symbol, s["highest"], s["stop"])
                stop = s.get("stop", 0.0)
                if stop > 0 and last <= stop:
                    msg = (f"🛑 {symbol} ราคา {last} ตกถึงจุดขาย {stop} "
                           f"(สูงสุด {s['highest']} -{trailing_pct}%) → ขาย {held} หุ้น")
                    logger.warning(msg)
                    s["last_action"] = msg
                    s["stop"] = 0
                    trailing_to_persist = (symbol, s["highest"], 0)
                    state["positions"][symbol] = 0
                    sell_action = (msg, held)

    if trailing_to_persist:
        save_trailing_async(*trailing_to_persist)

    if sell_action:
        msg, held = sell_action
        # auto-sell ยิง MP-MTL เท่านั้นเสมอ (price_type ค่าเริ่มต้น, ไม่ส่ง price) — ดูคอมเมนต์
        # ที่ place_order_fire_and_forget ด้านบนว่าทำไมถึงตั้งใจไม่เปิดให้เลือกราคาตรงนี้
        place_order_fire_and_forget("Sell", symbol, held, pin, tick_ts=tick_ts)
        t_decide = time.time()
        logger.info(
            f"⏱️ {symbol} tick→ตัดสินใจขาย+ส่งเข้าคิว: {(t_decide - tick_ts) * 1000:.1f}ms "
            f"(รอผลจริงจาก Settrade ดู log บรรทัดถัดไปจาก place_order_fire_and_forget)"
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
    """
    v2.9: แก้ regression — เวอร์ชันก่อนหน้าลืมล้าง prev_bid1_price (ล้างแต่ prev_bid1_vol)
    ทำให้เสี่ยงเจอบั๊กเดียวกับตอนบิดหายข้ามคืน แต่ผ่านทางราคาแทนวอลุ่ม (เคยแก้ไปรอบก่อน
    แล้วแต่หายไปจากเวอร์ชันนี้ น่าจะตกหล่นตอนรวมโค้ดหลายรอบ) แก้กลับให้ล้างทั้งคู่
    """
    with lock:
        for sym, s in state["symbols"].items():
            s["prev_bid1_vol"] = 0
            s["prev_bid1_price"] = 0
        subscribed.clear()
    logger.info("📅 เข้าสู่วันเทรดใหม่ → รีเซ็ต baseline บิดหาย%/ราคาตก% และบังคับ subscribe ใหม่")

def bot_loop():
    global _last_trading_date
    while True:
        try:
            today = get_bkk_date()
            if _last_trading_date != today:
                new_trading_day_reset()
                _last_trading_date = today

            wl = load_watchlist()
            if wl is not None:
                with lock:
                    state["watchlist"] = wl
                    for sym in wl:
                        if sym not in state["symbols"]:
                            h, st = load_trailing(sym)
                            state["symbols"][sym] = {"highest": h, "stop": st}

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
                    },
                    "watchlist": state["watchlist"],
                    "positions": state["positions"],
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

            # v2.9: ถ้าเลือก Limit ต้องมีราคาที่ใช้ได้จริงมาด้วยเสมอ กันเผลอส่งราคา 0/ว่าง
            # ไปที่ Settrade (บาง gateway อาจตีความ 0 เป็นอย่างอื่นไม่คาดคิด)
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
                    "ok": False, "msg": msg,
                }
                _record_order(entry)
                self.send_json({"ok": False, "msg": msg})
                return
            self.send_json(place_order(side, symbol, volume, pin, price_type=price_type, price=price))
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
  .order-row { display:flex; justify-content:space-between; gap:8px; font-size:12px; padding:6px 0; border-top:1px solid #1e293b; }
  .order-row:first-child { border-top:none; }
  .ok-yes { color:#4ade80; } .ok-no { color:#f87171; } .ok-pending { color:#facc15; }
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
    <div id="orderLogBody" style="font-size:12px;color:#64748b;">ยังไม่มีคำสั่ง</div>
  </div>

  <!-- โซน 3.6: พอร์ตปัจจุบัน (ดิบๆ ไม่ผ่านการกรอง) -->
  <div class="card">
    <div style="font-weight:bold;margin-bottom:8px;">💼 พอร์ตปัจจุบัน (จาก Settrade ตรงๆ)</div>
    <div id="portBody" style="font-size:13px;color:#94a3b8;">กำลังโหลด...</div>
  </div>

  <!-- โซน 4: Watchlist -->
  <div class="card">
    <div style="font-weight:bold;margin-bottom:8px;">📋 รายการเฝ้า (Watchlist)</div>
    <div style="font-size:11px;color:#64748b;margin-bottom:4px;">บิดหาย% หรือ ราคาตก% (แล้วแต่อันไหนถึงก่อน) → ขายหมดพอร์ตทันที ด้วย MP-MTL เสมอ (ไม่ใช้ราคาจำกัด กันขายไม่ออก)</div>
    <div style="font-size:11px;color:#64748b;margin-bottom:6px;">🔒 = หุ้นที่ถืออยู่จริง ระบบเพิ่มให้อัตโนมัติ ลบแล้วจะเพิ่มกลับถ้ายังถือของอยู่ (ขายหมดจะหายเอง) — ถ้าอยากหยุดเฝ้าโดยไม่ลบ ใช้ปุ่ม 🟢/⚪ แทน ส่วนหุ้นที่กด + เพิ่มเอง ลบได้อิสระ ไว้ทดสอบ</div>
    <div id="wlBody"></div>
    <div style="border-top:1px solid #263449;margin:10px 0;"></div>
    <div style="font-size:12px;color:#94a3b8;margin-bottom:6px;">➕ เพิ่มหุ้นใหม่ (ไว้ทดสอบ/เฝ้าก่อนซื้อ)</div>
    <div class="row">
      <div class="grow"><input id="newSym" placeholder="เช่น AOT" style="text-transform:uppercase;"></div>
      <div style="width:60px;"><input id="newDrop" type="number" value="60" title="บิดหาย%"></div>
      <div style="width:60px;"><input id="newPriceDrop" type="number" step="0.1" value="1.0" title="ราคาตก%"></div>
      <div style="width:60px;"><input id="newTrail" type="number" step="0.1" value="1.0" title="trailing%"></div>
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
const fmt=n=>n==null||n===0?'--':Number(n).toLocaleString('en-US');

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
  // v2.10: ไม่ auto-fill ราคาให้อีกต่อไป — เดิมเคยดึงราคาล่าสุดที่จอ "เลือกหุ้นดูจอ" แสดงอยู่
  // มาใส่ให้เอง แต่ช่องนั้นกับช่อง "หุ้น" ในเทรดด่วนเป็นคนละตัวแปรกัน (ตั้งใจแยกกันไว้กัน
  // การพิมพ์โดน overwrite) ถ้าผู้ใช้พิมพ์ชื่อหุ้นในช่องเทรดด่วนเองโดยไม่ได้เลือกจาก dropdown
  // ก่อน ราคาที่ auto-fill มาอาจเป็นราคาของหุ้นคนละตัวโดยไม่รู้ตัว เสี่ยงกดซื้อผิดราคาไปเลย
  // ตอนนี้ช่องราคาว่างเปล่าเสมอ บังคับให้พิมพ์เองทุกครั้ง เป็นตัวเลขที่ผู้ใช้ตัดสินใจเองจริงๆ
  const isLimit = document.getElementById('tradePriceType').value === 'Limit';
  document.getElementById('tradePriceWrap').style.display = isLimit ? '' : 'none';
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
        return `<div class="order-row"><span>${o.time} ${icon} ${sideTh} ${o.symbol} ${o.volume}${priceTxt}</span><span class="${cls}" style="text-align:right;max-width:55%;overflow-wrap:anywhere;">${(o.msg||'').slice(0,80)}</span></div>`;
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
        let whtml='<table><tr><th>หุ้น</th><th>ถือ</th><th>บิดหาย%</th><th>ราคาตก%</th><th>Trail%</th><th>บน/ปิด</th><th></th></tr>';
        for(const k of keys){
          const c=s.watchlist[k]||{};
          const held=(s.positions&&s.positions[k])||0;
          const autoTag=(!c.pinned && held>0)?' <span style="font-size:10px;color:#64748b;">🔒</span>':'';
          whtml+=`<tr class="wl-row">
            <td><b>${k}</b>${autoTag}</td>
            <td class="yellow mono">${held}</td>
            <td><input id="d_${k}" type="number" value="${c.bid_drop_pct}" onchange="updateRow('${k}')"></td>
            <td><input id="p_${k}" type="number" step="0.1" value="${c.price_drop_pct!=null?c.price_drop_pct:1.0}" onchange="updateRow('${k}')"></td>
            <td><input id="t_${k}" type="number" step="0.1" value="${c.trailing_pct}" onchange="updateRow('${k}')"></td>
            <td><button class="${c.active?'btn-buy':'btn-ghost'}" onclick="toggleActive('${k}')">${c.active?'🟢':'⚪'}</button></td>
            <td><button class="btn-danger" onclick="askRemove('${k}')">🗑</button></td>
          </tr>`;
        }
        whtml+='</table>';
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
  const body={symbol,bid_drop_pct:document.getElementById('newDrop').value,price_drop_pct:document.getElementById('newPriceDrop').value,trailing_pct:document.getElementById('newTrail').value};
  const res=await (await fetch('/api/watchlist/add',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)})).json();
  alert(res.ok?'✅ เพิ่ม '+symbol+' แล้ว':'❌ '+res.msg);
}
async function updateRow(sym){
  await fetch('/api/watchlist/update',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({
    symbol:sym,bid_drop_pct:document.getElementById('d_'+sym).value,price_drop_pct:document.getElementById('p_'+sym).value,trailing_pct:document.getElementById('t_'+sym).value,active:true
  })});
}
async function toggleActive(sym){
  const r=await (await fetch('/api/state')).json();
  const c=r.watchlist[sym]||{};
  await fetch('/api/watchlist/update',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({
    symbol:sym,bid_drop_pct:c.bid_drop_pct,price_drop_pct:c.price_drop_pct,trailing_pct:c.trailing_pct,active:!c.active
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
    # v2.9: กลับมาใช้ ThreadingHTTPServer (เจอว่าเวอร์ชันที่อัปโหลดมาใช้ HTTPServer ธรรมดา
    # ซึ่งจัดการทีละ 1 request — ถ้าสั่งซื้อ/ขายมือแล้ว Settrade ตอบช้า จะบล็อกทุก
    # request อื่นด้วย รวมถึง /api/state ที่หน้าเว็บ poll ทุกวินาที ทำให้แดชบอร์ดค้างทั้งหน้า)
    server = ThreadingHTTPServer(("0.0.0.0", port), DashboardHandler)
    logger.info(f"🌐 Dashboard: http://0.0.0.0:{port}")
    server.serve_forever()

if __name__ == "__main__":
    main()
