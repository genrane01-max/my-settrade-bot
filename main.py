# ==============================================================================
# SETTRADE BOT ฉบับเต็ม — เทรดด่วน MP-MTL + ระบบเฝ้าบิดหาย + Trailing Stop
# หน้าจอ: ราคาเรียลไทม์, ตารางบิด/ออฟเฟอร์ 5 ชั้น, ปุ่มซื้อ/ขาย, ปุ่มเปิด-ปิดบอท
# ==============================================================================
# ⚠️ ทดสอบ Sandbox ก่อนเสมอ (SETTRADE_APP_CODE=SANDBOX)
# เทรดจริง: ต้องได้ App ID/Secret จาก BLS + เปลี่ยนเป็น SETTRADE_APP_CODE=ALGO
# ==============================================================================

import os
import time
import json
import logging
import threading
import requests
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

import firebase_admin
from firebase_admin import credentials, db
from settrade_v2 import Investor

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ---------- สถานะบอท (อยู่ในหน่วยความจำ) ----------
state = {
    "enabled": True,
    "connected": False,
    "symbol": "TTB",
    "last_price": 0.0,
    "highest_price": 0.0,
    "stop_price": 0.0,
    "prev_bid1_vol": 0.0,
    "bid1_drop_pct": 0.0,
    "bids": [],       # [[ราคา, วอลุ่ม], ...] 5 ชั้น
    "offers": [],     # [[ราคา, วอลุ่ม], ...] 5 ชั้น
    "last_action": "รอข้อมูล...",
    "config": {},
}
lock = threading.Lock()
investor = None
equity = None

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

def get_config():
    try:
        ref = db.reference("config")
        data = ref.get()
        if data:
            return data
    except Exception as e:
        logger.error(f"Error fetching Firebase config: {e}")
    return {
        "target_symbol": os.getenv("TARGET_SYMBOL", "TTB"),
        "bid_drop_pct": float(os.getenv("BID_DROP_THRESHOLD_PCT", "60.0")),
        "trailing_offset": float(os.getenv("TRAILING_STOP_OFFSET", "0.02")),
        "sell_volume": int(os.getenv("SELL_VOLUME", "100")),
        "buy_volume": int(os.getenv("BUY_VOLUME", "100")),
    }

def save_config(cfg):
    try:
        ref = db.reference("config")
        ref.set(cfg)
        logger.info(f"Config saved: {cfg}")
        return True
    except Exception as e:
        logger.error(f"Error saving Firebase config: {e}")
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
        logger.warning("ยังไม่มี SETTRADE_APP_ID/SECRET — รอ key จาก BLS ก่อนถึงจะเชื่อมตลาดได้")
        return False
    try:
        investor = Investor(
            app_id=app_id,
            app_secret=app_secret,
            broker_id=broker_id,
            app_code=app_code,
        )
        equity = investor.Equity(account_no=account_no)
        with lock:
            state["connected"] = True
        logger.info(f"✅ Settrade เชื่อมต่อแล้ว (broker={broker_id}, app_code={app_code})")
        return True
    except Exception as e:
        logger.error(f"Settrade เชื่อมต่อไม่สำเร็จ: {e}")
        return False

# ---------- จัดรูปแบบ order book ให้เป็น [ราคา, วอลุ่ม] ----------
def normalize_book(data):
    rows = []
    for item in (data or [])[:5]:
        if isinstance(item, dict):
            rows.append([item.get("price", 0), item.get("volume", 0)])
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            rows.append([item[0], item[1]])
    return rows

# ---------- Callback สตรีมราคา + บิด/ออฟเฟอร์ ----------
def on_bids_offers(msg):
    try:
        with lock:
            state["bids"] = normalize_book(msg.get("bids"))
            state["offers"] = normalize_book(msg.get("offers"))
    except Exception as e:
        logger.error(f"on_bids_offers error: {e}")

def on_price_info(msg):
    try:
        with lock:
            state["last_price"] = msg.get("last", 0.0) or msg.get("price", 0.0)
    except Exception as e:
        logger.error(f"on_price_info error: {e}")

def start_realtime(symbol):
    """ เปิดสตรีม — รันใน thread แยก (method ชื่ออาจต่างตามรุ่น SDK ตรวจ docs) """
    try:
        rt = investor.RealtimeDataConnection()
        rt.subscribe_bids_offers(symbol, on_bids_offers)
        rt.subscribe_price_info(symbol, on_price_info)
        logger.info(f"📡 กำลังสตรีม {symbol} ...")
        rt.run()
    except Exception as e:
        logger.error(f"start_realtime error: {e}")

# ---------- สั่งซื้อ/ขาย (MP-MTL เสมอ) ----------
def place_order(side, symbol, volume, pin, price_type="MP-MTL"):
    """ side = 'Buy' หรือ 'Sell' — MP-MTL กันราคาหลุดไกล """
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
        msg = f"📤 {side} {symbol} {volume} ({price_type})\nตอบกลับ: {resp}"
        logger.info(msg)
        send_telegram(msg)
        with lock:
            state["last_action"] = msg
        return {"ok": True, "msg": str(resp)}
    except Exception as e:
        logger.error(f"place_order error: {e}")
        return {"ok": False, "msg": str(e)}

# ===================== ตรรกะเฝ้าหุ้น =====================
def check_and_autosell(cfg):
    with lock:
        if not state["enabled"] or not state["connected"]:
            return
        symbol = cfg.get("target_symbol", "TTB")
        threshold = float(cfg.get("bid_drop_pct", 60.0))
        offset = float(cfg.get("trailing_offset", 0.02))
        sell_vol = int(cfg.get("sell_volume", 100))
        pin = os.getenv("SETTRADE_PIN")
        bids = state["bids"]
        last_price = state["last_price"]

        # --- 1) บิดชั้น 1 วอลุ่มหายเกิน % → ขายทันที ---
        if bids:
            bid1_price, bid1_vol = bids[0]
            prev = state["prev_bid1_vol"]
            if prev > 0 and bid1_vol < prev:
                drop = (prev - bid1_vol) / prev * 100
                state["bid1_drop_pct"] = round(drop, 1)
                if drop >= threshold:
                    msg = f"🚨 บิด {bid1_price} วอลุ่มหาย {drop:.1f}% → ขาย {symbol} {sell_vol} ทันที!"
                    logger.warning(msg)
                    send_telegram(msg)
                    state["last_action"] = msg
                    place_order("Sell", symbol, sell_vol, pin)  # MP-MTL
            state["prev_bid1_vol"] = bid1_vol

        # --- 2) Trailing stop: ราคาขึ้น → ขยับจุดขายตาม ---
        if last_price > 0:
            if last_price > state["highest_price"]:
                state["highest_price"] = last_price
                state["stop_price"] = round(last_price - offset, 2)
            if state["stop_price"] > 0 and last_price <= state["stop_price"]:
                msg = (f"🛑 ราคาตกถึงจุดขาย {state['stop_price']} "
                       f"(สูงสุด {state['highest_price']}) → ขาย {symbol} {sell_vol}")
                logger.warning(msg)
                send_telegram(msg)
                state["last_action"] = msg
                place_order("Sell", symbol, sell_vol, pin)  # MP-MTL
                state["stop_price"] = 0  # ป้องกันขายซ้ำ

def bot_loop():
    while True:
        try:
            cfg = get_config()
            with lock:
                state["symbol"] = cfg.get("target_symbol", "TTB")
                state["config"] = cfg
            check_and_autosell(cfg)
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
                payload = {
                    "enabled": state["enabled"],
                    "connected": state["connected"],
                    "symbol": state["symbol"],
                    "last_price": state["last_price"],
                    "highest": state["highest_price"],
                    "stop": state["stop_price"],
                    "bid1_drop": state["bid1_drop_pct"],
                    "bids": state["bids"],
                    "offers": state["offers"],
                    "last_action": state["last_action"],
                    "config": state["config"],
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

        if path == "/api/order":
            side = data.get("side", "Buy")
            symbol = data.get("symbol", "TTB")
            volume = data.get("volume", 100)
            pin = data.get("pin", "")
            if not pin:
                self.send_json({"ok": False, "msg": "ต้องกรอก PIN"})
                return
            result = place_order(side, symbol, volume, pin)  # MP-MTL เสมอ
            self.send_json(result)
            return

        if path == "/api/config":
            cfg = {
                "target_symbol": data.get("target_symbol", "TTB").upper().strip(),
                "bid_drop_pct": float(data.get("bid_drop_pct", 60)),
                "trailing_offset": float(data.get("trailing_offset", 0.02)),
                "sell_volume": int(data.get("sell_volume", 100)),
                "buy_volume": int(data.get("buy_volume", 100)),
            }
            ok = save_config(cfg)
            with lock:
                state["config"] = cfg
            self.send_json({"ok": ok})
            return

        self.send_json({"ok": False, "msg": "unknown path"})

HTML = """<!DOCTYPE html>
<html lang="th">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Settrade Bot</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, 'Segoe UI', Roboto, sans-serif; background:#0b1220; color:#e2e8f0; padding:12px; }
  .card { background:#151e2e; border:1px solid #263449; border-radius:14px; padding:14px; margin-bottom:12px; }
  .row { display:flex; gap:8px; align-items:center; }
  .grow { flex:1; }
  .badge { padding:6px 12px; border-radius:20px; font-weight:bold; font-size:14px; }
  .on { background:#064e3b; color:#34d399; }
  .off { background:#7f1d1d; color:#fca5a5; }
  .price { font-size:34px; font-weight:800; }
  .red { color:#f87171; } .green { color:#4ade80; } .yellow { color:#facc15; }
  button { border:none; border-radius:10px; padding:12px; font-size:16px; font-weight:bold; color:#fff; cursor:pointer; width:100%; }
  .btn-buy { background:#059669; } .btn-sell { background:#dc2626; }
  .btn-toggle { background:#2563eb; } .btn-ghost { background:#334155; }
  input { width:100%; padding:10px; border-radius:8px; border:1px solid #334155; background:#0b1220; color:#fff; font-size:16px; margin-top:4px; }
  label { font-size:12px; color:#94a3b8; font-weight:600; }
  table { width:100%; border-collapse:collapse; font-size:13px; }
  th { color:#94a3b8; font-size:11px; padding:4px; text-align:center; }
  td { padding:5px; text-align:center; border-top:1px solid #1e293b; }
  .mid { width:64px; font-size:15px; font-weight:800; text-align:center; }
  .mono { font-variant-numeric: tabular-nums; }
  .log { background:#0b1220; border:1px solid #263449; border-radius:8px; padding:8px; font-size:12px; color:#93c5fd; min-height:20px; }
  .modal-bg { display:none; position:fixed; inset:0; background:rgba(0,0,0,.7); z-index:50; align-items:center; justify-content:center; }
  .modal { background:#1e293b; border:1px solid #475569; border-radius:14px; padding:20px; max-width:340px; width:92%; }
</style>
</head>
<body>
  <div class="card">
    <div class="row">
      <div class="grow">
        <span id="statusBadge" class="badge on">🟢 บอททำงาน</span>
        <span id="connBadge" class="badge off" style="margin-left:6px;">🔌 ยังไม่ต่อ</span>
      </div>
      <button id="toggleBtn" class="btn-toggle" style="width:110px;" onclick="toggleBot()">⏸ ปิดบอท</button>
    </div>
    <div class="row" style="margin-top:10px;">
      <div class="grow">
        <div id="symbolTxt" style="font-size:14px;color:#94a3b8;">TTB</div>
        <div id="priceTxt" class="price red mono">--</div>
      </div>
      <div style="text-align:right;font-size:13px;">
        <div>สูงสุด: <span id="highestTxt" class="yellow mono">--</span></div>
        <div>จุดขาย: <span id="stopTxt" class="green mono">--</span></div>
        <div>บิดหาย: <span id="dropTxt" class="red mono">0%</span></div>
      </div>
    </div>
    <div class="log" id="actionLog" style="margin-top:10px;">รอข้อมูล...</div>
  </div>

  <div class="card">
    <div style="font-weight:bold;margin-bottom:6px;">📊 บิด / ออฟเฟอร์ (เรียลไทม์)</div>
    <table>
      <tr><th>วอลุ่ม</th><th>บิด</th><th class="mid">ราคา</th><th>ออฟเฟอร์</th><th>วอลุ่ม</th></tr>
      <tbody id="bookBody"><tr><td colspan="5" style="color:#64748b;">กำลังโหลด...</td></tr></tbody>
    </table>
  </div>

  <div class="card">
    <div style="font-weight:bold;margin-bottom:8px;">⚡ เทรดด่วน (MP-MTL)</div>
    <label>หุ้น</label><input id="tradeSymbol" value="TTB">
    <div class="row" style="margin-top:8px;">
      <div class="grow"><label>จำนวน</label><input id="tradeVol" type="number" value="100" inputmode="numeric"></div>
      <div class="grow"><label>PIN</label><input id="tradePin" type="password" inputmode="numeric"></div>
    </div>
    <div class="row" style="margin-top:12px;">
      <button class="btn-buy" onclick="askOrder('Buy')">🟢 ซื้อ</button>
      <button class="btn-sell" onclick="askOrder('Sell')">🔴 ขาย</button>
    </div>
  </div>

  <div class="card">
    <div style="font-weight:bold;margin-bottom:8px;">🤖 ตั้งค่า Auto-Sell</div>
    <div class="row">
      <div class="grow"><label>หุ้นที่เฝ้า</label><input id="cfgSymbol" value="TTB"></div>
      <div class="grow"><label>บิดหายเกิน (%)</label><input id="cfgDrop" type="number" step="1" value="60"></div>
    </div>
    <div class="row" style="margin-top:8px;">
      <div class="grow"><label>Trailing (บาท)</label><input id="cfgTrail" type="number" step="0.01" value="0.02"></div>
      <div class="grow"><label>จำนวนขายอัตโนมัติ</label><input id="cfgSellVol" type="number" value="100"></div>
    </div>
    <button class="btn-ghost" style="margin-top:12px;" onclick="saveCfg()">💾 บันทึกค่าลง Firebase</button>
  </div>

  <div class="modal-bg" id="modalBg">
    <div class="modal">
      <div style="font-size:16px;font-weight:bold;margin-bottom:8px;" id="modalTitle">ยืนยัน</div>
      <div id="modalBody" style="font-size:14px;color:#cbd5e1;margin-bottom:14px;"></div>
      <div class="row">
        <button class="btn-ghost" onclick="closeModal()">ยกเลิก</button>
        <button id="modalOk" class="btn-sell" onclick="doOrder()">ยืนยัน</button>
      </div>
    </div>
  </div>

<script>
let pending = null;
function fmt(n){ return n==null?'--':Number(n).toLocaleString('en-US'); }
async function refresh(){
  try{
    const r = await fetch('/api/state');
    const s = await r.json();
    document.getElementById('symbolTxt').textContent = s.symbol;
    document.getElementById('priceTxt').textContent = s.last_price?fmt(s.last_price):'--';
    document.getElementById('priceTxt').className = 'price mono ' + (s.bid1_drop>=0?'red':'green');
    document.getElementById('highestTxt').textContent = s.highest?fmt(s.highest):'--';
    document.getElementById('stopTxt').textContent = s.stop?fmt(s.stop):'--';
    document.getElementById('dropTxt').textContent = s.bid1_drop+'%';
    document.getElementById('actionLog').textContent = s.last_action;
    const badge = document.getElementById('statusBadge');
    badge.className = 'badge ' + (s.enabled?'on':'off');
    badge.textContent = s.enabled?'🟢 บอททำงาน':'⏸ บอทหยุด';
    const cb = document.getElementById('connBadge');
    cb.className = 'badge ' + (s.connected?'on':'off');
    cb.textContent = s.connected?'🔌 เชื่อมต่อ':'🔌 ยังไม่ต่อ';
    document.getElementById('toggleBtn').textContent = s.enabled?'⏸ ปิดบอท':'▶ เปิดบอท';
    // ตารางบิด/ออฟเฟอร์
    const tbody = document.getElementById('bookBody');
    let html = '';
    const bids = s.bids||[], offers = s.offers||[];
    const n = Math.max(bids.length, offers.length);
    for(let i=0;i<n;i++){
      const b = bids[i]||[], o = offers[i]||[];
      html += '<tr>' +
        '<td class="yellow mono">'+(b[1]?fmt(b[1]):'')+'</td>' +
        '<td class="red mono">'+(b[0]?fmt(b[0]):'')+'</td>' +
        '<td class="mid"></td>' +
        '<td class="green mono">'+(o[0]?fmt(o[0]):'')+'</td>' +
        '<td class="yellow mono">'+(o[1]?fmt(o[1]):'')+'</td>' +
        '</tr>';
    }
    tbody.innerHTML = html || '<tr><td colspan="5" style="color:#64748b;">รอข้อมูล...</td></tr>';
    document.getElementById('cfgSymbol').value = s.config.target_symbol||'TTB';
    document.getElementById('cfgDrop').value = s.config.bid_drop_pct||60;
    document.getElementById('cfgTrail').value = s.config.trailing_offset||0.02;
    document.getElementById('cfgSellVol').value = s.config.sell_volume||100;
  }catch(e){}
}
async function toggleBot(){
  await fetch('/api/toggle',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});
}
function askOrder(side){
  const symbol = document.getElementById('tradeSymbol').value.trim().toUpperCase();
  const vol = document.getElementById('tradeVol').value;
  const pin = document.getElementById('tradePin').value;
  if(!pin){ alert('กรอก PIN ก่อน'); return; }
  pending = {side, symbol, volume:vol, pin};
  document.getElementById('modalTitle').textContent = (side==='Buy'?'🟢 ซื้อ':'🔴 ขาย')+' '+symbol+' '+vol+' หุ้น';
  document.getElementById('modalBody').textContent = 'ราคาตลาด (MP-MTL) — กันราคาหลุดไกล. ยืนยัน?';
  document.getElementById('modalOk').className = 'btn-'+(side==='Buy'?'buy':'sell');
  document.getElementById('modalBg').style.display = 'flex';
}
function closeModal(){ pending=null; document.getElementById('modalBg').style.display='none'; }
async function doOrder(){
  if(!pending) return;
  const r = await fetch('/api/order',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(pending)});
  const res = await r.json();
  alert(res.ok?'✅ ส่งคำสั่งแล้ว: '+res.msg:'❌ '+res.msg);
  closeModal();
}
async function saveCfg(){
  const body = {
    target_symbol: document.getElementById('cfgSymbol').value.trim().toUpperCase(),
    bid_drop_pct: document.getElementById('cfgDrop').value,
    trailing_offset: document.getElementById('cfgTrail').value,
    sell_volume: document.getElementById('cfgSellVol').value,
    buy_volume: document.getElementById('tradeVol').value
  };
  await fetch('/api/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  alert('💾 บันทึกแล้ว');
}
refresh();
setInterval(refresh, 1000);
</script>
</body>
</html>"""

# ===================== STARTUP =====================
def main():
    init_settrade()
    cfg = get_config()
    threading.Thread(target=start_realtime, args=(cfg.get("target_symbol", "TTB"),), daemon=True).start()
    threading.Thread(target=bot_loop, daemon=True).start()
    port = int(os.getenv("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), DashboardHandler)
    logger.info(f"🌐 Web Dashboard: http://0.0.0.0:{port}")
    server.serve_forever()

if __name__ == "__main__":
    main()
