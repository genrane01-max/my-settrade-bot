# ==============================================================================
# SETTRADE AUTO-SELL BOT WITH WEB DASHBOARD & FIREBASE
# ==============================================================================
import os
import time
import requests
import logging
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs
import firebase_admin
from firebase_admin import credentials, db

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 1. FIREBASE INITIALIZATION ---
def init_firebase():
    try:
        if not firebase_admin._apps:
            private_key = os.getenv("FIREBASE_PRIVATE_KEY", "").replace("\\n", "\n")
            cred_dict = {
                "type": "service_account",
                "project_id": os.getenv("FIREBASE_PROJECT_ID"),
                "client_email": os.getenv("FIREBASE_CLIENT_EMAIL"),
                "private_key": private_key,
            }
            cred = credentials.Certificate(cred_dict)
            db_url = os.getenv("FIREBASE_DATABASE_URL")
            firebase_admin.initialize_app(cred, {'databaseURL': db_url})
            logger.info("Firebase Realtime Database Connected!")
    except Exception as e:
        logger.error(f"Firebase Init Error: {e}")

init_firebase()

def get_config():
    """ ดึงค่าตั้งค่าจาก Firebase (ถ้าไม่มีให้ใช้ค่าเริ่มต้น) """
    try:
        ref = db.reference("config")
        data = ref.get()
        if data:
            return data
    except Exception as e:
        logger.error(f"Error fetching Firebase config: {e}")
    
    return {
        "target_symbol": os.getenv("TARGET_SYMBOL", "TTB"),
        "bid_drop_pct": float(os.getenv("BID_DROP_THRESHOLD_PCT", "50.0")),
        "trailing_offset": float(os.getenv("TRAILING_STOP_OFFSET", "0.04"))
    }

def save_config(symbol, bid_drop, trailing_offset):
    """ บันทึกค่าตั้งค่าใหม่ลง Firebase """
    try:
        ref = db.reference("config")
        ref.set({
            "target_symbol": symbol.upper().strip(),
            "bid_drop_pct": float(bid_drop),
            "trailing_offset": float(trailing_offset)
        })
        return True
    except Exception as e:
        logger.error(f"Error saving Firebase config: {e}")
        return False

# --- 2. WEB CONTROL PANEL (HTML UI) ---
class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            with open("index.html", "r", encoding="utf-8") as f:
                html = f.read()
            self.send_response(200)
            self.send_header("Content-type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(html.encode("utf-8"))
        except Exception as e:
            self.send_response(404)
            self.end_headers()
        <!DOCTYPE html>
        <html lang="th">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Settrade Bot Dashboard</title>
            <style>
                body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background-color: #0f172a; color: #f8fafc; padding: 20px; margin: 0; display: flex; justify-content: center; }}
                .card {{ background: #1e293b; border-radius: 16px; padding: 24px; max-width: 400px; width: 100%; box-shadow: 0 10px 25px rgba(0,0,0,0.5); border: 1px solid #334155; }}
                h2 {{ color: #38bdf8; text-align: center; margin-top: 0; margin-bottom: 16px; font-size: 22px; }}
                .status {{ background: #064e3b; color: #34d399; padding: 10px; border-radius: 8px; text-align: center; font-size: 14px; font-weight: bold; margin-bottom: 20px; }}
                label {{ display: block; margin-top: 14px; font-size: 14px; color: #94a3b8; font-weight: 600; }}
                input {{ width: 100%; padding: 12px; margin-top: 6px; border-radius: 8px; border: 1px solid #475569; background: #0f172a; color: #fff; font-size: 16px; box-sizing: border-box; }}
                input:focus {{ border-color: #38bdf8; outline: none; }}
                button {{ width: 100%; padding: 14px; margin-top: 24px; border-radius: 8px; border: none; background: #0284c7; color: white; font-size: 16px; font-weight: bold; cursor: pointer; transition: 0.2s; }}
                button:hover {{ background: #0369a1; }}
            </style>
        </head>
        <body>
            <div class="card">
                <h2>🤖 Settrade Control Panel</h2>
                <div class="status">🟢 Bot Online (Firebase Connected)</div>
                <form method="POST">
                    <label>📈 หุ้นที่โฟกัส (TARGET_SYMBOL):</label>
                    <input type="text" name="symbol" value="{cfg.get('target_symbol', 'TTB')}" required>
                    
                    <label>📉 Bid หายกะทันหัน (%):</label>
                    <input type="number" step="0.1" name="bid_drop" value="{cfg.get('bid_drop_pct', 50.0)}" required>
                    
                    <label>🛑 Trailing Stop Offset (บาท):</label>
                    <input type="number" step="0.01" name="trailing_offset" value="{cfg.get('trailing_offset', 0.04)}" required>
                    
                    <button type="submit">💾 บันทึกค่าลง Firebase</button>
                </form>
            </div>
        </body>
        </html>
        """
        self.send_response(200)
        self.send_header("Content-type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(html.encode("utf-8"))

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length).decode('utf-8')
        params = parse_qs(post_data)
        
        symbol = params.get('symbol', ['TTB'])[0]
        bid_drop = params.get('bid_drop', ['50.0'])[0]
        trailing_offset = params.get('trailing_offset', ['0.04'])[0]
        
        save_config(symbol, bid_drop, trailing_offset)
        
        self.send_response(303)
        self.send_header('Location', '/')
        self.end_headers()

def start_web_server():
    port = int(os.getenv("PORT", 10000))
    server = HTTPServer(('0.0.0.0', port), DashboardHandler)
    logger.info(f"Web Dashboard running on port {port}")
    server.serve_forever()

# --- 3. SETTRADE AUTO-SELL BOT LOOP ---
def start_bot():
    logger.info("🤖 Auto-Sell Bot Loop Active...")
    last_bid_price = 0.0
    last_bid_vol = 0
    last_time = time.time()
    highest_price = 0.0

    while True:
        try:
            # ดึงค่าคอนฟิกใหม่ล่าสุดจาก Firebase ทุกๆ วินาที
            cfg = get_config()
            target_symbol = cfg.get("target_symbol", "TTB")
            bid_drop_threshold = float(cfg.get("bid_drop_pct", 50.0))
            trailing_offset = float(cfg.get("trailing_offset", 0.04))

            time.sleep(1)
            # (ส่วนนี้จะดึงข้อมูล Real-time จาก Settrade API เมื่อใส่ App ID/Secret ครบ)

        except Exception as e:
            logger.error(f"Bot Loop Error: {e}")
            time.sleep(2)

if __name__ == "__main__":
    threading.Thread(target=start_web_server, daemon=True).start()
    start_bot()
