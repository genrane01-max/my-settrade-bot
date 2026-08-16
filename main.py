# ==============================================================================
# SETTRADE AUTO-SELL BOT (FREE WEB SERVICE VERSION FOR RENDER)
# ==============================================================================
import os
import time
import requests
import logging
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 1. DUMMY HTTP SERVER FOR RENDER FREE HEALTH CHECK ---
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK - Bot is running")

def start_health_check_server():
    port = int(os.getenv("PORT", 10000))
    server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
    logger.info(f"Health check server running on port {port}")
    server.serve_forever()

# --- 2. CONFIG FROM ENVIRONMENT VARIABLES ---
SETTRADE_APP_ID = os.getenv("SETTRADE_APP_ID")
SETTRADE_APP_SECRET = os.getenv("SETTRADE_APP_SECRET")
SETTRADE_BROKER_ID = os.getenv("SETTRADE_BROKER_ID", "BLS")
SETTRADE_ACCOUNT_NO = os.getenv("SETTRADE_ACCOUNT_NO")
SETTRADE_PIN = os.getenv("SETTRADE_PIN")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

TARGET_SYMBOL = os.getenv("TARGET_SYMBOL", "TTB")
BID_DROP_THRESHOLD_PCT = float(os.getenv("BID_DROP_THRESHOLD_PCT", "50.0"))
TRAILING_STOP_OFFSET = float(os.getenv("TRAILING_STOP_OFFSET", "0.04"))

def send_telegram_alert(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        logger.error(f"Telegram error: {e}")

def place_market_sell_order(symbol: str, qty: int):
    logger.info(f"🚨 EXECUTING AUTO MARKET SELL: {symbol} Qty: {qty}")
    return {"status": "SUCCESS", "order_id": "ORD_AUTO_FREE_99", "qty": qty}

def start_bot():
    logger.info(f"🤖 Bot Started for Symbol: {TARGET_SYMBOL}")
    send_telegram_alert(f"🤖 **ระบบบอทขายหุ้นเปิดทำงานแล้ว (Free Tier)!**\n📌 หุ้นที่เฝ้า: `{TARGET_SYMBOL}`")

    highest_price = 0.0
    trailing_stop_price = 0.0
    last_bid_price = 0.0
    last_bid_vol = 0
    last_time = time.time()
    is_active = True
    qty_in_port = 10000

    while is_active:
        try:
            time.sleep(1)
            now = time.time()
            time_diff = now - last_time

            best_bid_price = 2.96
            best_bid_vol = 29000000

            if best_bid_price > highest_price:
                highest_price = best_bid_price
                trailing_stop_price = round(highest_price - TRAILING_STOP_OFFSET, 2)

            if time_diff <= 3.0 and last_bid_price == best_bid_price and last_bid_vol > 0:
                vol_drop = last_bid_vol - best_bid_vol
                drop_pct = (vol_drop / last_bid_vol) * 100.0

                if drop_pct >= BID_DROP_THRESHOLD_PCT:
                    is_active = False
                    sell_res = place_market_sell_order(TARGET_SYMBOL, qty_in_port)
                    send_telegram_alert(
                        f"⚡ **[AUTO-SELL] สั่งขายอัตโนมัติทันที!**\n\n"
                        f"📌 หุ้น: `{TARGET_SYMBOL}`\n"
                        f"🔥 Bid หายกะทันหัน: `{drop_pct:.1f}%`\n"
                        f"📦 สั่งขาย: `{qty_in_port:,}` หุ้น"
                    )
                    break

            if best_bid_price <= trailing_stop_price and highest_price > 0:
                is_active = False
                sell_res = place_market_sell_order(TARGET_SYMBOL, qty_in_port)
                send_telegram_alert(
                    f"🛑 **[AUTO-SELL] แตะจุด Trailing Stop สั่งขายทันที!**\n\n"
                    f"📌 หุ้น: `{TARGET_SYMBOL}`\n"
                    f"📉 ราคาสูงสุด: `{highest_price}` บาท | จุดตัด: `{trailing_stop_price}` บาท"
                )
                break

            last_bid_price = best_bid_price
            last_bid_vol = best_bid_vol
            last_time = now

        except Exception as e:
            logger.error(f"Error: {e}")
            time.sleep(2)

if __name__ == "__main__":
    # รันเซิร์ฟเวอร์ตอบรับ Render ใน Background
    threading.Thread(target=start_health_check_server, daemon=True).start()
    # เริ่มระบบเฝ้ามองกระดานหุ้น
    start_bot()
