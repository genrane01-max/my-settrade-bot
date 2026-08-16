# ==============================================================================
# check_market_scanner_support.py (Render-compatible)
#
# เดิมสคริปต์นี้รันเช็คแล้ว "จบโปรแกรมทันที" — Render Web Service คาดว่าโปรแกรมต้อง
# ค้างทำงานอยู่ตลอด (bind port ไว้) พอเจอโปรแกรมจบเอง Render เลยตีว่า deploy failed
# ("Application exited early") — เวอร์ชันนี้แก้โดยรันเช็คครั้งเดียวตอนเริ่ม แล้วเปิด
# เว็บเซิร์ฟเวอร์เล็กๆ ค้างไว้โชว์ผลลัพธ์ ให้เข้าดูผ่าน URL ของ Render ได้เลย ไม่ต้องขุด log
#
# วิธีใช้: deploy ไฟล์นี้แทนไฟล์เดิมชั่วคราว (เปลี่ยน start command เป็นไฟล์นี้)
# พอเข้า URL ของ Render (หน้าเดียวกับที่เคยเปิดดูแดชบอร์ดบอท) จะเห็นผลเช็คทันที
# เช็คเสร็จ ก็เปลี่ยน start command กลับไปเป็นบอทตัวจริงตามเดิม
# ==============================================================================

import os
import io
import contextlib
from http.server import HTTPServer, BaseHTTPRequestHandler
from settrade_v2 import Investor

RESULT_LOG = []  # เก็บผลลัพธ์ไว้โชว์ผ่านเว็บ

def log(line=""):
    print(line, flush=True)   # ยังขึ้นใน Render Logs ตามเดิมด้วย
    RESULT_LOG.append(str(line))

def resolve_method(obj, candidates, label=""):
    for name in candidates:
        m = getattr(obj, name, None)
        if callable(m):
            log(f"✅ [{label}] เจอ method: {name}")
            return name
    available = sorted(m for m in dir(obj) if not m.startswith("_"))
    log(f"❌ [{label}] ไม่เจอ method ที่ลอง ({candidates})")
    log(f"   method จริงที่มีอยู่ใน {type(obj).__name__}: {available}")
    return None

def run_check():
    app_id = os.getenv("SETTRADE_APP_ID")
    app_secret = os.getenv("SETTRADE_APP_SECRET")
    broker_id = os.getenv("SETTRADE_BROKER_ID")
    app_code = os.getenv("SETTRADE_APP_CODE", "SANDBOX")

    if not app_id or not app_secret:
        log("❌ ไม่มี SETTRADE_APP_ID/SECRET ใน env — ตั้งให้ครบก่อน (เช็คในหน้า Environment ของ Render)")
        return

    try:
        log(f"กำลังเชื่อมต่อ (broker={broker_id}, app_code={app_code}) ...")
        investor = Investor(app_id=app_id, app_secret=app_secret,
                             broker_id=broker_id, app_code=app_code)
        log("✅ เชื่อมต่อ Investor สำเร็จ\n")

        log("=" * 70)
        log("1) รายชื่อทุก attribute/method บน Investor object (มองหาคำว่า market/quote/screen):")
        all_attrs = sorted(m for m in dir(investor) if not m.startswith("_"))
        log(str(all_attrs))
        interesting = [a for a in all_attrs if any(
            kw in a.lower() for kw in ["market", "quote", "screen", "rank", "active", "mp"]
        )]
        log(f"\n   attribute ที่น่าสนใจ: {interesting}")

        log("\n" + "=" * 70)
        log("2) ลองสร้าง MarketData object (ชื่อ class เดายอดฮิตของ Settrade SDK):")
        md = None
        for class_name in ["MarketData", "Market", "Quote", "RealtimeMarketData"]:
            ctor = getattr(investor, class_name, None)
            if callable(ctor):
                try:
                    md = ctor()
                    log(f"✅ สร้างสำเร็จจาก investor.{class_name}()")
                    break
                except Exception as e:
                    log(f"⚠️  investor.{class_name}() มีอยู่ แต่สร้างไม่สำเร็จ: {e}")
        if md is None:
            log("❌ ไม่พบ class ที่ชื่อคุ้นเคย (MarketData/Market/Quote/RealtimeMarketData) บน investor")
        else:
            md_attrs = sorted(m for m in dir(md) if not m.startswith("_"))
            log(f"   method ทั้งหมดใน {type(md).__name__}: {md_attrs}")
            screener_candidates = [
                "get_top_active_value", "get_top_active_volume", "get_most_active",
                "get_quote_list", "get_ranking", "top_ranking", "get_top_ranking",
                "most_active_volume", "most_active_value",
            ]
            resolve_method(md, screener_candidates, "market screener")

        log("\n" + "=" * 70)
        log("สรุป: เอาผลด้านบนกลับไปให้ดู")
    except Exception as e:
        log(f"❌ เกิด error ระหว่างเช็ค: {e}")

class ResultHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass
    def do_GET(self):
        body = ("<pre style='white-space:pre-wrap;font-family:monospace;padding:16px;"
                "background:#0b1220;color:#93c5fd;'>" + "\n".join(RESULT_LOG) + "</pre>").encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

def main():
    run_check()
    port = int(os.getenv("PORT", 10000))
    print(f"🌐 ผลเช็คพร้อมดูที่ URL หลักของ Render แล้ว (port {port})", flush=True)
    HTTPServer(("0.0.0.0", port), ResultHandler).serve_forever()

if __name__ == "__main__":
    main()
