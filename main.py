# ==============================================================================
# check_market_scanner_support.py
#
# สคริปต์ทดสอบ "แยกต่างหาก" ไม่ยุ่งกับบอทตัวจริง — ไม่มีการซื้อขายใดๆ ในสคริปต์นี้
# มีไว้เช็คอย่างเดียวว่า Settrade SDK (settrade_v2) ที่คุณมีสิทธิ์ใช้อยู่ตอนนี้
# มีฟังก์ชันดึง "รายชื่อหุ้นวอลุ่ม/มูลค่าซื้อขายสูงสุดทั้งตลาด" (market screener) หรือไม่
#
# วิธีรัน:
#   1. เอาไฟล์นี้ไปวางในโปรเจกต์เดียวกับบอท (มี settrade_v2 ติดตั้งอยู่แล้ว)
#   2. ตั้ง env vars ให้ครบเหมือนบอทตัวจริง (SETTRADE_APP_ID, SETTRADE_APP_SECRET,
#      SETTRADE_BROKER_ID, SETTRADE_APP_CODE, SETTRADE_ACCOUNT_N)
#   3. รัน: python3 check_market_scanner_support.py
#   4. เอา output ทั้งหมดที่ขึ้นมาส่งกลับมาให้ดู (โดยเฉพาะบรรทัดที่ขึ้นต้นด้วย "method จริงที่มี")
# ==============================================================================

import os
from settrade_v2 import Investor

def resolve_method(obj, candidates, label=""):
    for name in candidates:
        m = getattr(obj, name, None)
        if callable(m):
            print(f"✅ [{label}] เจอ method: {name}")
            return name
    available = sorted(m for m in dir(obj) if not m.startswith("_"))
    print(f"❌ [{label}] ไม่เจอ method ที่ลอง ({candidates})")
    print(f"   method จริงที่มีอยู่ใน {type(obj).__name__}: {available}")
    return None

def main():
    app_id = os.getenv("SETTRADE_APP_ID")
    app_secret = os.getenv("SETTRADE_APP_SECRET")
    broker_id = os.getenv("SETTRADE_BROKER_ID")
    app_code = os.getenv("SETTRADE_APP_CODE", "SANDBOX")
    account_no = os.getenv("SETTRADE_ACCOUNT_N")

    if not app_id or not app_secret:
        print("❌ ไม่มี SETTRADE_APP_ID/SECRET ใน env — ตั้งให้ครบก่อนรันสคริปต์นี้")
        return

    print(f"กำลังเชื่อมต่อ (broker={broker_id}, app_code={app_code}) ...")
    investor = Investor(app_id=app_id, app_secret=app_secret,
                         broker_id=broker_id, app_code=app_code)
    print("✅ เชื่อมต่อ Investor สำเร็จ\n")

    # --- 1) เช็คว่า Investor object เองมี class/method เกี่ยวกับ "market" อะไรบ้าง ---
    print("=" * 70)
    print("1) รายชื่อทุก attribute/method บน Investor object (มองหาคำว่า market/quote/screen):")
    all_attrs = sorted(m for m in dir(investor) if not m.startswith("_"))
    print(all_attrs)
    candidates_for_market_class = [a for a in all_attrs if any(
        kw in a.lower() for kw in ["market", "quote", "screen", "rank", "active", "mp"]
    )]
    print(f"\n   attribute ที่น่าสนใจ (มีคำว่า market/quote/screen/rank/active): {candidates_for_market_class}")

    # --- 2) ถ้ามี MarketData class ลองสร้างแล้วดูว่ามี method อะไรให้ใช้ ---
    print("\n" + "=" * 70)
    print("2) ลองสร้าง MarketData object (ชื่อ class เดายอดฮิตของ Settrade SDK):")
    md = None
    for class_name in ["MarketData", "Market", "Quote", "RealtimeMarketData"]:
        ctor = getattr(investor, class_name, None)
        if callable(ctor):
            try:
                md = ctor()
                print(f"✅ สร้างสำเร็จจาก investor.{class_name}()")
                break
            except Exception as e:
                print(f"⚠️  investor.{class_name}() มีอยู่ แต่สร้างไม่สำเร็จ: {e}")
    if md is None:
        print("❌ ไม่พบ class ที่ชื่อคุ้นเคย (MarketData/Market/Quote/RealtimeMarketData) บน investor")
        print("   → แปลว่า SDK เวอร์ชันนี้อาจไม่รองรับ market screener ผ่านทางนี้")
    else:
        md_attrs = sorted(m for m in dir(md) if not m.startswith("_"))
        print(f"   method ทั้งหมดใน {type(md).__name__}: {md_attrs}")
        screener_candidates = [
            "get_top_active_value", "get_top_active_volume", "get_most_active",
            "get_quote_list", "get_ranking", "top_ranking", "get_top_ranking",
            "most_active_volume", "most_active_value",
        ]
        resolve_method(md, screener_candidates, "market screener")

    print("\n" + "=" * 70)
    print("สรุป: เอา output ทั้งหมดด้านบน (โดยเฉพาะรายชื่อ method จริงที่ print ออกมา)")
    print("ส่งกลับมาให้ดู จะได้รู้ชัดว่าต้องเรียก method ไหนสำหรับสแกนวอลุ่มสูงสุดทั้งตลาด")

if __name__ == "__main__":
    main()
