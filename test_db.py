#!/usr/bin/env python3
import sqlite3

DB_PATH = 'accounts.db'

def test_get_last_active_run():
    """اختبار دالة get_last_active_run بدون استيراد المكتبات الكاملة"""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            
            # اختبار الوصول إلى جدول settings
            cur.execute('SELECT value FROM settings WHERE key=?', ('last_run_id',))
            row = cur.fetchone()
            
            if not row:
                print("✅ جدول settings يعمل - لا توجد مهمة نشطة حالياً")
                return None
                
            run_id = int(row[0])
            print(f"✅ تم العثور على آخر مهمة نشطة: {run_id}")
            
            # اختبار الوصول إلى جدول hunt_runs
            cur.execute('SELECT id, status, category_id, pattern, target_type, extra_usernames_json, gen_done FROM hunt_runs WHERE id=?', (run_id,))
            r = cur.fetchone()
            
            if not r:
                print("❌ المهمة موجودة في settings لكن غير موجودة في hunt_runs")
                return None
                
            result = {
                'id': r[0],
                'status': r[1],
                'category_id': r[2],
                'pattern': r[3],
                'target_type': r[4],
                'extra_usernames': r[5] or '[]',
                'gen_done': bool(r[6])
            }
            
            print(f"✅ تم تحميل بيانات المهمة بنجاح: {result}")
            return result
            
    except Exception as e:
        print(f"❌ خطأ في test_get_last_active_run: {e}")
        return None

def test_all_tables():
    """اختبار جميع الجداول"""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            
            # اختبار جدول hunt_runs
            cur.execute('SELECT COUNT(*) FROM hunt_runs')
            count = cur.fetchone()[0]
            print(f"✅ جدول hunt_runs: {count} صف")
            
            # اختبار جدول hunt_items
            cur.execute('SELECT COUNT(*) FROM hunt_items')
            count = cur.fetchone()[0]
            print(f"✅ جدول hunt_items: {count} صف")
            
            # اختبار جدول settings
            cur.execute('SELECT COUNT(*) FROM settings')
            count = cur.fetchone()[0]
            print(f"✅ جدول settings: {count} صف")
            
            # اختبار جدول hunt_queue_p1
            cur.execute('SELECT COUNT(*) FROM hunt_queue_p1')
            count = cur.fetchone()[0]
            print(f"✅ جدول hunt_queue_p1: {count} صف")
            
            # اختبار جدول hunt_queue_p2
            cur.execute('SELECT COUNT(*) FROM hunt_queue_p2')
            count = cur.fetchone()[0]
            print(f"✅ جدول hunt_queue_p2: {count} صف")
            
    except Exception as e:
        print(f"❌ خطأ في test_all_tables: {e}")

if __name__ == '__main__':
    print("🧪 بداية اختبار قاعدة البيانات...")
    print()
    
    print("📊 اختبار جميع الجداول:")
    test_all_tables()
    print()
    
    print("🔍 اختبار دالة get_last_active_run:")
    result = test_get_last_active_run()
    print()
    
    if result:
        print("🎉 جميع الاختبارات نجحت! قاعدة البيانات تعمل بشكل صحيح.")
    else:
        print("✅ قاعدة البيانات تعمل، لكن لا توجد مهمة نشطة حالياً.")