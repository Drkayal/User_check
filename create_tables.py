#!/usr/bin/env python3
import sqlite3
import os

DB_PATH = 'accounts.db'

def init_hunt_tables():
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            print("إنشاء جدول hunt_runs...")
            cur.execute(
                '''CREATE TABLE IF NOT EXISTS hunt_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT,
                    finished_at TEXT,
                    status TEXT,
                    category_id TEXT,
                    pattern TEXT,
                    target_type TEXT,
                    extra_usernames_json TEXT,
                    gen_done INTEGER DEFAULT 0
                )'''
            )
            
            print("إنشاء جدول hunt_items...")
            cur.execute(
                '''CREATE TABLE IF NOT EXISTS hunt_items (
                    run_id INTEGER,
                    username TEXT,
                    status TEXT,
                    score REAL,
                    last_attempt_ts TEXT,
                    attempts INTEGER DEFAULT 0,
                    PRIMARY KEY(run_id, username)
                )'''
            )
            
            print("إنشاء جدول settings...")
            cur.execute(
                '''CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )'''
            )
            
            # أعمدة إضافية اختيارية
            try:
                cur.execute('ALTER TABLE hunt_items ADD COLUMN account_id TEXT')
                print("تم إضافة عمود account_id")
            except sqlite3.OperationalError:
                print("عمود account_id موجود بالفعل")
                
            try:
                cur.execute('ALTER TABLE hunt_items ADD COLUMN failure_reason TEXT')
                print("تم إضافة عمود failure_reason")
            except sqlite3.OperationalError:
                print("عمود failure_reason موجود بالفعل")
                
            try:
                cur.execute('ALTER TABLE hunt_items ADD COLUMN claimed_at TEXT')
                print("تم إضافة عمود claimed_at")
            except sqlite3.OperationalError:
                print("عمود claimed_at موجود بالفعل")
                
            try:
                cur.execute('ALTER TABLE hunt_items ADD COLUMN checked_at TEXT')
                print("تم إضافة عمود checked_at")
            except sqlite3.OperationalError:
                print("عمود checked_at موجود بالفعل")
                
            conn.commit()
            print("تم حفظ التغييرات في قاعدة البيانات")
            
        # إنشاء جداول الطوابير الموزعة
        init_distributed_queues()
        
    except Exception as e:
        print(f"خطأ في init_hunt_tables: {e}")

def init_distributed_queues():
    try:
        with sqlite3.connect(DB_PATH, timeout=20) as conn:
            print("إنشاء جداول الطوابير الموزعة...")
            conn.execute('PRAGMA journal_mode=WAL;')
            conn.execute('PRAGMA synchronous=NORMAL;')
            cur = conn.cursor()
            
            cur.execute('''CREATE TABLE IF NOT EXISTS hunt_queue_p1 (
                               run_id INTEGER,
                               username TEXT,
                               created_at TEXT,
                               leased_by TEXT,
                               lease_until REAL,
                               PRIMARY KEY(run_id, username)
                           )''')
            print("تم إنشاء جدول hunt_queue_p1")
            
            cur.execute('CREATE INDEX IF NOT EXISTS idx_q1_lease ON hunt_queue_p1(run_id, lease_until)')
            print("تم إنشاء فهرس idx_q1_lease")
            
            cur.execute('''CREATE TABLE IF NOT EXISTS hunt_queue_p2 (
                               run_id INTEGER,
                               username TEXT,
                               score REAL,
                               created_at TEXT,
                               leased_by TEXT,
                               lease_until REAL,
                               PRIMARY KEY(run_id, username)
                           )''')
            print("تم إنشاء جدول hunt_queue_p2")
            
            cur.execute('CREATE INDEX IF NOT EXISTS idx_q2_lease ON hunt_queue_p2(run_id, lease_until, score)')
            print("تم إنشاء فهرس idx_q2_lease")
            
            conn.commit()
            print("تم إنشاء جميع جداول الطوابير بنجاح")
            
    except Exception as e:
        print(f"خطأ في init_distributed_queues: {e}")

if __name__ == '__main__':
    print("بداية إنشاء الجداول...")
    init_hunt_tables()
    print("تم إنشاء جميع الجداول بنجاح!")
    
    # عرض الجداول الموجودة
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute('SELECT name FROM sqlite_master WHERE type="table"')
        tables = cur.fetchall()
        print(f"\nالجداول الموجودة في قاعدة البيانات: {[t[0] for t in tables]}")