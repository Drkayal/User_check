import os
import asyncio
import random
import string
import logging
import itertools
import sqlite3
import time
import heapq
import re
import json
from datetime import datetime
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    BotCommand
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters
)
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import (
    FloodWaitError, UsernameInvalidError, UsernameOccupiedError,
    UsernameNotOccupiedError, ChatAdminRequiredError, SessionPasswordNeededError,
    ChannelInvalidError, UserDeactivatedError, UserDeactivatedBanError,
    UsernamePurchaseAvailableError
)
from telethon import functions
from telethon.tl.functions.channels import CreateChannelRequest, UpdateUsernameRequest, DeleteChannelRequest
from telethon.tl.types import Channel, InputChannel
from encryption import decrypt_session

# إعدادات البوت
API_ID = 26924046
API_HASH = '4c6ef4cee5e129b7a674de156e2bcc15'
BOT_TOKEN = '7941972743:AAFMmZgx2gRBgOaiY4obfhawleO9p1_TYn8'
ADMIN_IDS = [985612253]  # استبدل برقمك الخاص
DB_PATH = 'accounts.db'  # قاعدة بيانات مشتركة مع بوت إضافة الحسابات
LOG_FILE = 'username_checker.log'
MAX_CONCURRENT_TASKS = 5
CLAIMED_FILE = 'claimed_usernames.txt'
FRAGMENT_FILE = 'fragment_usernames.txt'
# قائمة بجلسات البوتات للفحص
BOT_SESSIONS = [
    "1BJWap1sAUISyr-XZ8_ESa_LuEMv4gvrI1ZP0MQKTveHCCvRh7ZLHaLJPVlBExY6RHpc0yHu52TCK8Cqu3FoxKrOiGl2LdCHA6n1cVlFyan8N5_UWOAlYmRaagjODxJxlVF4XorGVI_Ml2RKcXvz71ZaBey9Y-K_Uofv-pHkN2nxG7cOdw45Dh-8Yr06Gg9b81wyUmfN0I8ZVlDsKlT68yup7zFU00VZbei6j7Ic2f8Y8So_rWCM2o8wKPwERR-mJ8A_ZOMjVinX8eFrkqbIxoYX52Si-K0z-c5jpHE2VLRsnqAhiR5iwnTc6iXbJTSUIwRzfrWbjuqVoyCZnwTUFfPfztgt-LcU=", "1BJWap1sAUFEHNUQuiQU71l3PH-MoiqWpoXpdbwR90edR9k8Y7PLGdi8nTU36WdsUP7JZAxL0T8Wo688OjYmE_5VTtGKYPIsavnQ7mOedpGFdADG_hmTp3d4CnEsc-yWZbgYc4_hN8xadUbCD6yrUoLvb_kcTN1S6GvL_hxckkMYwd14hyDj_SjxLTQe_lKd-wdkn9haFOwXBHNJ7XiSMSIXfig3tdmaJ8PWnGtUDM4XM-d_x2q0b8SYsGi702bAGX-ohwnYtnQKJW6lZ_VPxje23auSX67QR5y2p3bKIygXP6QMa4jBNT0G7EZSJmITSWNZlo32FfYHDlb9mw2SuMscnWLKNEG0=", "1BJWap1sAUH7UfXGeNoTKG4TR1Vkw89xwWzFOBwrv_n8Trlnf-klEMGJT-3ed3SoGVpZlNO9ImrGQrqGtViNbaM00PEu6soA-SGf8RU5LwXme9iw5mlTInX-sMJAqENbrIQc9JDtmkPLvPxIAY2jpGJ7JoxNtbVBbGHoo6QSkXJPBplBilqNpSL89RAH2kHjjoBe3z9xoRG1yjvh-zmgarPmoud0AJRa0neE1ssuRfOOE6J4rt0DMa127OogFFnKL6NgpjRH15KpRU_DboQUvhpVGt6mWmRJG4pW7UmbBqAf5BnMBMyEI3zcZGhf-a3Jjo4OdkVMJoEnKQDRsEF1w5nmfAXexud8=", "1BJWap1sAUFCCcN-1lqCyxL3PxZeUgH0LQFipkNsiQ9DyPOEHgsixt4VOA3GKSAqTFdC1BcyTi51GJlpEsuskJfdl7cnuZLPlQgMXQOKeJh8B_tylOv9EozMDGur4H8PhcNyHh3oC4pT6UQ_lDrXNtIk-_0z3m-QWyXjlor1kCBaBJCoqW4EhkEO52agErVzN1nfffAVC6YmKXS9B8laXFnXs72pPQii52na2Bx7oymX2UhKIUeolZWt7_iesJHZO7RedSGE-UoKt75vNghn6y-fq041R97hKzu60Ts9ZKCj8GGBo9chc29pNat3NSSO3KXBBBv6atSa3yM76dqfaROrdjzMtwb8=", "1BJWap1sAUDq3Pq6nlCgPC4dxOGGjwmI6MOfu9wK8I2yPuXbXyFRXhXKebBtrsBwZ0H_ilG3ab0F_50ePxjjuIiCjjy8gL8AOBFaHO0DMcZl81eWkhG9ogz28i_EvMPQslBKynrdyepq6k5o1j5HJ-zdiXth-fnEW8JS7Hm_Kik_5Jc082AzDiGbgFYGwWs8Qx3p8jQX8cegVitXSwH0lU--CvYWuz7psIeY8uWlFveYJRZ_fdJEfUWO0J1UEpYpo-WODAiVI6-1VdTzOJ6OMX5DHxi0yGPTQp8ruZ3NFEraSj4tFt3I3GgbeU96wjTG7XUQWZJDZInuiMKW-EWHX7SDgnuRpeAM=", "1BJWap1sAUJJ1_qUKh1Lg8zy4nenSyHEfZpa68GNtNEM-77644Dd7f_lRcJ0rpsJWoajhZQConiZ8cZgoq4briC-DBsnnRFyaBFz8BDuvriFdQRKrYA9WsDivQZafojHzsgeNoUpopylvtefTajmdss2tK_q6oaBCisn24_cI9SmwfgdDw2CMYCFI6j_kSgAGHLO71577eRXJfjAqnHz1nssi5Oph8-cWhJ3Csl6KEs_7rKYX5tWH5ZVD_XYpnV8nUr5jbSgBQxs1IgeAeQsT3SSn0ykxtWcVMbOOTLzqBlLu97xZqDBDnr8zkcc5MfNHBpjgLan_W4DbcGqRSLllMxeAtBLVOUU=", "1BJWap1sAUI-a0iDjLFzKjsvoNOpaOhlNt2ygPyLfU98C4Ob9R4kEVLSzprsuvpCbBwAswIKcQLMItqNLOMu4CElDbALBqPDl-o4694xoyXhc88r7DwlZPaqAjxngrg1i8SqdfgSdFpI3-0v_0sb_bjV2r12R5wSTl1jAWFQsWtg8uBhnj0u8F8pNoCMiR5NxpOlv4u8n9MHCfN4Ust0ZutClnZ9UZTilbmqZGJJRgoRhQwrfDNIjrSJgXxhuzBlJ82HD3B4WP7ZRBVI0PDBefzF0w81RMWxOTh0fQ_eVaWVlKSVXTv93JLZiaDStHYIsAojPksqc43RpF-x9EyK9DLzKMt2xnD0=", "1BJWap1sAUKA7tcITef_VI_xUQzrdk4ggX1sNfY4Z-qkmHiM51Asf86OHmVyerOiRs0mHWrjdWhraPxIrZd-6LBQLQn7DOqIoBiW_flH75QjgXbAE7wwaxMykQ6WKlO7rPdnSxJV-zeX9UGiaRtAxrP0_kLEZRe80y5pK0vmIns7_P18XeiX0YwjmnjDmT1qung65grtR9T0LtgSSOitit0x7_CayNYYOJ_WCnWr2ahF87kP1koss93qN9FzmrECaYffSRRYsvw0XeINqtu2V23FDUiPyNvSIshJDU0Omv2TGDsEttOKu9rgZjWZAiXV44W6j8WVtOLD6b3k2r3dKc1-KnGaBJWk="
    # ... إضافة جلسات بوتات أخرى هنا
]

# حالات المحادثة
SELECT_CATEGORY, ENTER_PATTERN, HUNTING_IN_PROGRESS = range(3)
HUNTING_PAUSED = 3  # حالة جديدة للإيقاف المؤقت
# حالات جديدة للمرحلة 1
PREVIEW_PATTERN = 4
ENTER_NAME_LIST = 5

# ثوابت النظام
MAX_COOLDOWN_TIME = 150  # أقصى وقت تبريد مسموح به (ساعة واحدة)
EMERGENCY_THRESHOLD = 150  # 5 دقائق للتحول لحالة الطوارئ
MIN_WAIT_TIME = 0.5  # الحد الأدنى للانتظار بين الطلبات
MAX_WAIT_TIME = 3.0  # الحد الأقصى للانتظار بين الطلبات
ACCOUNT_CHECK_RATIO = 0.3  # نسبة استخدام الحسابات في حالة الطوارئ
# حد أقصى لمجموعة الأسماء التي تمت زيارتها لتفادي تضخم الذاكرة
VISITED_MAX_SIZE = int(os.getenv('VISITED_MAX_SIZE', '200000'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
logging.getLogger('telethon').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)  # تقليل تسجيل طلبات HTTP

# عدّاد تسلسلي عالمي لكسر التعادلات في طابور الأولوية
_SEQ = itertools.count()

# فئات القوالب
TEMPLATE_TYPES = {
    '١': ('char', 'fixed', string.ascii_uppercase),    # حرف موحد (كبير)
    '٢': ('char', 'full', string.ascii_lowercase),     # حرف عشوائي (صغير) - كل الاحتمالات
    '٣': ('digit', 'fixed', string.digits),            # رقم موحد
    '٤': ('digit', 'full', string.digits),             # رقم عشوائي - كل الاحتمالات
    '_': ('literal', '_', ['_']),                      # حرف ثابت
    'bot': ('literal', 'bot', ['bot'])                      # حرف ثابت
}

# ============== أدوات مساعدة للمرحلة 1: التصفية والمعاينة ==============
USERNAME_RE = re.compile(r'^[a-z0-9](?:[a-z0-9_]{3,30})[a-z0-9]$')

def normalize_username_input(text: str) -> str:
    """تطبيع إدخال اسم المستخدم إلى صيغة تبدأ بـ @ وحروف صغيرة."""
    if not text:
        return ''
    name = text.strip().lstrip('@').strip()
    name = name.lower()
    return f"@{name}" if name else ''


def is_valid_username_local(username: str) -> bool:
    """التحقق المحلي من صلاحية اسم المستخدم دون طلبات شبكة.
    القواعد: طول 5-32، أحرف [a-z0-9_] فقط، لا يبدأ/ينتهي بـ '_'، لا يحتوي '__'.
    """
    if not username:
        return False
    name = username.lstrip('@').lower()
    if not USERNAME_RE.match(name):
        return False
    if '__' in name:
        return False
    return True


def ensure_visited_capacity(visited: set):
    """تفادي تضخم الذاكرة لمجموعة visited."""
    try:
        if len(visited) > VISITED_MAX_SIZE:
            visited.clear()
    except Exception:
        pass


def generate_preview_samples(pattern: str, max_samples: int = 100) -> list:
    """توليد عينة من أسماء المستخدمين الصالحة بناءً على القالب محلياً."""
    gen = UsernameGenerator(pattern)
    samples = []
    seen = set()
    try:
        for username in gen.generate_usernames():
            if len(samples) >= max_samples:
                break
            u = normalize_username_input(username)
            if u in seen:
                continue
            if is_valid_username_local(u):
                samples.append(u)
                seen.add(u)
    except Exception as e:
        logger.error(f"خطأ أثناء توليد عينة المعاينة: {e}")
    return samples


def parse_username_list(text: str) -> tuple[set, int]:
    """تحويل نص المستخدم إلى مجموعة أسماء مطبّعة مع عدّاد غير الصالح."""
    raw_parts = re.split(r'[\s,\n\r]+', text or '')
    valid_set = set()
    invalid_count = 0
    for part in raw_parts:
        part = part.strip()
        if not part:
            continue
        u = normalize_username_input(part)
        if is_valid_username_local(u):
            valid_set.add(u)
        else:
            invalid_count += 1
    return valid_set, invalid_count

# ============== أدوات مساعدة للمرحلة 2: التقييم والأولوية ==============
def score_username(name_no_at: str) -> float:
    """حساب درجة أولوية للاسم: الأصغر أفضل، بدون '_' أفضل، تنويع الأحرف أفضل،
    مع عقوبة على التكرارات المتتالية وكثرة الأرقام/الشرطات.
    قيمة أصغر تعني أولوية أعلى.
    """
    n = name_no_at.lower()
    length = len(n)
    underscore_count = n.count('_')
    digit_count = sum(c.isdigit() for c in n)
    # عقوبة التكرار المتتالي
    consecutive_penalty = 0
    for i in range(1, length):
        if n[i] == n[i-1]:
            consecutive_penalty += 0.5
    # مكافأة تنويع الأحرف
    unique_chars = len(set(n))
    diversity_bonus = min(2.0, unique_chars / max(1, length) * 2.0)  # 0..2
    # عقوبة كثرة الأرقام
    digit_ratio = digit_count / max(1, length)
    digit_penalty = 0.5 if digit_ratio > 0.6 else 0.0
    # عقوبة الشرطة السفلية الكثيرة
    underscore_penalty = 0.3 * underscore_count
    # المعادلة النهائية
    score = (
        length
        + consecutive_penalty
        + digit_penalty
        + underscore_penalty
        - diversity_bonus
    )
    return round(score, 4)

# ============================ ديكورات التحقق ============================
def owner_only(func):
    """ديكور للتحقق من أن المستخدم هو المالك"""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            if update.callback_query:
                await update.callback_query.answer("⛔ هذا البوت مخصص للمالك فقط.", show_alert=True)
            elif update.message:
                await update.message.reply_text("⛔ هذا البوت مخصص للمالك فقط.")
            return
        return await func(update, context)
    return wrapper

# ============================ فئات التوليد ============================
class UsernameGenerator:
    """مولد يوزرات فعال حسب الطلب الجديد مع دعم الأحرف الثابتة"""
    def __init__(self, template):
        self.template = template
        self.groups = self._parse_template()
        
    def _parse_template(self):
        groups = []
        current_group = None
        fixed_chars = []  # لتخزين الأحرف الثابتة
        
        for char in self.template:
            if char in TEMPLATE_TYPES:
                # إذا كانت هناك أحرف ثابتة مكتشفة، نضيفها كمجموعة ثابتة
                if fixed_chars:
                    groups.append(('fixed', ''.join(fixed_chars)))
                    fixed_chars = []
                
                p_type, p_subtype, charset = TEMPLATE_TYPES[char]
                
                if current_group and current_group[0] == p_type and current_group[1] == p_subtype:
                    current_group[2] += 1
                else:
                    if current_group:
                        groups.append(current_group)
                    current_group = [p_type, p_subtype, 1, charset]
            else:
                # حرف ثابت (ليس من الرموز الخاصة)
                if current_group:
                    groups.append(current_group)
                    current_group = None
                fixed_chars.append(char)
        
        # إضافة المجموعة الأخيرة إذا كانت موجودة
        if current_group:
            groups.append(current_group)
        
        # إضافة أي أحرف ثابتة متبقية
        if fixed_chars:
            groups.append(('fixed', ''.join(fixed_chars)))
            
        return groups
    
    def generate_usernames(self):
        """توليد يوزرات باستخدام الضرب الديكارتي لكل الاحتمالات مع دمج الثوابت"""
        group_values = []
        
        for group in self.groups:
            if group[0] == 'fixed':
                # للمجموعات الثابتة: نستخدم القيمة كما هي
                group_values.append([group[1]])
            else:
                g_type, g_subtype, g_length, charset = group
                if g_type == 'literal':
                    values = [charset[0] * g_length]
                elif g_subtype == 'fixed':
                    values = [char * g_length for char in charset]
                elif g_subtype == 'full':
                    # توليد كل التركيبات الممكنة لهذه المجموعة
                    values = [''.join(p) for p in itertools.product(charset, repeat=g_length)]
                group_values.append(values)
        
        # توليد الضرب الديكارتي بين المجموعات
        for parts in itertools.product(*group_values):
            yield '@' + ''.join(parts)

# ============================ إدارة الجلسات ============================
class SessionManager:
    """مدير جلسات متقدم مع دعم الفئات"""
    def __init__(self, category_id=None):
        self.sessions = {}  # {account_id: {'client': TelegramClient, 'phone': str}}
        self.accounts_queue = asyncio.PriorityQueue()  # (priority, account_id)
        self.category_id = category_id
        self.channel_pool = {}  # {account_id: [{'channel': InputChannel, 'used': bool}]}
        self.account_priority = {}  # {account_id: priority (wait time)}
        self.banned_accounts = set()  # الحسابات المحظورة مؤقتاً
        
    async def load_sessions(self):
        """تحميل الجلسات من قاعدة البيانات مع تخطي الحسابات المحظورة"""
        try:
            with sqlite3.connect(DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT id, session_str, phone 
                    FROM accounts 
                    WHERE category_id = ? AND is_active = 1
                """, (self.category_id,))
                accounts = cursor.fetchall()
                
            if not accounts:
                logger.error("لا توجد حسابات متاحة في هذه الفئة!")
                return
                
            for account_id, encrypted_session, phone in accounts:
                try:
                    session_str = decrypt_session(encrypted_session)
                    client = TelegramClient(StringSession(session_str), API_ID, API_HASH)
                    await client.connect()
                    if not client.is_connected():
                        await client.start()
                    try:
                        me = await client.get_me()
                        if me.bot:
                            logger.error(f"الحساب بوت: {phone} - لا يمكن استخدام البوتات في هذه العملية")
                            continue
                        await client.get_dialogs(limit=1)
                    except (UserDeactivatedError, UserDeactivatedBanError, ChannelInvalidError) as e:
                        logger.error(f"الحساب محظور أو معطل: {phone} - {e}")
                        continue
                    except Exception as e:
                        logger.error(f"خطأ في التحقق من الحساب {phone}: {e}")
                        continue
                    # تخزين الجلسة بدون إنشاء قناة مبكراً
                    self.sessions[account_id] = {
                        'client': client,
                        'phone': phone,
                        'account_id': account_id
                    }
                    self.channel_pool.setdefault(account_id, [])
                    self.account_priority[account_id] = 0
                    await self.accounts_queue.put((0, account_id))
                    logger.info(f"تم تحميل الجلسة: {phone}")
                except SessionPasswordNeededError:
                    logger.error(f"الجلسة {phone} محمية بكلمة مرور ثنائية. تخطي.")
                except Exception as e:
                    logger.error(f"خطأ في تحميل الجلسة {phone}: {str(e)}")
        except Exception as e:
            logger.error(f"خطأ في قراءة قاعدة البيانات: {str(e)}")
    
    async def get_or_create_channel(self, account_id: str, target_type: str = 'channel', pool_limit: int = 2) -> InputChannel | None:
        """الحصول على قناة جاهزة من مسبح الحساب أو إنشاء قناة جديدة عند الحاجة."""
        pool = self.channel_pool.setdefault(account_id, [])
        # إعادة استخدام قناة غير مستخدمة إن وجدت
        for entry in pool:
            if not entry.get('used'):
                return entry.get('channel')
        # إنشاء عند الحاجة إذا لم يتجاوز الحد
        if len(pool) >= pool_limit:
            # لا توجد قناة متاحة وغير مستخدمة ضمن الحد
            return None
        try:
            client = self.sessions[account_id]['client']
            channel_name = f"Reserve {'Group' if target_type=='group' else 'Channel'} {random.randint(10000, 99999)}"
            is_group = (target_type == 'group')
            created = await client(CreateChannelRequest(
                title=channel_name,
                about="قناة/مجموعة مؤقتة لتثبيت اليوزرات",
                megagroup=is_group
            ))
            chat = created.chats[0]
            if not isinstance(chat, Channel):
                logger.error(f"فشل إنشاء {'مجموعة' if is_group else 'قناة'} للحساب {account_id}: النوع غير صحيح")
                return None
            input_channel = InputChannel(chat.id, chat.access_hash)
            pool.append({'channel': input_channel, 'used': False})
            logger.info(f"تم إنشاء {( 'مجموعة' if is_group else 'قناة')} احتياطية: {chat.id} للحساب {account_id}")
            return input_channel
        except Exception as e:
            logger.error(f"خطأ في إنشاء القناة للحساب {account_id}: {e}")
            return None

    def mark_channel_used(self, account_id: str, input_channel: InputChannel) -> None:
        pool = self.channel_pool.get(account_id, [])
        for entry in pool:
            ch = entry.get('channel')
            if isinstance(ch, InputChannel) and ch.channel_id == input_channel.channel_id:
                entry['used'] = True
                break

    async def get_account(self, timeout=30):
        """الحصول على حساب متاح من الطابور مع مهلة"""
        try:
            # انتظار حساب متاح مع المهلة
            _, account_id = await asyncio.wait_for(self.accounts_queue.get(), timeout=timeout)
            return self.sessions[account_id]
        except asyncio.TimeoutError:
            return None
    
    async def release_account(self, account_id, priority=None):
        """إعادة الحساب إلى الطابور"""
        if account_id in self.banned_accounts:
            return
        if priority is None:
            # زيادة الأولوية (الانتظار) لتجنب الاستخدام المكثف
            self.account_priority[account_id] += 1
        else:
            self.account_priority[account_id] = priority
        await self.accounts_queue.put((self.account_priority[account_id], account_id))
    
    async def mark_account_banned(self, account_id, ban_duration=3600):
        """وضع علامة على الحساب كمحظور مؤقتاً"""
        self.banned_accounts.add(account_id)
        # بعد مدة الحظر، نعيد الحساب
        asyncio.create_task(self._unban_account_after(account_id, ban_duration))
    
    async def _unban_account_after(self, account_id, delay):
        await asyncio.sleep(delay)
        self.banned_accounts.remove(account_id)
        await self.release_account(account_id, priority=0)  # أولوية عالية عند العودة
    
    def get_session_string(self, client):
        """الحصول على كود الجلسة"""
        return client.session.save()
    
    async def cleanup_unused_channels(self):
        """حذف القنوات التي لم يتم استخدامها (لم يثبت عليها يوزر)"""
        for account_id, pool in self.channel_pool.items():
            if account_id not in self.sessions or account_id in self.banned_accounts:
                continue
            client = self.sessions[account_id]['client']
            for entry in list(pool):
                if not entry.get('used'):
                    ch = entry.get('channel')
                    try:
                        await client(DeleteChannelRequest(channel=ch))
                        logger.info(f"تم حذف القناة/المجموعة غير المستخدمة: {ch.channel_id}")
                        pool.remove(entry)
                    except Exception as e:
                        logger.error(f"خطأ في حذف القناة {ch.channel_id}: {e}")

# ============================ نظام الحجز ============================
class AdvancedUsernameClaimer:
    """نظام متقدم لإدارة القنوات والتثبيت مع إرجاع معلومات الحساب"""
    def __init__(self, session_string, session_manager):
        self.session_string = session_string
        self.client = None
        self.session_manager = session_manager
        
    async def start(self):
        """بدء تشغيل العميل"""
        try:
            self.client = TelegramClient(
                StringSession(self.session_string), 
                API_ID, 
                API_HASH
            )
            await self.client.connect()
            
            if not await self.client.is_user_authorized():
                await self.client.start()
                if not await self.client.is_user_authorized():
                    raise Exception("فشل تفعيل الجلسة")
            
            return True
        except SessionPasswordNeededError:
            logger.error("الحساب محمي بكلمة مرور ثنائية. لا يمكن استخدامه.")
            return False
        except Exception as e:
            logger.error(f"فشل بدء الجلسة: {e}")
            return False
    
    async def is_username_available(self, username):
        """فحص توفر اليوزر"""
        try:
            await self.client.get_entity(username)
            return False
        except (ValueError, UsernameInvalidError):
            return True
        except Exception as e:
            logger.error(f"خطأ غير متوقع: {e}")
            return False

    async def claim_username(self, input_channel, username, max_attempts=3):
        """تثبيت اليوزر على القناة المحددة وإرجاع معلومات الحساب عند النجاح"""
        username_text = username.lstrip('@')
        
        for attempt in range(max_attempts):
            try:
                await self.client(UpdateUsernameRequest(
                    channel=input_channel,
                    username=username_text
                ))
                
                # تسجيل اليوزر المحجوز
                with open(CLAIMED_FILE, 'a', encoding='utf-8') as f:
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    f.write(f"{timestamp}: {username}\n")
                
                logger.info(f"تم تثبيت اليوزر: @{username_text}")
                
                # الحصول على معلومات الحساب للإشعار
                try:
                    me = await self.client.get_me()
                    account_info = {
                        'username': me.username,
                        'phone': me.phone,
                        'id': me.id
                    }
                except Exception as e:
                    logger.error(f"خطأ في الحصول على معلومات الحساب: {e}")
                    account_info = None
                
                return True, account_info
                
            except UsernamePurchaseAvailableError:
                logger.info(f"اليوزر معروض للبيع على Fragment: @{username_text}")
                return False, None  # لا يمكن حجزه
            except UsernameOccupiedError:
                logger.info(f"اليوزر محجوز الآن: @{username_text}")
                return False, None
            except UsernameInvalidError:
                logger.info(f"اليوزر غير صالح: @{username_text}")
                return False, None
            except FloodWaitError as e:
                # تحديد وقت الانتظار مع الحد الأقصى
                wait_time = min(e.seconds + random.randint(10, 30), MAX_COOLDOWN_TIME)
                logger.warning(f"فيضان! انتظار {wait_time} ثانية...")
                await asyncio.sleep(wait_time)
            except (ChannelInvalidError, ChatAdminRequiredError) as e:
                logger.error(f"خطأ دائم في الحساب: {e}")
                return False, None
            except Exception as e:
                error_msg = str(e)
                logger.error(f"خطأ في المحاولة {attempt+1}: {error_msg}")
                
                # معالجة خاصة لأخطاء InputEntity
                if "input entity" in error_msg.lower():
                    logger.warning("خطأ في الكيان المدخل، إعادة المحاولة بعد وقت قصير...")
                    await asyncio.sleep(2)
                else:
                    # زيادة وقت الانتظار مع كل محاولة فاشلة
                    wait_time = 2 + attempt * 2
                    await asyncio.sleep(wait_time)
        
        logger.warning(f"فشل التثبيت بعد {max_attempts} محاولات")
        return False, None

    async def cleanup(self):
        """تنظيف الموارد"""
        if self.client:
            await self.client.disconnect()

# ============================ نظام الفحص ============================
class UsernameChecker:
    """نظام فحص وحجز متقدم مع تعدد البوتات"""
    def __init__(self, bot_clients, session_manager):
        self.bot_clients = bot_clients
        self.session_manager = session_manager
        self.current_bot_index = 0
        self.reserved_usernames = []
        # طابور أولوية للأسماء المتاحة للتثبيت: (score, seq, username)
        self.available_usernames_queue = asyncio.PriorityQueue()
        self.claimed_usernames = []
        self.fragment_usernames = []
        self.lock = asyncio.Lock()
        self.bot_cooldown = {}
        self.cooldown_lock = asyncio.Lock()
        self.last_emergency_time = 0
        self.account_usage_counter = 0
        
    def get_next_bot_index(self):
        """الحصول على مؤشر البوت التالي مع تخطي المعطلة"""
        original_index = self.current_bot_index
        for _ in range(len(self.bot_clients)):
            self.current_bot_index = (self.current_bot_index + 1) % len(self.bot_clients)
            if self.bot_clients[self.current_bot_index] is not None:
                return self.current_bot_index
        # إذا لم يتم العثور على بوت نشط، إرجاع الفهرس الأصلي
        return original_index
    
    async def get_checker_client(self):
        """الحصول على عميل للفحص مع التعامل مع التبريد وحالة الطوارئ"""
        now = time.time()
        
        # حالة الطوارئ: إذا مر وقت معين منذ آخر استخدام للحسابات
        if now - self.last_emergency_time > EMERGENCY_THRESHOLD:
            # استخدام الحسابات بنسبة معينة
            if random.random() < ACCOUNT_CHECK_RATIO:
                logger.warning("حالة الطوارئ: استخدام الحسابات للفحص...")
                account_data = await self.session_manager.get_account(timeout=5)
                if account_data:
                    self.last_emergency_time = now
                    self.account_usage_counter += 1
                    return account_data['client'], 'account', account_data['account_id']
        
        # البحث عن أول بوت متاح
        async with self.cooldown_lock:
            for _ in range(len(self.bot_clients)):
                bot_index = self.get_next_bot_index()
                cooldown_end = self.bot_cooldown.get(bot_index, 0)
                
                if now >= cooldown_end:
                    return self.bot_clients[bot_index], 'bot', bot_index
        
        # إذا لم يتم العثور على بوت متاح، استخدام الحسابات
        logger.warning("لا توجد بوتات متاحة، استخدام الحسابات للفحص...")
        account_data = await self.session_manager.get_account(timeout=5)
        if account_data:
            self.account_usage_counter += 1
            return account_data['client'], 'account', account_data['account_id']
        
        # إذا فشل كل شيء، انتظر أقصر وقت تبريد
        min_cooldown = min(self.bot_cooldown.values(), default=0)
        wait_time = max(min_cooldown - now, 0.1)
        logger.warning(f"انتظار {wait_time:.1f} ثانية لتحرير بوت...")
        await asyncio.sleep(wait_time)
        return await self.get_checker_client()
    
    async def bot_check_username(self, username):
        """المرحلة الأولى: فحص اليوزر"""
        try:
            client, client_type, client_id = await self.get_checker_client()
            try:
                await client.get_entity(username)
                async with self.lock:
                    self.reserved_usernames.append(username)
                # تحديث الحالة في التخزين الدائم
                run_id = self.session_manager.category_id and None
                try:
                    run_id = int(self.session_manager.category_id)  # placeholder, سيتم تعيين صحيح لاحقاً من context
                except:
                    pass
                try:
                    update_item_status(self.session_manager.sessions and int(self.session_manager.sessions.get('run_id', 0)) or 0, username, ITEM_STATUS_RESERVED)
                except:
                    pass
                logger.info(f"اليوزر محجوز: {username}")
                return "reserved"
            except (UsernameInvalidError, ValueError):
                # تمرير للمرحلة الثانية
                name_no_at = username.lstrip('@')
                score = score_username(name_no_at)
                seq = next(_SEQ)
                await self.available_usernames_queue.put((score, seq, username))
                try:
                    # علامة Available
                    # سيتم تعيين run_id الصحيح من السياق في عامل الإنتاج؛ إذا لم يكن متاحاً نتجاهل
                    pass
                except:
                    pass
                logger.info(f"تم تمرير اليوزر للمرحلة الثانية: {username}")
                return "available"
            except UsernamePurchaseAvailableError:
                async with self.lock:
                    self.reserved_usernames.append(username)
                    self.fragment_usernames.append(username)
                try:
                    update_item_status(self.session_manager.sessions and int(self.session_manager.sessions.get('run_id', 0)) or 0, username, ITEM_STATUS_FRAGMENT)
                except:
                    pass
                logger.info(f"اليوزر معروض للبيع على Fragment: {username}")
                return "reserved"
            except FloodWaitError as e:
                # تحديد وقت الانتظار مع الحد الأقصى
                wait_time = min(e.seconds + random.randint(10, 30), MAX_COOLDOWN_TIME)
                logger.warning(f"فيضان! وضع العميل في التبريد لمدة {wait_time} ثانية...")
                
                # تحديد وقت انتهاء التبريد
                if client_type == 'bot':
                    async with self.cooldown_lock:
                        self.bot_cooldown[client_id] = time.time() + wait_time
                return await self.bot_check_username(username)
            except Exception as e:
                logger.error(f"خطأ في فحص اليوزر {username}: {str(e)}")
                # تبريد العميل لمدة قصيرة
                if client_type == 'bot':
                    async with self.cooldown_lock:
                        self.bot_cooldown[client_id] = time.time() + 10
                return "error"
            finally:
                # إعادة الحساب إلى الطابور إذا كان نوعه حساب
                if client_type == 'account':
                    await self.session_manager.release_account(client_id)
        except Exception as e:
            logger.error(f"خطأ جسيم في الحصول على عميل فحص: {e}")
            return "error"

# ============================ عمال المعالجة ============================
async def worker_bot_check(queue, checker, stop_event, pause_event, context):
    """عامل لفحص اليوزرات (المرحلة الأولى)"""
    while not stop_event.is_set():
        try:
            if pause_event.is_set():
                await asyncio.sleep(1)
                continue
            item = await asyncio.wait_for(queue.get(), timeout=1.0)
            if item is None:
                queue.task_done()
                break
            username = item
            # فلترة سريعة وفق allow/block
            if not is_username_allowed(context, username):
                queue.task_done()
                continue
            # وقت انتظار عشوائي ديناميكي
            runtime = context.user_data.get('runtime', {})
            min_w = runtime.get('min_wait', MIN_WAIT_TIME)
            max_w = runtime.get('max_wait', MAX_WAIT_TIME)
            wait_time = random.uniform(min_w, max_w)
            await asyncio.sleep(wait_time)

            await checker.bot_check_username(username)
            queue.task_done()
        except asyncio.TimeoutError:
            if stop_event.is_set():
                break
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"خطأ في عامل الفحص: {e}")

async def worker_account_claim(queue, checker, session_manager, stop_event, pause_event, context, progress_callback=None):
    """عامل لتثبيت اليوزرات بالحسابات (المرحلة الثانية) مع إرسال إشعارات"""
    while not stop_event.is_set():
        try:
            if pause_event.is_set():
                await asyncio.sleep(1)
                continue
            item = await asyncio.wait_for(queue.get(), timeout=1.0)
            if item is None:
                queue.task_done()
                break
            if isinstance(item, tuple) and len(item) == 3:
                _, _, username = item
            else:
                username = item
            # فلترة وفق allow/block كتحقق أخير
            if not is_username_allowed(context, username):
                queue.task_done()
                continue
            # تحديث إلى AVAILABLE قبل المحاولات
            try:
                update_item_status(context.user_data.get('run_id', 0), username, ITEM_STATUS_AVAILABLE)
            except:
                pass
            # Recheck سريع ...
            try:
                client, ctype, cid = await checker.get_checker_client()
                try:
                    await client.get_entity(username)
                    # أصبح محجوزاً الآن
                    update_item_status(context.user_data.get('run_id', 0), username, ITEM_STATUS_RESERVED)
                    queue.task_done()
                    if ctype == 'account':
                        await session_manager.release_account(cid)
                    continue
                except (UsernameInvalidError, ValueError):
                    pass
                except UsernamePurchaseAvailableError:
                    update_item_status(context.user_data.get('run_id', 0), username, ITEM_STATUS_FRAGMENT)
                    async with checker.lock:
                        checker.fragment_usernames.append(username)
                    queue.task_done()
                    if ctype == 'account':
                        await session_manager.release_account(cid)
                    continue
                finally:
                    if ctype == 'account':
                        await session_manager.release_account(cid)
            except Exception:
                pass

            max_accounts_try = min(5, max(1, len(session_manager.sessions)))
            attempts = 0
            claimed = False
            while attempts < max_accounts_try and not claimed and not stop_event.is_set():
                account_data = await session_manager.get_account(timeout=60)
                if account_data is None:
                    attempts += 1
                    continue
                account_id = account_data['account_id']
                client = account_data['client']
                phone = account_data['phone']
                claimer = None
                try:
                    target_type = context.user_data.get('target_type', 'channel')
                    input_channel = None
                    if target_type in ('channel', 'group'):
                        input_channel = await session_manager.get_or_create_channel(account_id, target_type)
                        if not input_channel:
                            await session_manager.release_account(account_id)
                            attempts += 1
                            continue
                    elif target_type == 'self':
                        try:
                            if claimer is None:
                                claimer = AdvancedUsernameClaimer(session_manager.get_session_string(client), session_manager)
                                started = await claimer.start()
                                if not started:
                                    await session_manager.release_account(account_id)
                                    attempts += 1
                                    continue
                            await claimer.client(functions.account.UpdateUsername(username=username.lstrip('@')))
                            async with checker.lock:
                                checker.claimed_usernames.append(username)
                            update_item_status(context.user_data.get('run_id', 0), username, ITEM_STATUS_CLAIMED)
                            if progress_callback:
                                await progress_callback(f"✅ تم تثبيت اليوزر على الحساب: {username}")
                            queue.task_done()
                            claimed = True
                            break
                        except Exception as e:
                            logger.error(f"فشل التثبيت على الحساب مباشرة: {e}")
                            update_item_status(context.user_data.get('run_id', 0), username, ITEM_STATUS_FAILED, inc_attempt=True)
                            await session_manager.release_account(account_id)
                            attempts += 1
                            continue
                    session_string = session_manager.get_session_string(client)
                    if claimer is None:
                        claimer = AdvancedUsernameClaimer(session_string, session_manager)
                        started = await claimer.start()
                        if not started:
                            update_item_status(context.user_data.get('run_id', 0), username, ITEM_STATUS_FAILED, inc_attempt=True)
                            await session_manager.release_account(account_id)
                            attempts += 1
                            continue
                    success, account_info = await claimer.claim_username(input_channel, username)
                    if success:
                        if input_channel:
                            session_manager.mark_channel_used(account_id, input_channel)
                        async with checker.lock:
                            checker.claimed_usernames.append(username)
                        update_item_status(context.user_data.get('run_id', 0), username, ITEM_STATUS_CLAIMED)
                        if progress_callback:
                            await progress_callback(f"✅ تم تثبيت اليوزر: {username}")
                        try:
                            me = await client.get_me()
                            account_username = f"@{me.username}" if me.username else f"+{me.phone}"
                            notification = (
                                f"🎉 **تم تثبيت يوزر جديد بنجاح!**\n\n"
                                f"• اليوزر المحجوز: `{username}`\n"
                                f"• الحساب: `{account_username}`\n"
                                f"• رقم الهاتف: `+{phone}`\n"
                                f"• معرّف الحساب: `{me.id}`"
                            )
                            for admin_id in ADMIN_IDS:
                                await context.bot.send_message(
                                    chat_id=admin_id,
                                    text=notification,
                                    parse_mode="Markdown"
                                )
                        except Exception as e:
                            logger.error(f"خطأ في إرسال الإشعار: {e}")
                        queue.task_done()
                        claimed = True
                    else:
                        update_item_status(context.user_data.get('run_id', 0), username, ITEM_STATUS_FAILED, inc_attempt=True)
                        attempts += 1
                except (UserDeactivatedError, UserDeactivatedBanError):
                    logger.error("الحساب محظور. إزالته من الطابور مؤقتاً.")
                    await session_manager.mark_account_banned(account_id)
                    attempts += 1
                except Exception as e:
                    logger.error(f"خطأ في عامل التثبيت: {e}")
                    update_item_status(context.user_data.get('run_id', 0), username, ITEM_STATUS_FAILED, inc_attempt=True)
                    attempts += 1
                finally:
                    if claimer:
                        try:
                            await claimer.cleanup()
                        except:
                            pass
                    if account_id not in session_manager.banned_accounts:
                        try:
                            await session_manager.release_account(account_id)
                        except:
                            pass
            if not claimed:
                queue.task_done()
            runtime = context.user_data.get('runtime', {})
            min_w = runtime.get('min_wait', MIN_WAIT_TIME)
            max_w = runtime.get('max_wait', MAX_WAIT_TIME)
            await asyncio.sleep(random.uniform(min_w, max_w))
        except asyncio.TimeoutError:
            if stop_event.is_set():
                break
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"خطأ في عامل التثبيت: {e}")
            try:
                queue.task_done()
            except:
                pass

# ============================ أوامر التحكم ============================
@owner_only
async def cmd_speed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = (update.message.text or '').split()
    if len(args) < 2:
        await update.message.reply_text("استخدم: /speed safe|normal|fast")
        return
    mode = args[1].lower()
    if mode not in {"safe", "normal", "fast"}:
        await update.message.reply_text("الوضع غير صالح. اختر: safe, normal, fast")
        return
    num_accounts = len(context.user_data.get('session_manager', SessionManager()).sessions) if context.user_data.get('session_manager') else 0
    msg = apply_speed_mode(context, mode, num_accounts)
    await adjust_workers(context)
    await update.message.reply_text("✅ " + msg)

@owner_only
async def cmd_workers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = (update.message.text or '').split()
    if len(args) < 2 or not args[1].isdigit():
        await update.message.reply_text("استخدم: /workers N")
        return
    n = max(1, min(20, int(args[1])))
    num_accounts = len(context.user_data.get('session_manager', SessionManager()).sessions) if context.user_data.get('session_manager') else 0
    targets = context.user_data.setdefault('runtime_targets', {})
    targets['phase1'] = n
    targets['phase2'] = max(1, min(n, num_accounts))
    context.user_data['mode'] = 'custom'
    await adjust_workers(context)
    await update.message.reply_text(f"✅ تم ضبط عدد العمال: مرحلة1={targets['phase1']}، مرحلة2={targets['phase2']}")

@owner_only
async def cmd_block(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text or ''
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        await update.message.reply_text("استخدم: /block <username|regex>")
        return
    pat_text = parts[1].strip().lstrip('@')
    pat = _compile_pattern(pat_text)
    lst = context.user_data.setdefault('block_patterns', [])
    lst.append(pat)
    await update.message.reply_text(f"✅ تمت إضافة حظر للنمط: {pat_text}")

@owner_only
async def cmd_allow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text or ''
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        await update.message.reply_text("استخدم: /allow <regex>")
        return
    pat_text = parts[1].strip().lstrip('@')
    pat = _compile_pattern(pat_text)
    lst = context.user_data.setdefault('allow_patterns', [])
    lst.append(pat)
    await update.message.reply_text(f"✅ تمت إضافة سماح للنمط: {pat_text}")

@owner_only
async def cmd_addnames(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text or ''
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        await update.message.reply_text("أرسل الأسماء بعد الأمر: /addnames name1 name2 ... أو مفصولة بسطر/فاصلة.")
        return
    valid_set, invalid_count = parse_username_list(parts[1])
    # تطبيق allow/block
    filtered = {u for u in valid_set if is_username_allowed(context, u)}
    before = len(context.user_data.get('extra_usernames_set', set()))
    context.user_data.setdefault('extra_usernames_set', set()).update(filtered)
    added = len(context.user_data['extra_usernames_set']) - before
    await update.message.reply_text(f"✅ تم قبول {added} اسم بعد التصفية. ❌ غير صالح/محجوب: {invalid_count + (len(valid_set)-len(filtered))}")
    # دفع هذه الأسماء فوراً إلى طابور المرحلة الأولى لتسبق القالب
    usernames_queue: asyncio.Queue = context.user_data.get('usernames_queue')
    visited: set = context.user_data.setdefault('visited', set())
    if usernames_queue:
        for u in filtered:
            if u in visited:
                continue
            ensure_visited_capacity(visited)
            visited.add(u)
            try:
                await usernames_queue.put(u)
            except:
                pass

# ============================ واجهة البوت التفاعلية ============================
@owner_only
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    commands = [
        BotCommand("start", "إعادة تشغيل البوت"),
        BotCommand("cancel", "إلغاء العملية الحالية"),
        BotCommand("status", "حالة المهام الجارية"),
        BotCommand("cleanup", "حذف القنوات المؤقتة غير المستخدمة"),
        BotCommand("resume", "استئناف عملية الصيد")
    ]
    await context.bot.set_my_commands(commands)
    keyboard = [
        [InlineKeyboardButton("بدء عملية الصيد", callback_data="choose_session_source")],
        [InlineKeyboardButton("استئناف العملية الأخيرة", callback_data="resume_hunt")],
    ]
    # إن كان هناك مهمة محفوظة نعرض زر موجّه
    last_run = get_last_active_run()
    if last_run and last_run.get('status') in (HUNT_STATUS_RUNNING, HUNT_STATUS_PAUSED):
        summary = (
            f"آخر مهمة: فئة {last_run['category_id']}, قالب {last_run['pattern']}, هدف {last_run['target_type']}, "
            f"حالة: {last_run['status']}"
        )
        if update.callback_query:
            await update.callback_query.edit_message_text(
                "⚡️ بوت صيد يوزرات تيليجرام المتطور\n\n" + summary,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        else:
            await update.message.reply_text(
                "⚡️ بوت صيد يوزرات تيليجرام المتطور\n\n" + summary,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
    else:
        if update.callback_query:
            await update.callback_query.edit_message_text(
                 "⚡️ بوت صيد يوزرات تيليجرام المتطور",
                 reply_markup=InlineKeyboardMarkup(keyboard)
            )
        else:
            await update.message.reply_text(
                 "⚡️ بوت صيد يوزرات تيليجرام المتطور",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
    return ConversationHandler.END

# معاينة القالب وإدارة القائمة الإضافية
async def show_pattern_preview(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    pattern = context.user_data.get('pattern', '')
    extra_usernames = context.user_data.get('extra_usernames_set', set())
    samples = generate_preview_samples(pattern, max_samples=100)
    extra_preview = list(sorted(extra_usernames))[:20]

    text_lines = [
        "🧪 معاينة القالب قبل البدء:\n",
        f"• القالب: <code>{pattern}</code>",
        f"• عيّنة من الأسماء ({len(samples)}):",
        "\n".join(samples) if samples else "لا توجد عيّنة متاحة لهذا القالب.",
    ]
    if extra_usernames:
        text_lines += [
            "\n— أسماء مضافة يدوياً (عرض حتى 20):",
            "\n".join(extra_preview)
        ]
    text_lines += [
        "\nاختر إجراء:",
        "- ابدأ الصيد مباشرة",
        "- تعديل القالب",
        "- إضافة قائمة أسماء"
    ]

    keyboard = [
        [InlineKeyboardButton("✅ ابدأ الصيد", callback_data="start_hunt_confirm")],
        [InlineKeyboardButton("✏️ تعديل القالب", callback_data="edit_pattern")],
        [InlineKeyboardButton("➕ إضافة قائمة أسماء", callback_data="add_name_list")],
        [InlineKeyboardButton("🔙 رجوع", callback_data="start")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await update.callback_query.edit_message_text(
            "\n".join(text_lines),
            parse_mode="HTML",
            reply_markup=reply_markup
        )
    else:
        await update.message.reply_text(
            "\n".join(text_lines),
            parse_mode="HTML",
            reply_markup=reply_markup
        )
    return PREVIEW_PATTERN

def get_categories():
    """الحصول على الفئات المتاحة من قاعدة البيانات"""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, name FROM categories WHERE is_active = 1")
            return cursor.fetchall()
    except Exception as e:
        logger.error(f"خطأ في قراءة الفئات: {str(e)}")
        return []

@owner_only
async def choose_session_source(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        query = update.callback_query
        await query.answer()
        
        categories = get_categories()
        if not categories:
            text = "❌ لا توجد فئات متاحة. تأكد من وجود حسابات في قاعدة البيانات."
            keyboard = [[InlineKeyboardButton("🔙 رجوع", callback_data="start")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(text, reply_markup=reply_markup)
            return SELECT_CATEGORY
        
        keyboard = []
        for cat_id, name in categories:
            callback_data = f"cat_{cat_id}"
            
            try:
                with sqlite3.connect(DB_PATH) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT COUNT(*) FROM accounts WHERE category_id = ? AND is_active = 1", (cat_id,))
                    count = cursor.fetchone()[0]
                button_text = f"{name} ({count} حساب)"
            except Exception as e:
                logger.error(f"خطأ في حساب عدد الحسابات للفئة {cat_id}: {e}")
                button_text = name
                
            keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
        
        keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="start")])
        
        await query.edit_message_text(
            "📂 <b>الخطوة 2: اختيار فئة الحسابات</b>\n\n"
            "اختر الفئة التي تريد استخدامها للصيد",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        return SELECT_CATEGORY
        
    except Exception as e:
        logger.error(f"خطأ في choose_session_source: {e}", exc_info=True)
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="❌ حدث خطأ أثناء تحميل الفئات."
        )
        return ConversationHandler.END

@owner_only
async def select_category(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        query = update.callback_query
        await query.answer()
        
        # استخراج معرف الفئة
        data = query.data
        category_id = data.split('_')[1]
        context.user_data['category_id'] = category_id
        
        # الحصول على اسم الفئة
        category_name = "الفئة المحددة"
        for cat_id, name in get_categories():
            if str(cat_id) == category_id:
                category_name = name
                break
        
        await query.edit_message_text(
            f"✏️ <b>الخطوة 3: إدخال القالب للفئة [{category_name}]</b>\n\n"
            "أرسل قالب اليوزر المراد صيده.\n"
            "مثال: ١١١٢٤\n\n"
            "📝 الرموز المتاحة:\n"
            "• ١: حرف إنجليزي كبير (موحد)\n"
            "• ٢: حرف إنجليزي صغير (كامل)\n"
            "• ٣: رقم (موحد)\n"
            "• ٤: رقم (كامل)\n"
            "• _: شرطة سفلية",
            parse_mode="HTML"
        )
        
        return ENTER_PATTERN
        
    except Exception as e:
        logger.error(f"خطأ في select_category: {e}", exc_info=True)
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="❌ حدث خطأ أثناء اختيار الفئة."
        )
        return ConversationHandler.END

@owner_only
async def enter_pattern(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """استقبال القالب ثم عرض معاينة قبل البدء"""
    try:
        pattern = update.message.text
        context.user_data['pattern'] = pattern
        # تهيئة قائمة الأسماء المضافة وvisited إن لم تكن موجودة
        context.user_data.setdefault('extra_usernames_set', set())
        context.user_data.setdefault('visited', set())
        # عرض المعاينة
        return await show_pattern_preview(update, context)
    except Exception as e:
        logger.error(f"خطأ في enter_pattern: {e}")
        await update.message.reply_text("❌ حدث خطأ أثناء معالجة القالب.")
        return ConversationHandler.END

@owner_only
async def request_edit_pattern(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """إعادة طلب القالب من المستخدم"""
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("✏️ أرسل القالب الجديد الآن:")
    return ENTER_PATTERN

@owner_only
async def prompt_name_list(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """طلب إرسال قائمة أسماء من المستخدم"""
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(
        "📝 أرسل قائمة الأسماء (سطر لكل اسم أو مفصولة بفواصل).\n"
        "سيتم التحقق محلياً ودمج الصالح مع القالب."
    )
    return ENTER_NAME_LIST

@owner_only
async def receive_name_list(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """استقبال قائمة الأسماء النصية ودمجها"""
    text = update.message.text or ''
    valid_set, invalid_count = parse_username_list(text)
    extra_set = context.user_data.get('extra_usernames_set', set())
    before = len(extra_set)
    extra_set |= valid_set
    context.user_data['extra_usernames_set'] = extra_set
    added = len(extra_set) - before
    await update.message.reply_text(
        f"✅ تم قبول {added} اسم صالح. ❌ غير صالح: {invalid_count}.\n"
        f"الإجمالي الآن: {len(extra_set)}.\n\nسيتم عرض معاينة محدثة..."
    )
    # عرض المعاينة مجدداً
    return await show_pattern_preview(update, context)

# إضافة دالة مساعدة لتحديث رسالة التقدم
async def update_progress(context, message):
    """تحديث رسالة التقدم"""
    try:
        await context.bot.edit_message_text(
            chat_id=context.user_data['chat_id'],
            message_id=context.user_data['progress_message_id'],
            text=message,
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"خطأ في تحديث التقدم: {e}")

@owner_only
async def confirm_start_hunt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """تأكيد البدء بعد المعاينة وبدء عملية الصيد"""
    query = update.callback_query
    await query.answer()
    # إرسال رسالة التقدم وبدء الصيد
    msg = await context.bot.send_message(
        chat_id=query.message.chat_id,
        text=f"⏳ جاري بدء عملية الصيد للقالب: <code>{context.user_data.get('pattern','')}</code>...",
        parse_mode="HTML"
    )
    context.user_data['progress_message_id'] = msg.message_id
    context.user_data['chat_id'] = query.message.chat_id
    # بدء عملية الصيد في الخلفية
    asyncio.create_task(start_hunting(update, context))
    return HUNTING_IN_PROGRESS

async def resume_hunt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # حاول الاستئناف من قاعدة البيانات إن توفر
    run = get_last_active_run()
    if run:
        context.user_data.update({
            'category_id': run['category_id'],
            'pattern': run['pattern'],
            'target_type': run['target_type'],
            'extra_usernames_set': set(run.get('extra_usernames', [])),
            'run_id': run['id']
        })
        # إعداد رسالة التقدم إن لم تكن
        msg = await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="⏳ جاري استئناف عملية الصيد من آخر نقطة محفوظة..."
        )
        context.user_data['progress_message_id'] = msg.message_id
        context.user_data['chat_id'] = query.message.chat_id
        asyncio.create_task(start_hunting(update, context, resume=True))
        return HUNTING_IN_PROGRESS
    # fallback للسياق القديم
    if 'hunt_data' not in context.user_data:
        await query.edit_message_text("❌ لا توجد بيانات عملية سابقة.")
        return
    hunt_data = context.user_data['hunt_data']
    category_id = hunt_data['category_id']
    pattern = hunt_data['pattern']
    progress_message_id = hunt_data['progress_message_id']
    chat_id = hunt_data['chat_id']
    extra_usernames = hunt_data.get('extra_usernames', [])
    
    # تخزين البيانات في context.user_data للعملية الجارية
    context.user_data.update({
        'category_id': category_id,
        'pattern': pattern,
        'progress_message_id': progress_message_id,
        'chat_id': chat_id,
        'extra_usernames_set': set(extra_usernames)
    })
    
    # تحديث رسالة التقدم
    await update_progress(context, "⏳ جاري استئناف عملية الصيد...")
    
    # بدء عملية الصيد
    asyncio.create_task(start_hunting(update, context, resume=True))
    
    return HUNTING_IN_PROGRESS

async def start_hunting(update: Update, context: ContextTypes.DEFAULT_TYPE, resume=False):
    bot_clients = context.bot_data.get('bot_clients', [])
    session_manager = None
    stop_event = asyncio.Event()
    pause_event = asyncio.Event()
    tasks = []
    context.user_data['stop_event'] = stop_event
    context.user_data['pause_event'] = pause_event
    try:
        init_hunt_tables()
        category_id = context.user_data['category_id']
        pattern = context.user_data['pattern']
        target_type = context.user_data.get('target_type', 'channel')
        # إنشاء/تحديد run_id
        run_id = context.user_data.get('run_id')
        if not run_id:
            run_id = create_hunt_run(category_id, pattern, target_type, list(context.user_data.get('extra_usernames_set', set())))
            context.user_data['run_id'] = run_id
        # تحميل العملاء ...
        if not bot_clients:
            bot_clients = []
            for session_string in BOT_SESSIONS:
                try:
                    bot_client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
                    await bot_client.start(bot_token=BOT_TOKEN)
                    bot_clients.append(bot_client)
                    logger.info(f"تم تحميل بوت فحص: {session_string[:10]}...")
                except Exception as e:
                    logger.error(f"فشل تحميل بوت الفحص: {e}")
            context.bot_data['bot_clients'] = bot_clients
        await update_progress(context, 
            f"🔍 <b>جاري بدء عملية الصيد</b>\n\n"
            f"📂 الفئة: {category_id}\n"
            f"🔄 القالب: {pattern}\n"
            f"⏳ جاري تحميل الحسابات..."
        )
        session_manager = SessionManager(category_id)
        await session_manager.load_sessions()
        num_accounts = len(session_manager.sessions)
        if num_accounts == 0:
            await update_progress(context, "❌ لا توجد حسابات متاحة في هذه الفئة!")
            return
        context.user_data['session_manager'] = session_manager
        if 'runtime' not in context.user_data:
            apply_speed_mode(context, context.user_data.get('mode', 'normal'), num_accounts)
        context.user_data.setdefault('block_patterns', [])
        context.user_data.setdefault('allow_patterns', [])
        await update_progress(context, 
            f"🚀 <b>بدأت عملية الصيد!</b>\n\n"
            f"📂 الفئة: {category_id}\n"
            f"🔄 القالب: {pattern}\n"
            f"👥 عدد الحسابات: {num_accounts}\n"
            f"🤖 عدد بوتات الفحص: {len(bot_clients)}\n"
            f"⏳ جاري توليد اليوزرات وبدء الفحص..."
        )
        generator = UsernameGenerator(pattern)
        total_count = 0
        usernames_queue = asyncio.Queue(maxsize=20000)
        checker = UsernameChecker(bot_clients, session_manager)
        context.user_data['checker'] = checker
        async def progress_callback(message):
            await update_progress(context, 
                f"🚀 <b>جاري عملية الصيد</b>\n\n"
                f"📂 الفئة: {category_id}\n"
                f"🔄 القالب: {pattern}\n"
                f"👥 الحسابات النشطة: {len(context.user_data.get('phase2_tasks', []))}\n"
                f"🤖 بوتات الفحص النشطة: {len(context.user_data.get('phase1_tasks', []))}\n"
                f"✅ المحجوزة: {len(checker.reserved_usernames)}\n"
                f"🔄 قيد الفحص (مرحلة 1): {usernames_queue.qsize()} | (مرحلة 2): {checker.available_usernames_queue.qsize()}\n"
                f"🎯 المحجوزة بنجاح: {len(checker.claimed_usernames)}\n"
                f"💎 يوزرات Fragment: {len(checker.fragment_usernames)}\n\n"
                f"📊 {message}"
            )
        context.user_data['progress_callback'] = progress_callback
        # استئناف الطوابير والعناصر إن لزم
        if resume and run_id:
            pending, available, visited = load_items_for_resume(run_id)
            # ضخ المتوفر مباشرة في طابور المرحلة 2
            for item in available:
                await checker.available_usernames_queue.put(item)
            # حقن pending في مرحلة 1
            for _, _, u in pending:
                await usernames_queue.put(u)
            context.user_data['visited'] = visited
        # ضبط العمال وفق targets الحالية
        phase2_tasks = []
        phase1_tasks = []
        targets = context.user_data.get('runtime_targets', {})
        target_p2 = int(targets.get('phase2', min(num_accounts, MAX_CONCURRENT_TASKS)))
        for i in range(target_p2):
            task = asyncio.create_task(
                worker_account_claim(
                    checker.available_usernames_queue, 
                    checker, 
                    session_manager, 
                    stop_event,
                    pause_event,
                    context,
                    progress_callback
                )
            )
            phase2_tasks.append(task)
        target_p1 = int(targets.get('phase1', MAX_CONCURRENT_TASKS))
        for i in range(target_p1):
            task = asyncio.create_task(
                worker_bot_check(usernames_queue, checker, stop_event, pause_event, context)
            )
            phase1_tasks.append(task)
        tasks.extend(phase1_tasks + phase2_tasks)
        context.user_data['phase1_tasks'] = phase1_tasks
        context.user_data['phase2_tasks'] = phase2_tasks
        context.user_data['usernames_queue'] = usernames_queue
        # منتِج مع حفظ دوري للعناصر المتولدة
        async def generate_usernames():
            nonlocal total_count
            visited: set = context.user_data.setdefault('visited', set())
            extra_usernames: set = context.user_data.get('extra_usernames_set', set())
            count = 0
            BATCH_SIZE = 1000
            batch = []
            to_persist = []
            last_drain = time.time()
            last_total_put = 0
            try:
                for extra in list(extra_usernames):
                    if stop_event.is_set():
                        break
                    if pause_event.is_set():
                        await asyncio.sleep(1)
                        continue
                    u = normalize_username_input(extra)
                    if not is_valid_username_local(u):
                        continue
                    if u in visited:
                        continue
                    if not is_username_allowed(context, u):
                        continue
                    ensure_visited_capacity(visited)
                    visited.add(u)
                    await usernames_queue.put(u)
                    to_persist.append(u)
                    count += 1
                    total_count = count
                    if len(to_persist) >= 500:
                        add_hunt_items_batch(run_id, to_persist)
                        to_persist = []
                for username in generator.generate_usernames():
                    if stop_event.is_set():
                        break
                    if pause_event.is_set():
                        await asyncio.sleep(1)
                        continue
                    u = normalize_username_input(username)
                    if not is_valid_username_local(u):
                        continue
                    if u in visited:
                        continue
                    if not is_username_allowed(context, u):
                        continue
                    batch.append(u)
                    if len(batch) >= BATCH_SIZE:
                        for u2 in batch:
                            ensure_visited_capacity(visited)
                            visited.add(u2)
                            await usernames_queue.put(u2)
                        to_persist.extend(batch)
                        count += len(batch)
                        total_count = count
                        if len(to_persist) >= 500:
                            add_hunt_items_batch(run_id, to_persist)
                            to_persist = []
                        # backpressure كما سابقاً
                        q1 = usernames_queue.qsize()
                        q2 = checker.available_usernames_queue.qsize()
                        now = time.time()
                        drain_rate = max(1.0, (count - last_total_put) / max(0.1, now - last_drain))
                        if q1 > 15000 or q2 > 3000:
                            BATCH_SIZE = max(200, int(BATCH_SIZE * 0.7))
                        elif q1 < 2000 and q2 < 500 and drain_rate > 500:
                            BATCH_SIZE = min(5000, int(BATCH_SIZE * 1.2))
                        last_drain = now
                        last_total_put = count
                        batch = []
                        if count % 10000 == 0:
                            await progress_callback(f"تم توليد {count} يوزر حتى الآن")
                if batch:
                    for u2 in batch:
                        ensure_visited_capacity(visited)
                        visited.add(u2)
                        await usernames_queue.put(u2)
                    to_persist.extend(batch)
                    count += len(batch)
                    total_count = count
                if to_persist:
                    add_hunt_items_batch(run_id, to_persist)
                for _ in range(len(phase1_tasks)):
                    if not stop_event.is_set():
                        await usernames_queue.put(None)
                set_gen_done(run_id)
            except asyncio.CancelledError:
                if batch:
                    for u2 in batch:
                        ensure_visited_capacity(visited)
                        visited.add(u2)
                        await usernames_queue.put(u2)
                    count += len(batch)
                if to_persist:
                    add_hunt_items_batch(run_id, to_persist)
                raise
            return count
        gen_task = asyncio.create_task(generate_usernames())
        tasks.append(gen_task)
        await gen_task
        await usernames_queue.join()
        await progress_callback(f"✅ اكتملت المرحلة الأولى: {len(checker.reserved_usernames)} محجوزة")
        for _ in range(len(phase2_tasks)):
            if not stop_event.is_set():
                await checker.available_usernames_queue.put(None)
        # النتائج النهائية
        mark_run_finished(run_id, HUNT_STATUS_FINISHED)
        result_message = (
            f"🎉 <b>اكتملت عملية الصيد بنجاح!</b>\n\n"
            f"📂 الفئة: {category_id}\n"
            f"🔄 القالب: {pattern}\n"
            f"👥 عدد الحسابات: {num_accounts}\n"
            f"🤖 عدد بوتات الفحص: {len(bot_clients)}\n"
            f"🔢 العدد الكلي: {total_count}\n"
            f"🔒 اليوزرات المحجوزة: {len(checker.reserved_usernames)}\n"
            f"🎯 اليوزرات المحجوزة بنجاح: {len(checker.claimed_usernames)}\n"
            f"💎 يوزرات Fragment: {len(checker.fragment_usernames)}\n"
            f"💾 تم حفظ النتائج في: {CLAIMED_FILE}\n"
            f"💎 تم حفظ يوزرات Fragment في: {FRAGMENT_FILE}\n\n"
            f"⚠️ ملاحظة: القنوات المؤقتة لم تحذف. استخدم الأمر /cleanup لحذف القنوات غير المستخدمة"
        )
        with open(FRAGMENT_FILE, 'w', encoding='utf-8') as f:
            for username in checker.fragment_usernames:
                f.write(f"{username}\n")
        await update_progress(context, result_message)
        # إرسال تقارير CSV/JSON تلقائياً للمالكين
        await send_final_reports(context, run_id)
        await session_manager.cleanup_unused_channels()
    except asyncio.CancelledError:
        logger.info("تم إلغاء عملية الصيد")
        # تحديث حالة المهمة للاستئناف
        run_id = context.user_data.get('run_id')
        if run_id:
            mark_run_finished(run_id, HUNT_STATUS_PAUSED)
        await update_progress(context, "⏸ تم إيقاف العملية مؤقتاً. يمكنك استئنافها من القائمة الرئيسية.")
    except Exception as e:
        logger.error(f"خطأ جسيم في عملية الصيد: {e}", exc_info=True)
        await update_progress(context, f"❌ فشلت عملية الصيد بسبب خطأ: {str(e)}")
    finally:
        stop_event.set()
        pause_event.set()
        for task in tasks:
            if not task.done():
                task.cancel()
        try:
            # انتظار إنهاء المهام مع معالجة الاستثناءات
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"خطأ أثناء انتظار إنهاء المهام: {e}")

        # إغلاق جلسات البوتات بشكل صحيح
        for bot_client in bot_clients:
            try:
                if isinstance(bot_client, TelegramClient) and bot_client.is_connected():
                    await bot_client.disconnect()
            except Exception as e:
                logger.error(f"خطأ في إغلاق بوت الفحص: {e}")

        # إغلاق جلسات الحسابات بشكل صحيح
        if session_manager:
            for account_id, session_data in session_manager.sessions.items():
                try:
                    client = session_data.get('client')
                    if isinstance(client, TelegramClient) and client.is_connected():
                        await client.disconnect()
                except Exception as e:
                    logger.error(f"خطأ في إغلاق حساب: {account_id} - {e}")
            
            # تنظيف القنوات
            try:
                await session_manager.cleanup_unused_channels()
            except Exception as e:
                logger.error(f"خطأ في تنظيف القنوات: {e}")

        # تنظيف
        context.user_data.pop('stop_event', None)
        context.user_data.pop('pause_event', None)
        logger.info("تم تنظيف جميع الموارد.")

@owner_only
async def cleanup_channels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """حذف القنوات المؤقتة غير المستخدمة"""
    session_manager = context.user_data.get('session_manager')
    
    if not session_manager:
        await update.message.reply_text("❌ لا يوجد مدير جلسات نشط. ابدأ عملية صيد أولاً.")
        return
    
    try:
        msg = await update.message.reply_text("⏳ جاري حذف القنوات المؤقتة غير المستخدمة...")
        await session_manager.cleanup_unused_channels()
        await msg.edit_text("✅ تم حذف جميع القنوات المؤقتة غير المستخدمة بنجاح!")
    except Exception as e:
        logger.error(f"خطأ في حذف القنوات: {e}")
        await update.message.reply_text(f"❌ حدث خطأ أثناء حذف القنوات: {str(e)}")

@owner_only
async def pause_hunt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """إيقاف عملية الصيد مؤقتاً"""
    if 'pause_event' in context.user_data:
        pause_event = context.user_data['pause_event']
        if not pause_event.is_set():
            pause_event.set()
            await update.message.reply_text("⏸ تم إيقاف العملية مؤقتاً. استخدم /resume لاستئناف.")
        else:
            await update.message.reply_text("⏸ العملية متوقفة بالفعل.")
    else:
        await update.message.reply_text("❌ لا توجد عملية نشطة لإيقافها.")

@owner_only
async def resume_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """استئناف عملية الصيد"""
    if 'pause_event' in context.user_data:
        pause_event = context.user_data['pause_event']
        if pause_event.is_set():
            pause_event.clear()
            await update.message.reply_text("▶️ تم استئناف العملية.")
        else:
            await update.message.reply_text("▶️ العملية تعمل بالفعل.")
    else:
        await update.message.reply_text("❌ لا توجد عملية متوقفة للاستئناف.")

@owner_only
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """إلغاء العملية الحالية"""
    user_data = context.user_data
    stop_event = user_data.get('stop_event')
    if stop_event and not stop_event.is_set():
        stop_event.set()
    
    await update.message.reply_text("✅ تم إلغاء العملية الحالية.")
    
    # تنظيف البيانات المؤقتة
    user_data.clear()
    return ConversationHandler.END

@owner_only
async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    run = get_last_active_run()
    checker: 'UsernameChecker' = context.user_data.get('checker')
    usernames_queue: asyncio.Queue = context.user_data.get('usernames_queue')
    p1 = usernames_queue.qsize() if usernames_queue else 0
    p2 = checker.available_usernames_queue.qsize() if checker else 0
    claimed = len(checker.claimed_usernames) if checker else 0
    reserved = len(checker.reserved_usernames) if checker else 0
    fragment = len(checker.fragment_usernames) if checker else 0
    phase1_workers = len(context.user_data.get('phase1_tasks', []))
    phase2_workers = len(context.user_data.get('phase2_tasks', []))
    runtime = context.user_data.get('runtime', {})
    mode = context.user_data.get('mode', 'normal')
    msg = (
        f"📊 الحالة الحالية:\n"
        f"• وضع السرعة: {mode} ({runtime.get('min_wait','?')}-{runtime.get('max_wait','?')}s)\n"
        f"• عمال المرحلة1/2: {phase1_workers}/{phase2_workers}\n"
        f"• طوابير المرحلة1/2: {p1}/{p2}\n"
        f"• محجوزة (فحص): {reserved} | Fragment: {fragment} | محجوزة بنجاح: {claimed}\n"
    )
    await update.message.reply_text(msg)

@owner_only
async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    run = get_last_active_run()
    if not run:
        await update.message.reply_text("❌ لا توجد مهمة لتوليد تقرير لها.")
        return
    await update.message.reply_text("⏳ جاري تجهيز التقرير...")
    await send_final_reports(context, run['id'])
    # زر أفضل 100
    keyboard = [[InlineKeyboardButton("🏆 أفضل 100 اسم", callback_data="show_top100")]]
    await update.message.reply_text("اختر لعرض أفضل 100 اسم محجوز:", reply_markup=InlineKeyboardMarkup(keyboard))

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """معالجة الأخطاء العامة"""
    logger.error(f"حدث خطأ: {context.error}", exc_info=True)
    if update and update.effective_message:
        await update.effective_message.reply_text("❌ حدث خطأ غير متوقع أثناء المعالجة.")

# ============== أدوات مساعدة للمرحلة 3: التحكم أثناء التشغيل ==============

def _compile_pattern(p: str):
    try:
        return re.compile(p, re.IGNORECASE)
    except re.error:
        # اعتبرها نصاً حرفياً إذا فشل regex
        escaped = re.escape(p)
        return re.compile(f"^{escaped}$", re.IGNORECASE)


def is_username_allowed(context: ContextTypes.DEFAULT_TYPE, username: str) -> bool:
    name = username.lstrip('@').lower()
    block_patterns = context.user_data.get('block_patterns', [])
    allow_patterns = context.user_data.get('allow_patterns', [])
    for pat in block_patterns:
        if pat.search(name):
            return False
    if allow_patterns:
        for pat in allow_patterns:
            if pat.search(name):
                return True
        return False
    return True

async def adjust_workers(context: ContextTypes.DEFAULT_TYPE) -> None:
    """تعديل عدد العمال للمرحلتيْن وفق الإعدادات الحالية دون إيقاف المهمة."""
    async with context.user_data.setdefault('control_lock', asyncio.Lock()):
        phase1_tasks: list = context.user_data.get('phase1_tasks', [])
        phase2_tasks: list = context.user_data.get('phase2_tasks', [])
        stop_event: asyncio.Event = context.user_data.get('stop_event')
        pause_event: asyncio.Event = context.user_data.get('pause_event')
        checker: 'UsernameChecker' = context.user_data.get('checker')
        usernames_queue: asyncio.Queue = context.user_data.get('usernames_queue')
        session_manager: 'SessionManager' = context.user_data.get('session_manager')
        bot_clients = context.bot_data.get('bot_clients', [])

        if not all([phase1_tasks is not None, phase2_tasks is not None, stop_event, pause_event, checker, usernames_queue, session_manager]):
            return

        targets = context.user_data.get('runtime_targets', {})
        target_p1 = int(targets.get('phase1', len(phase1_tasks)))
        target_p2 = int(targets.get('phase2', len(phase2_tasks)))

        # ضبط المرحلة 1 (الفحص عبر البوتات)
        diff_p1 = target_p1 - len(phase1_tasks)
        if diff_p1 > 0:
            for _ in range(diff_p1):
                task = asyncio.create_task(
                    worker_bot_check(usernames_queue, checker, stop_event, pause_event, context)
                )
                phase1_tasks.append(task)
        elif diff_p1 < 0:
            # إلغاء بعض العمال
            for _ in range(-diff_p1):
                if phase1_tasks:
                    t = phase1_tasks.pop()
                    try:
                        t.cancel()
                    except:
                        pass

        # ضبط المرحلة 2 (التثبيت بالحسابات)
        diff_p2 = target_p2 - len(phase2_tasks)
        if diff_p2 > 0:
            for _ in range(diff_p2):
                task = asyncio.create_task(
                    worker_account_claim(
                        checker.available_usernames_queue,
                        checker,
                        session_manager,
                        stop_event,
                        pause_event,
                        context,
                        context.user_data.get('progress_callback')
                    )
                )
                phase2_tasks.append(task)
        elif diff_p2 < 0:
            for _ in range(-diff_p2):
                if phase2_tasks:
                    t = phase2_tasks.pop()
                    try:
                        t.cancel()
                    except:
                        pass

        context.user_data['phase1_tasks'] = phase1_tasks
        context.user_data['phase2_tasks'] = phase2_tasks


def apply_speed_mode(context: ContextTypes.DEFAULT_TYPE, mode: str, num_accounts: int) -> str:
    """تعيين أوضاع السرعة وإرجاع رسالة تأكيد."""
    mode = mode.lower()
    runtime = context.user_data.setdefault('runtime', {})
    targets = context.user_data.setdefault('runtime_targets', {})

    if mode == 'safe':
        runtime['min_wait'] = 1.2
        runtime['max_wait'] = 3.0
        targets['phase1'] = 2
        targets['phase2'] = max(1, min(2, num_accounts))
    elif mode == 'fast':
        runtime['min_wait'] = 0.2
        runtime['max_wait'] = 1.0
        targets['phase1'] = 8
        targets['phase2'] = max(1, min(8, num_accounts))
    else:  # normal
        runtime['min_wait'] = 0.5
        runtime['max_wait'] = 3.0
        targets['phase1'] = 5
        targets['phase2'] = max(1, min(5, num_accounts))

    context.user_data['mode'] = mode
    return f"تم ضبط الوضع إلى {mode}. عمال المرحلة1={targets['phase1']}, المرحلة2={targets['phase2']}, الانتظار={runtime['min_wait']}-{runtime['max_wait']} ثانية."

# اختيار هدف التثبيت قبل البدء
TARGET_TYPES = {
    'channel': 'قناة مؤقتة',
    'group': 'مجموعة مؤقتة',
    'self': 'الحساب نفسه'
}

@owner_only
async def choose_target_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    keyboard = [
        [InlineKeyboardButton("📢 قناة مؤقتة", callback_data="target_channel")],
        [InlineKeyboardButton("👥 مجموعة مؤقتة", callback_data="target_group")],
        [InlineKeyboardButton("👤 الحساب نفسه", callback_data="target_self")],
        [InlineKeyboardButton("🔙 رجوع", callback_data="start")]
    ]
    await query.edit_message_text(
        "🎯 اختر هدف التثبيت الافتراضي:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return SELECT_CATEGORY

# تعديل البداية لعرض اختيار الهدف أولاً
@owner_only
async def choose_session_source(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        query = update.callback_query
        await query.answer()
        # تخزين هدف افتراضي إن لم يوجد
        context.user_data.setdefault('target_type', 'channel')
        # عرض اختيار الهدف قبل اختيار الفئة
        return await choose_target_type(update, context)
    except Exception as e:
        logger.error(f"خطأ في choose_session_source: {e}", exc_info=True)
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="❌ حدث خطأ أثناء التهيئة."
        )
        return ConversationHandler.END

@owner_only
async def target_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    data = query.data
    if data == 'target_channel':
        context.user_data['target_type'] = 'channel'
    elif data == 'target_group':
        context.user_data['target_type'] = 'group'
    elif data == 'target_self':
        context.user_data['target_type'] = 'self'
    # بعد اختيار الهدف نعرض الفئات كما كان
    categories = get_categories()
    if not categories:
        text = "❌ لا توجد فئات متاحة. تأكد من وجود حسابات في قاعدة البيانات."
        keyboard = [[InlineKeyboardButton("🔙 رجوع", callback_data="start")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(text, reply_markup=reply_markup)
        return SELECT_CATEGORY
    keyboard = []
    for cat_id, name in categories:
        try:
            with sqlite3.connect(DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM accounts WHERE category_id = ? AND is_active = 1", (cat_id,))
                count = cursor.fetchone()[0]
            button_text = f"{name} ({count} حساب)"
        except Exception as e:
            logger.error(f"خطأ في حساب عدد الحسابات للفئة {cat_id}: {e}")
            button_text = name
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"cat_{cat_id}")])
    keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="start")])
    await query.edit_message_text(
        "📂 <b>الخطوة 2: اختيار فئة الحسابات</b>\n\nاختر الفئة التي تريد استخدامها للصيد",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return SELECT_CATEGORY

# ============================ تخزين واستئناف دائم (المرحلة 5) ============================
HUNT_STATUS_RUNNING = 'running'
HUNT_STATUS_PAUSED = 'paused'
HUNT_STATUS_FINISHED = 'finished'

ITEM_STATUS_PENDING = 'pending'
ITEM_STATUS_AVAILABLE = 'available'  # متاح ومرسل للمرحلة 2
ITEM_STATUS_RESERVED = 'reserved'    # محجوز في الفحص
ITEM_STATUS_FRAGMENT = 'fragment'
ITEM_STATUS_CLAIMED = 'claimed'
ITEM_STATUS_FAILED = 'failed'

def init_hunt_tables():
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
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
            cur.execute(
                '''CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )'''
            )
            # أعمدة إضافية اختيارية
            try:
                cur.execute('ALTER TABLE hunt_items ADD COLUMN account_id TEXT')
            except sqlite3.OperationalError:
                pass
            try:
                cur.execute('ALTER TABLE hunt_items ADD COLUMN failure_reason TEXT')
            except sqlite3.OperationalError:
                pass
            try:
                cur.execute('ALTER TABLE hunt_items ADD COLUMN claimed_at TEXT')
            except sqlite3.OperationalError:
                pass
            try:
                cur.execute('ALTER TABLE hunt_items ADD COLUMN checked_at TEXT')
            except sqlite3.OperationalError:
                pass
            conn.commit()
    except Exception as e:
        logger.error(f"init_hunt_tables error: {e}")


def create_hunt_run(category_id: str, pattern: str, target_type: str, extra_usernames: list[str]) -> int:
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute(
                'INSERT INTO hunt_runs(started_at,status,category_id,pattern,target_type,extra_usernames_json) VALUES (?,?,?,?,?,?)',
                (datetime.utcnow().isoformat(), HUNT_STATUS_RUNNING, category_id, pattern, target_type, json.dumps(extra_usernames or []))
            )
            run_id = cur.lastrowid
            # حفظ معرف آخر مهمة نشطة
            cur.execute('INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)', ('last_run_id', str(run_id)))
            conn.commit()
            return run_id
    except Exception as e:
        logger.error(f"create_hunt_run error: {e}")
        return 0


def mark_run_finished(run_id: int, status: str):
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            if status == HUNT_STATUS_FINISHED:
                cur.execute('UPDATE hunt_runs SET status=?, finished_at=?, gen_done=1 WHERE id=?', (status, datetime.utcnow().isoformat(), run_id))
            else:
                cur.execute('UPDATE hunt_runs SET status=? WHERE id=?', (status, run_id))
            conn.commit()
    except Exception as e:
        logger.error(f"mark_run_finished error: {e}")


def set_gen_done(run_id: int):
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute('UPDATE hunt_runs SET gen_done=1 WHERE id=?', (run_id,))
            conn.commit()
    except Exception as e:
        logger.error(f"set_gen_done error: {e}")


def add_hunt_items_batch(run_id: int, usernames: list[str]):
    if not usernames:
        return
    try:
        rows = [(run_id, u.lstrip('@').lower(), ITEM_STATUS_PENDING, float(score_username(u.lstrip('@'))), None, 0) for u in usernames]
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            cur.executemany(
                'INSERT OR IGNORE INTO hunt_items(run_id,username,status,score,last_attempt_ts,attempts) VALUES (?,?,?,?,?,?)',
                rows
            )
            conn.commit()
    except Exception as e:
        logger.error(f"add_hunt_items_batch error: {e}")


def update_item_status(run_id: int, username: str, status: str, inc_attempt: bool = False):
    try:
        name = username.lstrip('@').lower()
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            checked_at = datetime.utcnow().isoformat() if status in (ITEM_STATUS_AVAILABLE, ITEM_STATUS_RESERVED, ITEM_STATUS_FRAGMENT) else None
            if inc_attempt:
                if checked_at:
                    cur.execute(
                        'UPDATE hunt_items SET status=?, last_attempt_ts=?, attempts=attempts+1, checked_at=COALESCE(checked_at,?) WHERE run_id=? AND username=?',
                        (status, datetime.utcnow().isoformat(), checked_at, run_id, name)
                    )
                else:
                    cur.execute(
                        'UPDATE hunt_items SET status=?, last_attempt_ts=?, attempts=attempts+1 WHERE run_id=? AND username=?',
                        (status, datetime.utcnow().isoformat(), run_id, name)
                    )
            else:
                if checked_at:
                    cur.execute(
                        'UPDATE hunt_items SET status=?, last_attempt_ts=?, checked_at=COALESCE(checked_at,?) WHERE run_id=? AND username=?',
                        (status, datetime.utcnow().isoformat(), checked_at, run_id, name)
                    )
                else:
                    cur.execute(
                        'UPDATE hunt_items SET status=?, last_attempt_ts=? WHERE run_id=? AND username=?',
                        (status, datetime.utcnow().isoformat(), run_id, name)
                    )
            if cur.rowcount == 0:
                cur.execute(
                    'INSERT OR IGNORE INTO hunt_items(run_id,username,status,score,last_attempt_ts,attempts,checked_at) VALUES (?,?,?,?,?,?,?)',
                    (run_id, name, status, float(score_username(name)), datetime.utcnow().isoformat(), 1 if inc_attempt else 0, checked_at)
                )
            conn.commit()
    except Exception as e:
        logger.error(f"update_item_status error: {e}")


def set_item_claimed(run_id: int, username: str, account_id: str):
    try:
        name = username.lstrip('@').lower()
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                'UPDATE hunt_items SET status=?, claimed_at=?, account_id=? WHERE run_id=? AND username=?',
                (ITEM_STATUS_CLAIMED, datetime.utcnow().isoformat(), account_id, run_id, name)
            )
            conn.commit()
    except Exception as e:
        logger.error(f"set_item_claimed error: {e}")


def set_item_failed(run_id: int, username: str, reason: str):
    try:
        name = username.lstrip('@').lower()
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                'UPDATE hunt_items SET status=?, failure_reason=? WHERE run_id=? AND username=?',
                (ITEM_STATUS_FAILED, reason[:200], run_id, name)
            )
            conn.commit()
    except Exception as e:
        logger.error(f"set_item_failed error: {e}")


def generate_report_files(run_id: int) -> tuple[str, str]:
    csv_path = f"hunt_report_{run_id}.csv"
    json_path = f"hunt_report_{run_id}.json"
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute('''SELECT username,status,score,last_attempt_ts,attempts,account_id,failure_reason,claimed_at,checked_at 
                           FROM hunt_items WHERE run_id=?''', (run_id,))
            rows = cur.fetchall()
        # JSON
        data = []
        for r in rows:
            data.append({
                'username': f"@{r[0]}",
                'status': r[1],
                'score': r[2],
                'last_attempt_ts': r[3],
                'attempts': r[4],
                'account_id': r[5],
                'failure_reason': r[6],
                'claimed_at': r[7],
                'checked_at': r[8]
            })
        with open(json_path, 'w', encoding='utf-8') as jf:
            json.dump(data, jf, ensure_ascii=False, indent=2)
        # CSV
        import csv
        with open(csv_path, 'w', encoding='utf-8', newline='') as cf:
            writer = csv.writer(cf)
            writer.writerow(['username','status','score','last_attempt_ts','attempts','account_id','failure_reason','claimed_at','checked_at'])
            for r in rows:
                writer.writerow([f"@{r[0]}", r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8]])
        return csv_path, json_path
    except Exception as e:
        logger.error(f"generate_report_files error: {e}")
        return '', ''

async def send_final_reports(context: ContextTypes.DEFAULT_TYPE, run_id: int):
    csv_path, json_path = generate_report_files(run_id)
    if not csv_path and not json_path:
        return
    for admin_id in ADMIN_IDS:
        try:
            if csv_path:
                await context.bot.send_document(chat_id=admin_id, document=open(csv_path, 'rb'), filename=os.path.basename(csv_path), caption=f"تقرير CSV للمهمة {run_id}")
            if json_path:
                await context.bot.send_document(chat_id=admin_id, document=open(json_path, 'rb'), filename=os.path.basename(json_path), caption=f"تقرير JSON للمهمة {run_id}")
        except Exception as e:
            logger.error(f"send_final_reports error: {e}")

@owner_only
async def top100_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    run = get_last_active_run()
    if not run:
        await query.edit_message_text("❌ لا توجد مهمة لعرضها.")
        return
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute('''SELECT username, score FROM hunt_items WHERE run_id=? AND status=? ORDER BY score ASC LIMIT 100''',
                        (run['id'], ITEM_STATUS_CLAIMED))
            rows = cur.fetchall()
        if not rows:
            await query.edit_message_text("لا توجد نتائج محجوزة لعرض أفضل 100.")
            return
        lines = [f"@{u} — {s}" for (u, s) in rows]
        text = "🏆 أفضل 100 (أقل score):\n\n" + "\n".join(lines)
        await query.edit_message_text(text)
    except Exception as e:
        logger.error(f"top100_handler error: {e}")
        await query.edit_message_text("❌ خطأ أثناء عرض أفضل 100.")

def get_last_active_run() -> dict | None:
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute('SELECT value FROM settings WHERE key=?', ('last_run_id',))
            row = cur.fetchone()
            if not row:
                return None
            run_id = int(row[0])
            cur.execute('SELECT id, status, category_id, pattern, target_type, extra_usernames_json, gen_done FROM hunt_runs WHERE id=?', (run_id,))
            r = cur.fetchone()
            if not r:
                return None
            return {
                'id': r[0],
                'status': r[1],
                'category_id': r[2],
                'pattern': r[3],
                'target_type': r[4],
                'extra_usernames': json.loads(r[5] or '[]'),
                'gen_done': bool(r[6])
            }
    except Exception as e:
        logger.error(f"get_last_active_run error: {e}")
        return None


def load_items_for_resume(run_id: int) -> tuple[list[tuple[float,int,str]], list[tuple[float,int,str]], set[str]]:
    """إرجاع (pending_for_phase1, available_for_phase2, visited_set). عناصر الطوابير كـ (score, seq, @username)."""
    pending = []
    available = []
    visited = set()
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute('SELECT username, status, score FROM hunt_items WHERE run_id=?', (run_id,))
            rows = cur.fetchall()
            for uname, status, score in rows:
                u = f"@{uname}"
                visited.add(u)
                if status in (ITEM_STATUS_PENDING,):
                    pending.append((float(score), next(_SEQ), u))
                elif status in (ITEM_STATUS_AVAILABLE,):
                    available.append((float(score), next(_SEQ), u))
            return pending, available, visited
    except Exception as e:
        logger.error(f"load_items_for_resume error: {e}")
        return pending, available, visited

def main() -> None:
    """تشغيل البوت"""
    application = Application.builder().token(BOT_TOKEN).build()
    
    # تعريف محادثة الصيد
    conv_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(choose_session_source, pattern="^choose_session_source$"),
            CallbackQueryHandler(resume_hunt, pattern="^resume_hunt$")
        ],
        states={
            SELECT_CATEGORY: [
                CallbackQueryHandler(target_selected, pattern=r"^target_(channel|group|self)$"),
                CallbackQueryHandler(select_category, pattern=r"^cat_.+$"),
                CallbackQueryHandler(start, pattern="^start$")
            ],
            ENTER_PATTERN: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, enter_pattern)
            ],
            PREVIEW_PATTERN: [
                CallbackQueryHandler(confirm_start_hunt, pattern="^start_hunt_confirm$"),
                CallbackQueryHandler(request_edit_pattern, pattern="^edit_pattern$"),
                CallbackQueryHandler(prompt_name_list, pattern="^add_name_list$"),
                CallbackQueryHandler(start, pattern="^start$")
            ],
            ENTER_NAME_LIST: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_name_list)
            ],
            HUNTING_IN_PROGRESS: [
                CommandHandler("pause", pause_hunt),
                CommandHandler("resume", resume_command),
                CommandHandler("cancel", cancel),
                CommandHandler("speed", cmd_speed),
                CommandHandler("workers", cmd_workers),
                CommandHandler("block", cmd_block),
                CommandHandler("allow", cmd_allow),
                CommandHandler("addnames", cmd_addnames),
            ],
            HUNTING_PAUSED: [
                CommandHandler("resume", resume_command),
                CommandHandler("cancel", cancel),
                CommandHandler("speed", cmd_speed),
                CommandHandler("workers", cmd_workers),
                CommandHandler("block", cmd_block),
                CommandHandler("allow", cmd_allow),
                CommandHandler("addnames", cmd_addnames),
            ]
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CallbackQueryHandler(start, pattern="^start$")
        ],
        map_to_parent={
            ConversationHandler.END: ConversationHandler.END
        }
    )
    
    # إضافة المعالجات
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("cancel", cancel))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(CommandHandler("cleanup", cleanup_channels))
    application.add_handler(CommandHandler("pause", pause_hunt))
    application.add_handler(CommandHandler("resume", resume_command))
    # أوامر التحكم متاحة أيضاً خارج المحادثة عند وجود عملية
    application.add_handler(CommandHandler("speed", cmd_speed))
    application.add_handler(CommandHandler("workers", cmd_workers))
    application.add_handler(CommandHandler("block", cmd_block))
    application.add_handler(CommandHandler("allow", cmd_allow))
    application.add_handler(CommandHandler("addnames", cmd_addnames))
    application.add_handler(CommandHandler("report", report_command))
    application.add_handler(conv_handler)
    application.add_error_handler(error_handler)
    
    # بدء البوت
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()