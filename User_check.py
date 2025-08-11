import os
import asyncio
import random
import string
import logging
import itertools
import sqlite3
import time
import heapq
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

# ثوابت النظام
MAX_COOLDOWN_TIME = 150  # أقصى وقت تبريد مسموح به (ساعة واحدة)
EMERGENCY_THRESHOLD = 150  # 5 دقائق للتحول لحالة الطوارئ
MIN_WAIT_TIME = 0.5  # الحد الأدنى للانتظار بين الطلبات
MAX_WAIT_TIME = 3.0  # الحد الأقصى للانتظار بين الطلبات
ACCOUNT_CHECK_RATIO = 0.3  # نسبة استخدام الحسابات في حالة الطوارئ

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
logging.getLogger('telethon').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)  # تقليل تسجيل طلبات HTTP

# فئات القوالب
TEMPLATE_TYPES = {
    '١': ('char', 'fixed', string.ascii_uppercase),    # حرف موحد (كبير)
    '٢': ('char', 'full', string.ascii_lowercase),     # حرف عشوائي (صغير) - كل الاحتمالات
    '٣': ('digit', 'fixed', string.digits),            # رقم موحد
    '٤': ('digit', 'full', string.digits),             # رقم عشوائي - كل الاحتمالات
    '_': ('literal', '_', ['_']),                      # حرف ثابت
    'bot': ('literal', 'bot', ['bot'])                      # حرف ثابت
}

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
        self.sessions = {}  # {account_id: {'client': TelegramClient, 'channel': InputChannel}}
        self.accounts_queue = asyncio.PriorityQueue()  # (priority, account_id)
        self.category_id = category_id
        self.created_channels = []  # لتخزين القنوات التي تم إنشاؤها
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
                    # فك تشفير الجلسة
                    session_str = decrypt_session(encrypted_session)
                    
                    # إنشاء عميل تيليثون
                    client = TelegramClient(StringSession(session_str), API_ID, API_HASH)
                    await client.connect()
                    
                    if not client.is_connected():
                        await client.start()
                    
                    # التحقق من أن الحساب غير محظور
                    try:
                        me = await client.get_me()
                        if me.bot:
                            logger.error(f"الحساب بوت: {phone} - لا يمكن استخدام البوتات في هذه العملية")
                            continue
                            
                        # محاولة بسيطة للتأكد من عدم حظر الحساب
                        await client.get_dialogs(limit=1)
                    except (UserDeactivatedError, UserDeactivatedBanError, ChannelInvalidError) as e:
                        logger.error(f"الحساب محظور أو معطل: {phone} - {e}")
                        continue
                    except Exception as e:
                        logger.error(f"خطأ في التحقق من الحساب {phone}: {e}")
                        continue
                    
                    # إنشاء قناة احتياطية
                    try:
                        channel_name = f"Reserve Channel {random.randint(10000, 99999)}"
                        channel = await client(CreateChannelRequest(
                            title=channel_name,
                            about="قناة مؤقتة لتثبيت اليوزرات",
                            megagroup=False
                        ))
                        chat = channel.chats[0]
                        if not isinstance(chat, Channel):
                            logger.error(f"فشل إنشاء القناة للحساب {phone}: النوع غير صحيح")
                            continue
                            
                        # تخزين كائن InputChannel كاملاً
                        input_channel = InputChannel(chat.id, chat.access_hash)
                        logger.info(f"تم إنشاء القناة الاحتياطية: {chat.id} للحساب {phone}")
                        self.created_channels.append((client, input_channel, account_id))
                    except Exception as e:
                        logger.error(f"خطأ في إنشاء القناة للحساب {phone}: {e}")
                        continue
                    
                    # تخزين الجلسة
                    self.sessions[account_id] = {
                        'client': client,
                        'input_channel': input_channel,
                        'phone': phone,
                        'account_id': account_id
                    }
                    # إضافة الحساب إلى الطابور بالأولوية (0 أولوية عالية)
                    self.account_priority[account_id] = 0
                    await self.accounts_queue.put((0, account_id))
                    logger.info(f"تم تحميل الجلسة: {phone}")
                except SessionPasswordNeededError:
                    logger.error(f"الجلسة {phone} محمية بكلمة مرور ثنائية. تخطي.")
                except Exception as e:
                    logger.error(f"خطأ في تحميل الجلسة {phone}: {str(e)}")
        except Exception as e:
            logger.error(f"خطأ في قراءة قاعدة البيانات: {str(e)}")
    
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
        for client, input_channel, account_id in self.created_channels:
            # إذا كان الحساب لا يزال في القائمة ولم يتم حظره، والقناة لم تستخدم للتثبيت، نحذفها
            if account_id in self.sessions and account_id not in self.banned_accounts:
                try:
                    await client(DeleteChannelRequest(channel=input_channel))
                    logger.info(f"تم حذف القناة الاحتياطية غير المستخدمة: {input_channel.channel_id}")
                except Exception as e:
                    logger.error(f"خطأ في حذف القناة {input_channel.channel_id}: {e}")

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
        self.available_usernames_queue = asyncio.Queue()
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
                logger.info(f"اليوزر محجوز: {username}")
                return "reserved"
            except (UsernameInvalidError, ValueError):
                await self.available_usernames_queue.put(username)
                logger.info(f"تم تمرير اليوزر للمرحلة الثانية: {username}")
                return "available"
            except UsernamePurchaseAvailableError:
                # اليوزر معروض للبيع على Fragment
                async with self.lock:
                    self.reserved_usernames.append(username)
                    self.fragment_usernames.append(username)
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
async def worker_bot_check(queue, checker, stop_event, pause_event):
    """عامل لفحص اليوزرات (المرحلة الأولى)"""
    while not stop_event.is_set():
        try:
            # التحقق من حالة الإيقاف المؤقت
            if pause_event.is_set():
                await asyncio.sleep(1)
                continue
                
            username = await asyncio.wait_for(queue.get(), timeout=1.0)
            if username is None:
                queue.task_done()
                break
                
            # وقت انتظار عشوائي بين الطلبات
            wait_time = random.uniform(MIN_WAIT_TIME, MAX_WAIT_TIME)
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
            # التحقق من حالة الإيقاف المؤقت
            if pause_event.is_set():
                await asyncio.sleep(1)
                continue
                
            username = await asyncio.wait_for(queue.get(), timeout=1.0)
            if username is None:
                queue.task_done()
                break
                
            account_data = await session_manager.get_account(timeout=60)
            if account_data is None:
                logger.warning("انتهت مهلة انتظار الحصول على حساب. إعادة المحاولة...")
                continue
                
            account_id = account_data['account_id']
            client = account_data['client']
            input_channel = account_data['input_channel']
            phone = account_data['phone']
            claimed = False
            claimer = None
            account_info = None
            
            try:
                session_string = session_manager.get_session_string(client)
                claimer = AdvancedUsernameClaimer(session_string, session_manager)
                started = await claimer.start()
                if not started:
                    continue
                    
                # محاولة تثبيت اليوزر
                claimed, account_info = await claimer.claim_username(input_channel, username)
                
                if claimed:
                    async with checker.lock:
                        checker.claimed_usernames.append(username)
                        
                    if progress_callback:
                        await progress_callback(f"✅ تم تثبيت اليوزر: {username}")
                    
                    # إرسال إشعار للمالك
                    try:
                        # الحصول على معلومات الحساب للإشعار
                        me = await client.get_me()
                        account_username = f"@{me.username}" if me.username else f"+{me.phone}"
                        
                        # إنشاء نص الإشعار
                        notification = (
                            f"🎉 **تم تثبيت يوزر جديد بنجاح!**\n\n"
                            f"• اليوزر المحجوز: `{username}`\n"
                            f"• الحساب: `{account_username}`\n"
                            f"• رقم الهاتف: `+{phone}`\n"
                            f"• معرّف الحساب: `{me.id}`"
                        )
                        
                        # إرسال الإشعار لكل مسؤول
                        for admin_id in ADMIN_IDS:
                            await context.bot.send_message(
                                chat_id=admin_id,
                                text=notification,
                                parse_mode="Markdown"
                            )
                    except Exception as e:
                        logger.error(f"خطأ في إرسال الإشعار: {e}")
            except (UserDeactivatedError, UserDeactivatedBanError):
                logger.error("الحساب محظور. إزالته من الطابور مؤقتاً.")
                await session_manager.mark_account_banned(account_id)
            except Exception as e:
                logger.error(f"خطأ في عامل التثبيت: {e}")
            finally:
                # تأكد من إعادة الحساب حتى في حالة الخطأ
                if claimer:
                    try:
                        await claimer.cleanup()
                    except:
                        pass
                # إعادة الحساب إلى الطابور إذا لم يتم حظره
                if account_id not in session_manager.banned_accounts:
                    try:
                        await session_manager.release_account(account_id)
                    except:
                        pass
                queue.task_done()
                
                # وقت انتظار عشوائي بين الطلبات
                wait_time = random.uniform(MIN_WAIT_TIME, MAX_WAIT_TIME)
                await asyncio.sleep(wait_time)
        except asyncio.TimeoutError:
            if stop_event.is_set():
                break
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"خطأ في عامل التثبيت: {e}")
            # تأكد من إكمال المهمة حتى في حالة الخطأ
            try:
                queue.task_done()
            except:
                pass

# ============================ واجهة البوت التفاعلية ============================
@owner_only
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """يعرض القائمة الرئيسية"""
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
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
             "⚡️ بوت صيد يوزرات تيليجرام المتطور",
             reply_markup=reply_markup
        )
    else:
        await update.message.reply_text(
             "⚡️ بوت صيد يوزرات تيليجرام المتطور",
            reply_markup=reply_markup
        )
    return ConversationHandler.END

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
    """بدء عملية الصيد بالقالب المحدد"""
    try:
        pattern = update.message.text
        context.user_data['pattern'] = pattern
        
        # إرسال رسالة تأكيد
        msg = await update.message.reply_text(
            f"⏳ جاري بدء عملية الصيد للقالب: <code>{pattern}</code>...",
            parse_mode="HTML"
        )
        context.user_data['progress_message_id'] = msg.message_id
        context.user_data['chat_id'] = update.message.chat_id
        
        # بدء عملية الصيد في الخلفية
        asyncio.create_task(start_hunting(update, context))
        
        return HUNTING_IN_PROGRESS
        
    except Exception as e:
        logger.error(f"خطأ في enter_pattern: {e}")
        await update.message.reply_text("❌ حدث خطأ أثناء بدء عملية الصيد.")
        return ConversationHandler.END

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
async def resume_hunt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """استئناف عملية الصيد الأخيرة"""
    query = update.callback_query
    await query.answer()
    
    # استرجاع بيانات العملية من user_data
    if 'hunt_data' not in context.user_data:
        await query.edit_message_text("❌ لا توجد بيانات عملية سابقة.")
        return
    
    hunt_data = context.user_data['hunt_data']
    category_id = hunt_data['category_id']
    pattern = hunt_data['pattern']
    progress_message_id = hunt_data['progress_message_id']
    chat_id = hunt_data['chat_id']
    
    # تخزين البيانات في context.user_data للعملية الجارية
    context.user_data.update({
        'category_id': category_id,
        'pattern': pattern,
        'progress_message_id': progress_message_id,
        'chat_id': chat_id
    })
    
    # تحديث رسالة التقدم
    await update_progress(context, "⏳ جاري استئناف عملية الصيد...")
    
    # بدء عملية الصيد
    asyncio.create_task(start_hunting(update, context, resume=True))
    
    return HUNTING_IN_PROGRESS

async def start_hunting(update: Update, context: ContextTypes.DEFAULT_TYPE, resume=False):
    """بدء عملية الصيد الفعلية"""
    bot_clients = context.bot_data.get('bot_clients', [])
    session_manager = None
    stop_event = asyncio.Event()
    pause_event = asyncio.Event()
    tasks = []
    
    # تخزين أحداث التحكم في السياق للوصول من الأوامر
    context.user_data['stop_event'] = stop_event
    context.user_data['pause_event'] = pause_event
    
    try:
        category_id = context.user_data['category_id']
        pattern = context.user_data['pattern']
        
        if not bot_clients:
            # تحميل جلسات البوتات للفحص
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
        
        # تحديث حالة التقدم
        await update_progress(context, 
            f"🔍 <b>جاري بدء عملية الصيد</b>\n\n"
            f"📂 الفئة: {category_id}\n"
            f"🔄 القالب: {pattern}\n"
            f"⏳ جاري تحميل الحسابات..."
        )
        
        # تحميل جلسات الحسابات
        session_manager = SessionManager(category_id)
        await session_manager.load_sessions()
        num_accounts = len(session_manager.sessions)
        
        if num_accounts == 0:
            await update_progress(context, "❌ لا توجد حسابات متاحة في هذه الفئة!")
            return
        
        # تخزين مدير الجلسات للوصول من الأوامر
        context.user_data['session_manager'] = session_manager
        
        # تحديث حالة التقدم
        await update_progress(context, 
            f"🚀 <b>بدأت عملية الصيد!</b>\n\n"
            f"📂 الفئة: {category_id}\n"
            f"🔄 القالب: {pattern}\n"
            f"👥 عدد الحسابات: {num_accounts}\n"
            f"🤖 عدد بوتات الفحص: {len(bot_clients)}\n"
            f"⏳ جاري توليد اليوزرات وبدء الفحص..."
        )
        
        # توليد اليوزرات
        generator = UsernameGenerator(pattern)
        total_count = 0
        usernames_queue = asyncio.Queue(maxsize=10000)
        
        # إنشاء نظام الفحص
        checker = UsernameChecker(bot_clients, session_manager)
        
        # بدء عمال المرحلة الثانية (الحسابات)
        num_workers = min(num_accounts, MAX_CONCURRENT_TASKS)
        
        # دالة لتحديث التقدم
        async def progress_callback(message):
            await update_progress(context, 
                f"🚀 <b>جاري عملية الصيد</b>\n\n"
                f"📂 الفئة: {category_id}\n"
                f"🔄 القالب: {pattern}\n"
                f"👥 الحسابات النشطة: {num_workers}\n"
                f"🤖 بوتات الفحص النشطة: {len(bot_clients)}\n"
                f"✅ المحجوزة: {len(checker.reserved_usernames)}\n"
                f"🔄 قيد الفحص: {usernames_queue.qsize()}\n"
                f"🎯 المحجوزة بنجاح: {len(checker.claimed_usernames)}\n"
                f"💎 يوزرات Fragment: {len(checker.fragment_usernames)}\n\n"
                f"📊 {message}"
            )
        
        # إنشاء عمال التثبيت (المرحلة الثانية)
        for i in range(num_workers):
            task = asyncio.create_task(
                worker_account_claim(
                    checker.available_usernames_queue, 
                    checker, 
                    session_manager, 
                    stop_event,
                    pause_event,
                    progress_callback
                )
            )
            tasks.append(task)
        
        # إنشاء عمال فحص البوت (المرحلة الأولى)
        for i in range(MAX_CONCURRENT_TASKS):
            task = asyncio.create_task(
                worker_bot_check(usernames_queue, checker, stop_event, pause_event)
            )
            tasks.append(task)
        
        # مهمة لتوليد اليوزرات باستخدام الدفعات
        async def generate_usernames():
            nonlocal total_count
            count = 0
            BATCH_SIZE = 1000  # حجم الدفعة
            batch = []
            
            try:
                for username in generator.generate_usernames():
                    if stop_event.is_set():
                        break
                    if pause_event.is_set():
                        await asyncio.sleep(1)
                        continue
                        
                    batch.append(username)
                    if len(batch) >= BATCH_SIZE:
                        # إضافة الدفعة كاملة إلى الطابور
                        for u in batch:
                            await usernames_queue.put(u)
                        count += len(batch)
                        total_count = count
                        batch = []
                        if count % 10000 == 0:
                            await progress_callback(f"تم توليد {count} يوزر حتى الآن")
                
                # إضافة ما تبقى
                if batch:
                    for u in batch:
                        await usernames_queue.put(u)
                    count += len(batch)
                    total_count = count
                
                # إشارات نهاية للعمال
                for _ in range(MAX_CONCURRENT_TASKS):
                    if not stop_event.is_set():
                        await usernames_queue.put(None)
            except asyncio.CancelledError:
                # إضافة ما تبقى في حالة الإلغاء
                if batch:
                    for u in batch:
                        await usernames_queue.put(u)
                    count += len(batch)
                raise
            return count
        
        gen_task = asyncio.create_task(generate_usernames())
        tasks.append(gen_task)
        
        # انتظار انتهاء التوليد والفحص الأولي
        await gen_task
        await usernames_queue.join()
        
        # تحديث حالة التقدم
        await progress_callback(f"✅ اكتملت المرحلة الأولى: {len(checker.reserved_usernames)} محجوزة")
        
        # انتظار انتهاء المرحلة الثانية
        for _ in range(num_workers):
            if not stop_event.is_set():
                await checker.available_usernames_queue.put(None)
        
        # النتائج النهائية
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
        
        # حفظ يوزرات Fragment في ملف
        with open(FRAGMENT_FILE, 'w', encoding='utf-8') as f:
            for username in checker.fragment_usernames:
                f.write(f"{username}\n")
        
        await update_progress(context, result_message)
        
        # حذف القنوات غير المستخدمة
        await session_manager.cleanup_unused_channels()
        
    except asyncio.CancelledError:
        logger.info("تم إلغاء عملية الصيد")
        
        # حفظ حالة العملية للاستئناف
        context.user_data['hunt_data'] = {
            'category_id': category_id,
            'pattern': pattern,
            'progress_message_id': context.user_data['progress_message_id'],
            'chat_id': context.user_data['chat_id']
        }
        
        await update_progress(context, "⏸ تم إيقاف العملية مؤقتاً. يمكنك استئنافها من القائمة الرئيسية.")
    except Exception as e:
        logger.error(f"خطأ جسيم في عملية الصيد: {e}", exc_info=True)
        await update_progress(context, f"❌ فشلت عملية الصيد بسبب خطأ: {str(e)}")
    finally:
        # إشارة التوقف لجميع العمال
        stop_event.set()
        pause_event.set()
        
        # إلغاء جميع المهام
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
    """عرض حالة المهام الجارية"""
    # يمكن تطوير هذه الوظيفة لعرض حالة أكثر تفصيلاً
    await update.message.reply_text("🔄 جاري التحقق من حالة المهام...")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """معالجة الأخطاء العامة"""
    logger.error(f"حدث خطأ: {context.error}", exc_info=True)
    if update.effective_message:
        await update.effective_message.reply_text("❌ حدث خطأ غير متوقع أثناء المعالجة.")

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
                CallbackQueryHandler(select_category, pattern=r"^cat_.+$"),
                CallbackQueryHandler(start, pattern="^start$")
            ],
            ENTER_PATTERN: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, enter_pattern)
            ],
            HUNTING_IN_PROGRESS: [
                CommandHandler("pause", pause_hunt),
                CommandHandler("resume", resume_command),
                CommandHandler("cancel", cancel)
            ],
            HUNTING_PAUSED: [
                CommandHandler("resume", resume_command),
                CommandHandler("cancel", cancel)
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
    application.add_handler(conv_handler)
    application.add_error_handler(error_handler)
    
    # بدء البوت
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()