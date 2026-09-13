import os
import sys
import ssl
import time
import random
import re
import json
import threading
import logging
import queue
import html as html_lib
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from bs4 import BeautifulSoup
from flask import Flask
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

# SSL-проверку глобально не отключаем. Ненадёжные MITM-прокси лучше исключать.

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    print("curl_cffi не установлен. Добавьте в requirements.txt")
    sys.exit(1)

load_dotenv()

# ============ НАСТРОЙКИ ============
EBAY_SEARCH_URL = os.getenv("EBAY_SEARCH_URL")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "40"))
DATABASE_URL = os.getenv("DATABASE_URL")
PROXY_LIST_URL = os.getenv("PROXY_LIST")
# ProxyScrape обновляет бесплатный список примерно раз в минуту.
PROXY_REFRESH_INTERVAL = 60

# Discovery: проверяем несколько РАЗНЫХ IP параллельно. Три потока — хороший
# компромисс для Render: поиск ускоряется, но мы не создаём агрессивный burst.
PROBE_CONCURRENCY = max(1, min(int(os.getenv("PROBE_CONCURRENCY", "3")), 4))
PROBE_CONNECT_TIMEOUT = float(os.getenv("PROBE_CONNECT_TIMEOUT", "3.5"))
# Быстрый discovery не должен зависать на proxy, который начал отдавать большой HTML,
# но не способен закончить ответ. В свежем логе такой адрес держал batch 18.5 сек.
PROBE_READ_TIMEOUT = float(os.getenv("PROBE_READ_TIMEOUT", "8"))
# Во второй половине discovery слегка расширяем окно, чтобы не отбрасывать
# абсолютно все медленные, но потенциально рабочие бесплатные proxy.
PROBE_FALLBACK_CONNECT_TIMEOUT = float(os.getenv("PROBE_FALLBACK_CONNECT_TIMEOUT", "4.5"))
PROBE_FALLBACK_READ_TIMEOUT = float(os.getenv("PROBE_FALLBACK_READ_TIMEOUT", "12"))
PROBE_FALLBACK_AFTER = float(os.getenv("PROBE_FALLBACK_AFTER", "45"))

# Если eBay UK не дал ни одного успешного ответа более 10 минут, один раз
# уведомляем в Telegram. После следующего успеха аварийный флаг сбрасывается.
CONNECTION_ALERT_AFTER = max(60, int(os.getenv("CONNECTION_ALERT_AFTER", "600")))
CONNECTION_WATCHDOG_INTERVAL = max(10, int(os.getenv("CONNECTION_WATCHDOG_INTERVAL", "15")))

# ============ НАПОМИНАНИЯ ОБ АУКЦИОНАХ ============
# Храним расписание в PostgreSQL (Aiven), поэтому рестарт/deploy Render не стирает его.
# Планировщик не опрашивает БД каждые несколько секунд: он спит до ближайшего
# события (не дольше минуты) и мгновенно просыпается при добавлении новой ссылки.
AUCTION_REMINDER_MINUTES = (60, 30, 10, 5)
AUCTION_SCHEDULER_MAX_SLEEP = max(15, int(os.getenv("AUCTION_SCHEDULER_MAX_SLEEP", "60")))
AUCTION_FETCH_MAX_PROXIES = max(1, min(int(os.getenv("AUCTION_FETCH_MAX_PROXIES", "4")), 6))
AUCTION_FETCH_CONNECT_TIMEOUT = float(os.getenv("AUCTION_FETCH_CONNECT_TIMEOUT", "5"))
AUCTION_FETCH_READ_TIMEOUT = float(os.getenv("AUCTION_FETCH_READ_TIMEOUT", "14"))
# Лёгкая проверка сохранённых аукционов: максимум два лота за проход. Это позволяет
# заметить досрочное завершение/изменение end time, не создавая burst на eBay/Render.
AUCTION_STATUS_BATCH = max(1, min(int(os.getenv("AUCTION_STATUS_BATCH", "1")), 2))
AUCTION_STATUS_TICK = max(30, int(os.getenv("AUCTION_STATUS_TICK", "60")))
AUCTION_STATUS_CONNECT_TIMEOUT = float(os.getenv("AUCTION_STATUS_CONNECT_TIMEOUT", "4"))
AUCTION_STATUS_READ_TIMEOUT = float(os.getenv("AUCTION_STATUS_READ_TIMEOUT", "8"))
# Не удаляем запись мгновенно в сохранённую секунду окончания: eBay официально
# тестирует extended bidding в некоторых категориях. Grace даёт время увидеть
# продление, даже если Render/proxy временно недоступны возле самого конца.
AUCTION_END_GRACE = max(300, int(os.getenv("AUCTION_END_GRACE", "1800")))

KYIV_TZ = ZoneInfo("Europe/Kyiv")
LONDON_TZ = ZoneInfo("Europe/London")

# Для уже найденной рабочей пары таймауты мягче: её не надо выбрасывать
# только потому, что один ответ оказался чуть медленнее.
FIXED_CONNECT_TIMEOUT = float(os.getenv("FIXED_CONNECT_TIMEOUT", "6"))
FIXED_READ_TIMEOUT = float(os.getenv("FIXED_READ_TIMEOUT", "18"))
# Если уже хороший proxy сорвался, даём ему ОДИН короткий шанс с новой Session.
# Максимум около 12 сек., затем сразу начинаем discovery по другим proxy.
FIXED_RECOVERY_CONNECT_TIMEOUT = float(os.getenv("FIXED_RECOVERY_CONNECT_TIMEOUT", "4"))
FIXED_RECOVERY_READ_TIMEOUT = float(os.getenv("FIXED_RECOVERY_READ_TIMEOUT", "8"))

# Один цикл discovery не должен зависать на минуты.
SEARCH_TIME_BUDGET = int(os.getenv("SEARCH_TIME_BUDGET", "90"))
MAX_SEARCH_ATTEMPTS = int(os.getenv("MAX_SEARCH_ATTEMPTS", "90"))

# Успешные proxy запоминаем и относим к ним мягче после единичного сбоя.
GOOD_PROXY_MEMORY = 60 * 60
SSL_PROXY_COOLDOWN = 60 * 60

MAX_ITEMS = 20
RETRY_DELAY = 2
GBP_TO_UAH = 60
EXTRA_DELIVERY_COST = 120

def normalize_ebay_search_url(raw_url):
    """
    Убираем конфликтующие/дублированные параметры из URL Render.
    Для ebay.co.uk: LH_PrefLoc=1 = UK Only.
    _ipg=60 достаточно: код всё равно анализирует только MAX_ITEMS=20,
    зато ответ заметно меньше и устойчивее через proxy.
    """
    if not raw_url:
        return raw_url

    parts = urlsplit(raw_url)
    pairs = parse_qsl(parts.query, keep_blank_values=True)

    # Сохраняем исходные параметры, но эти три задаём ровно по одному разу.
    replace_keys = {'LH_PrefLoc', '_ipg', '_sop'}
    cleaned = [(k, v) for k, v in pairs if k not in replace_keys]
    cleaned.extend([
        ('LH_PrefLoc', '1'),   # UK Only на ebay.co.uk
        ('_ipg', '60'),        # вместо 240: меньше HTML и меньше обрывов proxy
        ('_sop', '10'),        # Newly listed
    ])

    return urlunsplit((
        parts.scheme,
        parts.netloc,
        parts.path,
        urlencode(cleaned, doseq=True),
        parts.fragment,
    ))


EBAY_SEARCH_URL = normalize_ebay_search_url(EBAY_SEARCH_URL)

if not all([EBAY_SEARCH_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DATABASE_URL]):
    logging.error("Не хватает переменных окружения.")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
app = Flask(__name__)

is_paused = False

# Telegram listener только кладёт ссылку в очередь и сразу возвращается к getUpdates.
# Тяжёлое получение страницы eBay выполняется отдельным ОДНИМ worker'ом, чтобы
# пользовательские ссылки не создавали параллельный burst и не мешали основному monitor.
auction_add_queue = queue.Queue(maxsize=100)
auction_wakeup_event = threading.Event()
db_ready_event = threading.Event()

# ============ КОНТРОЛЬ ДОСТУПНОСТИ EBAY ============
# Используем monotonic(): системные часы Render могут корректироваться, а интервал
# "10 минут без успешного подключения" должен оставаться точным.
connection_state_lock = threading.Lock()
connection_watch_started_at = time.monotonic()
last_ebay_success_at = None
connection_alert_sent = False


def record_ebay_success():
    """Фиксирует успешный HTTP 200 с валидной выдачей и сбрасывает outage alert."""
    global last_ebay_success_at, connection_alert_sent
    now = time.monotonic()
    with connection_state_lock:
        was_alerted = connection_alert_sent
        last_ebay_success_at = now
        connection_alert_sent = False
    if was_alerted:
        logging.info("✅ Связь с eBay восстановлена; 10-минутный alert снова разрешён для будущего сбоя")


def reset_connection_watch_after_manual_resume():
    """После /start даём новые 10 минут, чтобы ручная пауза не считалась аварией."""
    global connection_watch_started_at, last_ebay_success_at, connection_alert_sent
    with connection_state_lock:
        connection_watch_started_at = time.monotonic()
        last_ebay_success_at = None
        connection_alert_sent = False


def _connection_outage_snapshot():
    now = time.monotonic()
    with connection_state_lock:
        reference = last_ebay_success_at if last_ebay_success_at is not None else connection_watch_started_at
        elapsed = max(0.0, now - reference)
        return elapsed, connection_alert_sent, (last_ebay_success_at is not None)


# ============ ПРОФИЛИ БРАУЗЕРОВ ============
# По свежему UK-логу именно Chrome150 дал HTTP 200 на IP, где Firefox147
# перед этим получил 403. Поэтому Chrome150 используем как основной профиль.
# Chrome146 — только технический fallback, если текущая сборка curl_cffi
# вдруг не поддерживает Chrome150. На 403 профиль НЕ ротируем.
BROWSER_PROFILES = [
    {
        'name': 'Chrome150_Native',
        'impersonate': 'chrome150',
        'disabled': False,
    },
    {
        'name': 'Chrome146_Native',
        'impersonate': 'chrome146',
        'disabled': False,
    },
]


def get_preferred_profile():
    for preferred in ('Chrome150_Native', 'Chrome146_Native'):
        for p in BROWSER_PROFILES:
            if p['name'] == preferred and not p.get('disabled', False):
                return p
    return None


def disable_profile(profile_name):
    for p in BROWSER_PROFILES:
        if p['name'] == profile_name:
            p['disabled'] = True
            logging.warning(f"Профиль {profile_name} отключён (не поддерживается curl_cffi)")
            break


# ============ МЕНЕДЖЕР ПРОКСИ ============
def _proxy_host(proxy):
    """Возвращает IP/hostname без порта, чтобы не проверять один exit-IP много раз."""
    if not proxy:
        return ""
    try:
        return (urlsplit(proxy).hostname or proxy).lower()
    except Exception:
        return proxy.lower()


def _proxy_scheme(proxy):
    try:
        return (urlsplit(proxy).scheme or 'http').lower()
    except Exception:
        return 'http'


class ProxyManager:
    def __init__(self, proxy_list_url=None):
        self.proxy_list_url = proxy_list_url
        self.proxies = []
        self.all_proxies = []
        self.lock = threading.Lock()
        self.last_refresh = 0
        self.refresh_interval = PROXY_REFRESH_INTERVAL

        # cooldown конкретного protocol://ip:port
        self.bad_until = {}
        # cooldown IP целиком. Нужен прежде всего для 403/429 и плохого TLS:
        # eBay видит exit IP, поэтому перебор 5 портов одного IP бессмысленен.
        self.host_bad_until = {}

        self.last_used = {}
        self.success_score = {}
        self.last_success_at = {}
        self.fail_streak = {}

    def _cleanup_bad_locked(self):
        now = time.time()
        for p in [p for p, until in self.bad_until.items() if until <= now]:
            self.bad_until.pop(p, None)
        for host in [h for h, until in self.host_bad_until.items() if until <= now]:
            self.host_bad_until.pop(host, None)

        # Возвращаем proxy после cooldown, если он есть в свежем списке.
        current = set(self.proxies)
        for p in self.all_proxies:
            if self.bad_until.get(p, 0) <= now and p not in current:
                self.proxies.append(p)
                current.add(p)

    def fetch_proxies_from_api(self):
        if not self.proxy_list_url:
            return []

        try:
            logging.info(f"Загрузка прокси из {self.proxy_list_url}")
            resp = requests.get(self.proxy_list_url, timeout=15)
            if resp.status_code != 200:
                logging.error(f"Ошибка загрузки прокси: HTTP {resp.status_code}")
                return []

            proxies = []
            seen = set()
            scheme_counts = {'http': 0, 'https': 0, 'socks5': 0, 'skipped': 0}

            for raw_line in resp.text.splitlines():
                line = raw_line.strip()
                if not line:
                    continue
                if '://' not in line:
                    line = 'http://' + line

                scheme = _proxy_scheme(line)
                if scheme not in ('http', 'https', 'socks5'):
                    scheme_counts['skipped'] += 1
                    continue
                if line in seen:
                    continue

                seen.add(line)
                proxies.append(line)
                scheme_counts[scheme] = scheme_counts.get(scheme, 0) + 1

            random.shuffle(proxies)
            logging.info(
                f"Загружено {len(proxies)} пригодных proxy "
                f"(http={scheme_counts.get('http', 0)}, "
                f"https={scheme_counts.get('https', 0)}, "
                f"socks5={scheme_counts.get('socks5', 0)}, "
                f"пропущено={scheme_counts.get('skipped', 0)})"
            )
            return proxies
        except Exception as e:
            logging.error(f"Ошибка при получении прокси: {e}")
            return []

    def refresh_proxies(self, force=False):
        with self.lock:
            self._cleanup_bad_locked()
            now = time.time()
            if not force and self.all_proxies and (now - self.last_refresh) < self.refresh_interval:
                return

        new_proxies = self.fetch_proxies_from_api()

        with self.lock:
            now = time.time()
            self._cleanup_bad_locked()
            if new_proxies:
                self.all_proxies = new_proxies
                self.proxies = [p for p in new_proxies if self.bad_until.get(p, 0) <= now]
                self.last_refresh = now

                # Не удаляем историю успешных proxy при каждом refresh: хороший IP
                # может исчезнуть из одного снимка ProxyScrape и снова появиться.
                # Чистим только совсем старую историю, чтобы память не росла бесконечно.
                cutoff = now - 6 * GOOD_PROXY_MEMORY
                stale = [p for p, ts in self.last_success_at.items() if ts < cutoff]
                for p in stale:
                    self.last_success_at.pop(p, None)
                    self.success_score.pop(p, None)
                    self.fail_streak.pop(p, None)
                    self.last_used.pop(p, None)

                logging.info(
                    f"Пул proxy обновлён: {len(self.proxies)} доступно "
                    f"(proxy cooldown: {len(self.bad_until)}, host cooldown: {len(self.host_bad_until)})"
                )
            elif not self.proxies:
                logging.warning("Не удалось получить пригодные proxy")
            else:
                logging.warning("Не удалось обновить proxy, продолжаем использовать старые")

    def _is_recent_good_locked(self, proxy, now=None):
        if now is None:
            now = time.time()
        return (now - self.last_success_at.get(proxy, 0)) <= GOOD_PROXY_MEMORY

    def is_recent_good(self, proxy):
        with self.lock:
            return self._is_recent_good_locked(proxy)

    def _candidate_score_locked(self, proxy, now):
        # Недавно успешные адреса всегда впереди, как только их короткий cooldown закончился.
        last_ok = self.last_success_at.get(proxy, 0)
        age_ok = now - last_ok if last_ok else 10**9
        recent_bonus = 0.0
        if age_ok <= 5 * 60:
            recent_bonus = 100.0
        elif age_ok <= 30 * 60:
            recent_bonus = 60.0
        elif age_ok <= GOOD_PROXY_MEMORY:
            recent_bonus = 30.0

        success_bonus = self.success_score.get(proxy, 0) * 8.0
        fail_penalty = self.fail_streak.get(proxy, 0) * 4.0

        # По двум свежим UK-логам реальные победители были HTTP. Это лишь мягкий
        # приоритет, SOCKS5 по-прежнему участвует в поиске.
        scheme_bonus = 1.2 if _proxy_scheme(proxy) in ('http', 'https') else 0.0

        # Давно не пробовавшиеся адреса немного выше только что проверенных.
        idle = now - self.last_used.get(proxy, 0)
        idle_bonus = min(4.0, idle / 60.0) if idle < 10**8 else 4.0

        return recent_bonus + success_bonus + scheme_bonus + idle_bonus - fail_penalty + random.uniform(0, 2.0)

    def get_candidate_batch(self, batch_size, tried_hosts=None):
        """Выдаёт несколько proxy с уникальными IP для параллельной проверки."""
        tried_hosts = tried_hosts or set()
        self.refresh_proxies()
        now = time.time()

        with self.lock:
            self._cleanup_bad_locked()
            candidates = list(self.proxies)

            # Недавно успешный proxy можно повторно проверить даже если его временно
            # нет в текущем снимке ProxyScrape.
            for p, ts in self.last_success_at.items():
                if now - ts <= GOOD_PROXY_MEMORY and p not in candidates:
                    if self.bad_until.get(p, 0) <= now:
                        candidates.append(p)

            usable = []
            for p in candidates:
                if self.bad_until.get(p, 0) > now:
                    continue
                host = _proxy_host(p)
                if not host or host in tried_hosts:
                    continue
                if self.host_bad_until.get(host, 0) > now:
                    continue
                usable.append(p)

            usable.sort(key=lambda p: self._candidate_score_locked(p, now), reverse=True)

            batch = []
            batch_hosts = set()
            for p in usable:
                host = _proxy_host(p)
                if host in batch_hosts:
                    continue
                batch.append(p)
                batch_hosts.add(host)
                self.last_used[p] = now
                if len(batch) >= batch_size:
                    break

            return batch

    def mark_success(self, proxy):
        if not proxy:
            return
        now = time.time()
        host = _proxy_host(proxy)
        with self.lock:
            self.success_score[proxy] = min(20, self.success_score.get(proxy, 0) + 2)
            self.last_success_at[proxy] = now
            self.fail_streak[proxy] = 0
            self.bad_until.pop(proxy, None)
            # Если этот же IP только что доказал работоспособность, снимаем host cooldown.
            self.host_bad_until.pop(host, None)
            if proxy not in self.proxies:
                self.proxies.append(proxy)

    def mark_failure(self, proxy, result, reason=None):
        """Адаптивный cooldown: хороший proxy после одного сбоя возвращается быстро."""
        if not proxy:
            return 0

        now = time.time()
        host = _proxy_host(proxy)
        with self.lock:
            known_good = self._is_recent_good_locked(proxy, now)
            streak = self.fail_streak.get(proxy, 0) + 1
            self.fail_streak[proxy] = streak

            # Единичный сбой недавно успешного proxy не уничтожает его репутацию.
            old_score = self.success_score.get(proxy, 0)
            self.success_score[proxy] = max(1 if known_good else 0, old_score - 1)

            if result == 'proxy_ssl':
                cooldown = SSL_PROXY_COOLDOWN
                host_cooldown = cooldown
            elif result == 'rate_limited':
                if known_good:
                    cooldown = [120, 300, 600, 1200][min(streak - 1, 3)]
                else:
                    cooldown = [600, 1200, 1800, 1800][min(streak - 1, 3)]
                host_cooldown = cooldown
            elif result == 'blocked':
                if known_good:
                    # Главное изменение: успешный IP после единичного 403 вернётся
                    # через ~30 сек, а не будет потерян на 30 минут.
                    cooldown = [30, 90, 300, 600][min(streak - 1, 3)]
                else:
                    cooldown = [300, 600, 1200, 1800][min(streak - 1, 3)]
                host_cooldown = cooldown
            elif result == 'proxy_rejected':
                # CONNECT 400/405/500/aborted у никогда не работавшего endpoint обычно
                # означает, что он не умеет нормально туннелировать HTTPS к eBay.
                # Не тратим на него следующий discovery через 1-2 минуты. Для ранее
                # успешного proxy сохраняем мягкое отношение — сбой мог быть временным.
                if known_good:
                    cooldown = [20, 45, 120, 300][min(streak - 1, 3)]
                else:
                    cooldown = [600, 900, 1200, 1800][min(streak - 1, 3)]
                host_cooldown = 0
            elif result == 'proxy_timeout':
                if known_good:
                    cooldown = [20, 45, 120, 300][min(streak - 1, 3)]
                else:
                    cooldown = [75, 150, 300, 600][min(streak - 1, 3)]
                host_cooldown = 0
            elif result == 'proxy_error':
                if known_good:
                    cooldown = [15, 30, 90, 300][min(streak - 1, 3)]
                else:
                    cooldown = [90, 180, 300, 600][min(streak - 1, 3)]
                host_cooldown = 0
            else:
                if known_good:
                    cooldown = [30, 90, 180, 300][min(streak - 1, 3)]
                else:
                    cooldown = [180, 300, 600, 900][min(streak - 1, 3)]
                host_cooldown = 0

            self.bad_until[proxy] = now + cooldown
            if host_cooldown:
                self.host_bad_until[host] = max(self.host_bad_until.get(host, 0), now + host_cooldown)
            if proxy in self.proxies:
                self.proxies.remove(proxy)

            label = reason or result
            logging.info(
                f"Proxy {proxy} cooldown {cooldown} сек. "
                f"Причина: {label}; known_good={known_good}, fail_streak={streak}. "
                f"Осталось {len(self.proxies)} proxy"
            )
            return cooldown


proxy_manager = ProxyManager(PROXY_LIST_URL)

# ============ ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ============
fixed_proxy = None
fixed_profile = None
fixed_session = None


def close_session(session):
    if session is None:
        return
    try:
        session.close()
    except Exception:
        pass

# ============ БАЗА ДАННЫХ ============
def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def init_db():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS seen_items (item_id TEXT PRIMARY KEY, first_seen TIMESTAMP)")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS auction_reminders (
                    item_id TEXT PRIMARY KEY,
                    url TEXT NOT NULL,
                    title TEXT NOT NULL,
                    end_time_utc TIMESTAMPTZ NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    reminder_60_sent BOOLEAN NOT NULL DEFAULT FALSE,
                    reminder_30_sent BOOLEAN NOT NULL DEFAULT FALSE,
                    reminder_10_sent BOOLEAN NOT NULL DEFAULT FALSE,
                    reminder_5_sent BOOLEAN NOT NULL DEFAULT FALSE,
                    last_status_check TIMESTAMPTZ NULL
                )
                """
            )
            # Миграция для баз, созданных предыдущими версиями.
            cur.execute(
                "ALTER TABLE auction_reminders "
                "ADD COLUMN IF NOT EXISTS last_status_check TIMESTAMPTZ NULL"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_auction_reminders_end_time "
                "ON auction_reminders (end_time_utc)"
            )
        conn.commit()

def get_seen_ids():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT item_id FROM seen_items")
            return {row[0] for row in cur.fetchall()}

def add_seen_ids_batch(item_ids):
    if not item_ids:
        return
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            data = [(item_id, datetime.now()) for item_id in item_ids]
            execute_values(cur, "INSERT INTO seen_items (item_id, first_seen) VALUES %s ON CONFLICT (item_id) DO NOTHING", data)
        conn.commit()

def is_db_empty():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT NOT EXISTS (SELECT 1 FROM seen_items)")
            return cur.fetchone()[0]


# ============ АУКЦИОНЫ: БД И ВРЕМЯ ============
def _ensure_aware_utc(dt):
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def format_kyiv_datetime(dt):
    dt = _ensure_aware_utc(dt).astimezone(KYIV_TZ)
    return dt.strftime("%d.%m.%Y в %H:%M:%S")


def format_remaining(seconds, with_seconds=True):
    seconds = max(0, int(seconds))
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days} дн.")
    if hours or days:
        parts.append(f"{hours} ч.")
    parts.append(f"{minutes} мин.")
    if with_seconds and not days:
        parts.append(f"{secs} сек.")
    return " ".join(parts)


def _initial_reminder_flags(end_time_utc):
    remaining = (_ensure_aware_utc(end_time_utc) - datetime.now(timezone.utc)).total_seconds()
    # Уже прошедшие пороги помечаем отправленными: если пользователь добавил лот
    # за 22 минуты до конца, мы не шлём сразу "за час" и "за 30 минут".
    return {
        60: remaining <= 60 * 60,
        30: remaining <= 30 * 60,
        10: remaining <= 10 * 60,
        5: remaining <= 5 * 60,
    }


def save_auction_reminder(item_id, url, title, end_time_utc):
    end_time_utc = _ensure_aware_utc(end_time_utc)
    flags = _initial_reminder_flags(end_time_utc)
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO auction_reminders (
                    item_id, url, title, end_time_utc,
                    reminder_60_sent, reminder_30_sent, reminder_10_sent, reminder_5_sent
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (item_id) DO UPDATE SET
                    url = EXCLUDED.url,
                    title = EXCLUDED.title,
                    end_time_utc = EXCLUDED.end_time_utc,
                    updated_at = NOW(),
                    reminder_60_sent = CASE
                        WHEN auction_reminders.end_time_utc = EXCLUDED.end_time_utc
                        THEN auction_reminders.reminder_60_sent
                        ELSE EXCLUDED.reminder_60_sent END,
                    reminder_30_sent = CASE
                        WHEN auction_reminders.end_time_utc = EXCLUDED.end_time_utc
                        THEN auction_reminders.reminder_30_sent
                        ELSE EXCLUDED.reminder_30_sent END,
                    reminder_10_sent = CASE
                        WHEN auction_reminders.end_time_utc = EXCLUDED.end_time_utc
                        THEN auction_reminders.reminder_10_sent
                        ELSE EXCLUDED.reminder_10_sent END,
                    reminder_5_sent = CASE
                        WHEN auction_reminders.end_time_utc = EXCLUDED.end_time_utc
                        THEN auction_reminders.reminder_5_sent
                        ELSE EXCLUDED.reminder_5_sent END
                """,
                (
                    item_id, url, title, end_time_utc,
                    flags[60], flags[30], flags[10], flags[5],
                ),
            )
        conn.commit()
    auction_wakeup_event.set()


def list_active_auctions(limit=20):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT item_id, url, title, end_time_utc
                FROM auction_reminders
                WHERE end_time_utc > NOW()
                ORDER BY end_time_utc ASC
                LIMIT %s
                """,
                (limit,),
            )
            return cur.fetchall()


def delete_auction_reminder(item_id):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM auction_reminders WHERE item_id = %s RETURNING item_id", (item_id,))
            deleted = cur.fetchone() is not None
        conn.commit()
    auction_wakeup_event.set()
    return deleted


def mark_auction_status_checked(item_id):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE auction_reminders SET last_status_check = NOW(), updated_at = NOW() WHERE item_id = %s",
                (item_id,),
            )
        conn.commit()


def update_verified_auction(item_id, title, new_end_time_utc):
    """Обновляет title/end time и возвращает (exists, changed, old_end, new_end).

    Если eBay сдвинул окончание, заново активируем только те reminders, чей новый
    порог ещё впереди. Уже прошедшие пороги не шлём задним числом.
    """
    new_end_time_utc = _ensure_aware_utc(new_end_time_utc)
    now = datetime.now(timezone.utc)
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT end_time_utc, reminder_60_sent, reminder_30_sent,
                       reminder_10_sent, reminder_5_sent
                FROM auction_reminders WHERE item_id = %s FOR UPDATE
                """,
                (item_id,),
            )
            row = cur.fetchone()
            if not row:
                return False, False, None, new_end_time_utc
            old_end, s60, s30, s10, s5 = row
            old_end = _ensure_aware_utc(old_end)
            changed = abs((old_end - new_end_time_utc).total_seconds()) > 5
            if changed:
                sent = {60: s60, 30: s30, 10: s10, 5: s5}
                for mins in sent:
                    if (new_end_time_utc - now).total_seconds() > mins * 60:
                        sent[mins] = False
                cur.execute(
                    """
                    UPDATE auction_reminders
                    SET title=%s, end_time_utc=%s, last_status_check=NOW(), updated_at=NOW(),
                        reminder_60_sent=%s, reminder_30_sent=%s,
                        reminder_10_sent=%s, reminder_5_sent=%s
                    WHERE item_id=%s
                    """,
                    (title, new_end_time_utc, sent[60], sent[30], sent[10], sent[5], item_id),
                )
            else:
                cur.execute(
                    "UPDATE auction_reminders SET title=%s, last_status_check=NOW(), updated_at=NOW() WHERE item_id=%s",
                    (title, item_id),
                )
        conn.commit()
    if changed:
        auction_wakeup_event.set()
    return True, changed, old_end, new_end_time_utc


def get_auctions_for_status_check(limit=100):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT item_id, url, title, end_time_utc, last_status_check
                FROM auction_reminders
                WHERE end_time_utc > NOW() - (%s * INTERVAL '1 second')
                ORDER BY COALESCE(last_status_check, TIMESTAMPTZ '1970-01-01') ASC,
                         end_time_utc ASC
                LIMIT %s
                """,
                (AUCTION_END_GRACE, limit),
            )
            return cur.fetchall()


def _status_check_interval_seconds(remaining):
    # Возле самого конца проверяем чаще: если eBay продлит торги после ставки
    # в последние 60 секунд, новое end time попадёт в БД до очистки записи.
    if remaining <= 120:
        return 30
    if remaining <= 10 * 60:
        return 60
    if remaining > 24 * 3600:
        return 3600
    if remaining > 6 * 3600:
        return 1800
    if remaining > 3600:
        return 900
    return 300


def _reminder_column(minutes):
    mapping = {
        60: 'reminder_60_sent',
        30: 'reminder_30_sent',
        10: 'reminder_10_sent',
        5: 'reminder_5_sent',
    }
    return mapping[minutes]


def _mark_reminder_sent(conn, item_id, minutes):
    column = _reminder_column(minutes)
    # column приходит только из жёсткого mapping выше, пользовательского SQL здесь нет.
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE auction_reminders SET {column} = TRUE, updated_at = NOW() WHERE item_id = %s",
            (item_id,),
        )
    conn.commit()


def _delete_expired_auctions(conn, now_utc):
    cutoff = now_utc - timedelta(seconds=AUCTION_END_GRACE)
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM auction_reminders WHERE end_time_utc <= %s RETURNING item_id",
            (cutoff,),
        )
        deleted = cur.fetchall()
    conn.commit()
    if deleted:
        logging.info(f"🧹 Удалено завершённых аукционов из reminders: {len(deleted)}")


# ============ TELEGRAM ============
def send_telegram_message(message, parse_mode='HTML', reply_markup=None, disable_preview=False):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message,
        'parse_mode': parse_mode,
        'disable_web_page_preview': disable_preview,
    }
    if reply_markup is not None:
        payload['reply_markup'] = reply_markup
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            logging.error(f"Ошибка Telegram: {r.text}")
            return False
        return True
    except Exception as e:
        logging.error(f"Не удалось отправить в Telegram: {e}")
        return False


def answer_callback_query(callback_query_id, text=None, show_alert=False):
    if not callback_query_id:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
    payload = {'callback_query_id': callback_query_id, 'show_alert': bool(show_alert)}
    if text:
        payload['text'] = str(text)[:200]
    try:
        r = requests.post(url, json=payload, timeout=8)
        return r.status_code == 200
    except Exception as e:
        logging.error(f"Не удалось ответить на callback Telegram: {e}")
        return False


def edit_message_reply_markup(message_id, reply_markup=None):
    if not message_id:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageReplyMarkup"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'message_id': message_id,
        'reply_markup': reply_markup or {'inline_keyboard': []},
    }
    try:
        r = requests.post(url, json=payload, timeout=8)
        return r.status_code == 200
    except Exception as e:
        logging.error(f"Не удалось обновить кнопки Telegram: {e}")
        return False


def auction_message_keyboard(item_id, url, include_list=True):
    rows = [[{'text': '🔗 Открыть eBay', 'url': url}]]
    bottom = [{'text': '🗑 Удалить', 'callback_data': f'aucdel:{item_id}'}]
    if include_list:
        bottom.append({'text': '📋 Все аукционы', 'callback_data': 'auclist'})
    rows.append(bottom)
    return {'inline_keyboard': rows}


def auction_list_only_keyboard():
    return {'inline_keyboard': [[{'text': '📋 Все аукционы', 'callback_data': 'auclist'}]]}

def connection_watchdog():
    """Шлёт ровно одно предупреждение на один непрерывный outage >= 10 минут."""
    global connection_alert_sent
    logging.info(
        f"🩺 Watchdog eBay запущен: предупреждение после {CONNECTION_ALERT_AFTER // 60} мин без успеха"
    )
    while True:
        try:
            if not is_paused:
                elapsed, already_sent, had_success = _connection_outage_snapshot()
                if elapsed >= CONNECTION_ALERT_AFTER and not already_sent:
                    # Ставим флаг ДО Telegram-запроса, чтобы два прохода watchdog
                    # никогда не отправили дубль. Если Telegram не принял сообщение,
                    # флаг вернём назад и попробуем снова на следующем проходе.
                    should_send = False
                    with connection_state_lock:
                        if not connection_alert_sent:
                            connection_alert_sent = True
                            should_send = True
                    if not should_send:
                        time.sleep(CONNECTION_WATCHDOG_INTERVAL)
                        continue

                    minutes = max(10, int(elapsed // 60))
                    if had_success:
                        status_line = f"⏱ Уже <b>{minutes} мин.</b> нет успешного подключения к eBay UK."
                    else:
                        status_line = f"⏱ Уже <b>{minutes} мин.</b> после запуска нет успешного подключения к eBay UK."

                    message = (
                        "⚠️ <b>eBay UK — проверьте подключение</b> 🇬🇧\n\n"
                        f"{status_line}\n"
                        "🤖 Бот продолжает работать и автоматически искать рабочий proxy.\n\n"
                        "👉 <b>Пожалуйста, проверьте сайт/подключение вручную.</b>\n\n"
                        "Следующее такое предупреждение будет отправлено только если связь "
                        "сначала восстановится, а затем снова пропадёт более чем на 10 минут."
                    )
                    if send_telegram_message(message):
                        logging.warning(
                            f"📨 Отправлено Telegram-предупреждение: eBay недоступен {elapsed:.0f} сек."
                        )
                    else:
                        with connection_state_lock:
                            connection_alert_sent = False
            time.sleep(CONNECTION_WATCHDOG_INTERVAL)
        except Exception as e:
            logging.error(f"Ошибка connection watchdog: {e}", exc_info=True)
            time.sleep(CONNECTION_WATCHDOG_INTERVAL)



# ============ TELEGRAM -> EBAY AUCTION REMINDERS ============
def _is_allowed_ebay_url(url):
    try:
        host = (urlsplit(url).hostname or '').lower().rstrip('.')
    except Exception:
        return False
    allowed = ('ebay.co.uk', 'ebay.com', 'ebay.us')
    return any(host == suffix or host.endswith('.' + suffix) for suffix in allowed)


def extract_ebay_urls(text):
    urls = []
    for raw in re.findall(r'https?://[^\s<>]+', text or '', flags=re.I):
        url = raw.rstrip(').,;]>}\"\'')
        if _is_allowed_ebay_url(url) and url not in urls:
            urls.append(url)
    return urls


def extract_ebay_item_id_any(url, html=None):
    for source in (url or '', html or ''):
        patterns = (
            r'/itm/(?:[^/?#]+/)?(\d{9,15})(?:[/?#]|$)',
            r'[?&](?:item|itemid|item_id)=(\d{9,15})(?:&|$)',
            r'eBay\s+item\s+number\s*:?\s*(\d{9,15})',
            r'"itemId"\s*:\s*"?(\d{9,15})"?',
        )
        for pattern in patterns:
            m = re.search(pattern, source, re.I)
            if m:
                return m.group(1)
    return None


def _request_auction_page_once(url, proxy, profile, connect_timeout=None, read_timeout=None):
    session = None
    connect_timeout = AUCTION_FETCH_CONNECT_TIMEOUT if connect_timeout is None else connect_timeout
    read_timeout = AUCTION_FETCH_READ_TIMEOUT if read_timeout is None else read_timeout
    try:
        session = _create_session(proxy, profile, timeout=(connect_timeout, read_timeout))
        response = session.get(
            url,
            timeout=(connect_timeout, read_timeout),
            allow_redirects=True,
        )
        final_url = str(getattr(response, 'url', '') or '')
        if response.status_code not in (200, 404, 410):
            return None, final_url
        blocked, _ = _is_ebay_block_page(response)
        if blocked:
            return None, final_url
        if final_url and not _is_allowed_ebay_url(final_url):
            return None, final_url
        return response.text, final_url
    except Exception as e:
        logging.info(f"Auction page через {proxy} не получена: {e}")
        return None, ''
    finally:
        close_session(session)


def fetch_auction_page(url, max_reserve_proxies=None, connect_timeout=None, read_timeout=None):
    """Редкая операция: fixed proxy первым, затем небольшой резерв без burst."""
    if not _is_allowed_ebay_url(url):
        return None, None, None

    profile = get_preferred_profile()
    if profile is None:
        return None, None, None

    if max_reserve_proxies is None:
        max_reserve_proxies = AUCTION_FETCH_MAX_PROXIES
    max_reserve_proxies = max(0, min(int(max_reserve_proxies), 6))

    candidates = []
    current_fixed = fixed_proxy
    if current_fixed:
        candidates.append(current_fixed)

    tried_hosts = {_proxy_host(p) for p in candidates if p}
    if max_reserve_proxies:
        for p in proxy_manager.get_candidate_batch(max_reserve_proxies, tried_hosts=tried_hosts):
            if p not in candidates:
                candidates.append(p)

    for proxy in candidates[:1 + max_reserve_proxies]:
        html, final_url = _request_auction_page_once(
            url, proxy, profile,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
        )
        if html:
            return html, final_url or url, proxy
    return None, None, None

def _parse_iso_datetime(value):
    if not value:
        return None
    try:
        normalized = value.strip().replace('Z', '+00:00')
        dt = datetime.fromisoformat(normalized)
        return _ensure_aware_utc(dt)
    except Exception:
        return None


def parse_auction_page(html, final_url):
    """Возвращает item_id, title, exact end_time_utc, source, status.

    status: active | ended | not_auction | no_item_id | no_exact_end_time | time_conflict
    В fixed-price листинге один itemEndDate НЕ считается доказательством аукциона.
    """
    if not html:
        return None, None, None, 'empty', 'unknown'

    soup = BeautifulSoup(html, 'html.parser')
    visible = re.sub(r'\s+', ' ', soup.get_text(' ', strip=True))
    item_id = extract_ebay_item_id_any(final_url, html)

    title = ''
    h1 = soup.find('h1')
    if h1:
        title = re.sub(r'\s+', ' ', h1.get_text(' ', strip=True)).strip()
    if not title:
        title = _response_title(html)
        title = re.sub(r'\s*\|\s*eBay(?:\s+UK)?\s*$', '', title, flags=re.I).strip()
    title = title[:300] or f'eBay item {item_id or ""}'.strip()

    # 1) Точное время, которое eBay показывает пользователю. Учитываем явный BST/GMT,
    # а не предполагаем смещение: BST=UTC+1, GMT=UTC+0.
    visible_end_utc = None
    visible_kind = None
    m = re.search(
        r'\b(Ends|Ended)\s*:?\s*'
        r'(\d{1,2}\s+[A-Za-z]{3},\s+\d{4}\s+\d{1,2}:\d{2}:\d{2})'
        r'\s*(BST|GMT)\b',
        visible,
        re.I,
    )
    if m:
        try:
            naive = datetime.strptime(m.group(2), '%d %b, %Y %H:%M:%S')
            offset = timedelta(hours=1 if m.group(3).upper() == 'BST' else 0)
            visible_end_utc = naive.replace(tzinfo=timezone(offset)).astimezone(timezone.utc)
            visible_kind = m.group(1).lower()
        except ValueError:
            pass

    # 2) Независимый UTC timestamp из данных страницы — используем как fallback
    # и как перекрёстную проверку, когда доступны оба источника.
    iso_end_utc = None
    patterns = (
        r'"itemEndDate"\s*:\s*"([^"\\]+)"',
        r'\\"itemEndDate\\"\s*:\s*\\"([^"\\]+)\\"',
    )
    # В большой eBay-странице есть recommendation-карточки с чужими end dates.
    # Поэтому сначала ищем timestamp только рядом с ID текущего лота.
    windows = []
    if item_id:
        for mm in re.finditer(re.escape(item_id), html):
            windows.append(html[mm.start(): min(len(html), mm.end()+9000)])
            if len(windows) >= 8:
                break
    for window in windows:
        for pattern in patterns:
            m2 = re.search(pattern, window, re.I)
            if m2:
                iso_end_utc = _parse_iso_datetime(m2.group(1))
                if iso_end_utc:
                    break
        if iso_end_utc:
            break
    # Если рядом с item_id ключа нет, используем его только когда на всей странице
    # найден ровно один уникальный itemEndDate — так не перепутаем с рекомендациями.
    if iso_end_utc is None:
        raw_values = []
        for pattern in patterns:
            raw_values.extend(re.findall(pattern, html, re.I))
        unique_values = list(dict.fromkeys(raw_values))
        if len(unique_values) == 1:
            iso_end_utc = _parse_iso_datetime(unique_values[0])

    if visible_end_utc and iso_end_utc:
        if abs((visible_end_utc - iso_end_utc).total_seconds()) > 5:
            return item_id, title, None, 'visible_vs_itemEndDate_conflict', 'time_conflict'
        end_time_utc = visible_end_utc
        source = 'visible_exact_uk_time+itemEndDate'
    elif visible_end_utc:
        end_time_utc = visible_end_utc
        source = 'visible_exact_uk_time'
    elif iso_end_utc:
        end_time_utc = iso_end_utc
        source = 'itemEndDate'
    else:
        end_time_utc = None
        source = 'none'

    # Положительное подтверждение аукциона. Не доверяем словам из recommendation-карточек:
    # структурный AUCTION ищем рядом с item_id, а видимые признаки — характерные для bid box.
    bid_box_marker = bool(
        re.search(r'\bplace\s+bid\b', visible, re.I)
        or re.search(r'\bcurrent\s+bid\b', visible, re.I)
        or re.search(r'\bstarting\s+bid\b', visible, re.I)
    )
    structured_auction = False
    if item_id:
        for mm in re.finditer(re.escape(item_id), html):
            window = html[mm.start(): min(len(html), mm.end()+7000)]
            if (re.search(r'"buyingOptions"\s*:\s*\[[^\]]*"AUCTION"', window, re.I)
                    or re.search(r'\\"buyingOptions\\"\s*:\s*\[[^\]]*\\"AUCTION\\"', window, re.I)
                    or re.search(r'"listingType"\s*:\s*"Auction"', window, re.I)
                    or re.search(r'"format"\s*:\s*"AUCTION"', window, re.I)):
                structured_auction = True
                break

    explicit_closed = bool(
        visible_kind == 'ended'
        or re.search(r'\bthis\s+listing\s+(?:has\s+)?ended\b', visible, re.I)
        or re.search(r'\bthis\s+listing\s+was\s+ended\b', visible, re.I)
        or re.search(r'\bthis\s+item\s+is\s+no\s+longer\s+available\b', visible, re.I)
        or re.search(r'\bthe\s+listing\s+you(?:’|\'|\s)?re\s+looking\s+for\s+has\s+ended\b', visible, re.I)
        or re.search(r'\bwe\s+looked\s+everywhere.*looks\s+like\s+this\s+page\s+is\s+missing\b', visible, re.I)
    )

    if not item_id:
        return None, title, end_time_utc, source, 'no_item_id'
    if explicit_closed:
        return item_id, title, end_time_utc, source, 'ended'
    if not end_time_utc:
        return item_id, title, None, source, 'no_exact_end_time'
    if not (bid_box_marker or structured_auction):
        return item_id, title, end_time_utc, source, 'not_auction'
    return item_id, title, end_time_utc, source, 'active'

def _future_reminder_labels(end_time_utc):
    remaining = (_ensure_aware_utc(end_time_utc) - datetime.now(timezone.utc)).total_seconds()
    labels = []
    for mins, label in ((60, '1 час'), (30, '30 минут'), (10, '10 минут'), (5, '5 минут')):
        if remaining > mins * 60:
            labels.append(label)
    return labels


def process_auction_link(url):
    if not _is_allowed_ebay_url(url):
        return

    html, final_url, proxy_used = fetch_auction_page(url)
    if not html:
        send_telegram_message(
            "❌ <b>Не удалось проверить аукцион</b>\n\n"
            "eBay сейчас не дал открыть страницу через доступные proxy. "
            "Основной мониторинг продолжает работать. Попробуйте отправить ссылку ещё раз немного позже."
        )
        return

    parse_url = final_url if extract_ebay_item_id_any(final_url or '') else url
    item_id, title, end_time_utc, parse_source, auction_status = parse_auction_page(html, parse_url)
    if proxy_used and item_id:
        proxy_manager.mark_success(proxy_used)

    if not item_id:
        send_telegram_message("❌ Не удалось определить номер лота eBay по этой ссылке.")
        return
    if auction_status == 'time_conflict':
        send_telegram_message(
            "⚠️ <b>eBay показал противоречивое время окончания.</b>\n\n"
            "Я специально не сохраняю такой лот автоматически, чтобы не дать неверное напоминание. "
            "Попробуйте отправить ссылку ещё раз через минуту."
        )
        return
    if auction_status == 'ended':
        line = f"\n🕒 Окончание по Киеву: <b>{format_kyiv_datetime(end_time_utc)}</b>" if end_time_utc else ''
        send_telegram_message("⌛ <b>Этот аукцион уже завершён.</b>" + line)
        return
    if auction_status == 'not_auction':
        send_telegram_message(
            "ℹ️ <b>Это не активный аукцион со ставками.</b>\n\n"
            "Обычные товары Buy It Now / Best Offer без режима торгов в список напоминаний не сохраняются."
        )
        return
    if auction_status != 'active' or not end_time_utc:
        send_telegram_message(
            "❌ <b>Не удалось точно подтвердить активный аукцион и время его окончания.</b>\n\n"
            "Я не сохраняю приблизительное время, чтобы вы не пропустили ставку из-за неверного расчёта."
        )
        return

    end_time_utc = _ensure_aware_utc(end_time_utc)
    remaining = (end_time_utc - datetime.now(timezone.utc)).total_seconds()
    if remaining <= 0:
        send_telegram_message(
            "⌛ <b>Этот аукцион уже завершён.</b>\n\n"
            f"🕒 Окончание по Киеву: <b>{format_kyiv_datetime(end_time_utc)}</b>"
        )
        return

    canonical_url = f"https://www.ebay.co.uk/itm/{item_id}"
    save_auction_reminder(item_id, canonical_url, title, end_time_utc)
    mark_auction_status_checked(item_id)

    safe_title = html_lib.escape(title)
    labels = _future_reminder_labels(end_time_utc)
    if labels:
        reminder_line = "🔔 Напомню: <b>" + ", ".join(labels) + "</b> до окончания."
    else:
        reminder_line = "⚠️ До конца меньше 5 минут — плановые напоминания уже прошли."

    message = (
        "✅ <b>Аукцион сохранён</b> 🇬🇧\n\n"
        f"📦 <b>{safe_title}</b>\n\n"
        f"🕒 Окончание по Киеву: <b>{format_kyiv_datetime(end_time_utc)}</b>\n"
        f"⏳ Осталось: <b>{format_remaining(remaining)}</b>\n"
        f"{reminder_line}\n\n"
        "✅ Время подтверждено по странице eBay и сохранено в PostgreSQL."
    )
    send_telegram_message(
        message,
        reply_markup=auction_message_keyboard(item_id, canonical_url),
        disable_preview=True,
    )
    logging.info(
        f"⏰ Auction reminder сохранён: item={item_id}, end_utc={end_time_utc.isoformat()}, source={parse_source}"
    )

def auction_link_worker():
    logging.info("🔗 Worker ссылок на eBay-аукционы запущен")
    db_ready_event.wait()
    while True:
        url = auction_add_queue.get()
        try:
            process_auction_link(url)
        except Exception as e:
            logging.error(f"Ошибка обработки auction URL {url}: {e}", exc_info=True)
            send_telegram_message(
                "❌ Не удалось обработать ссылку на аукцион из-за временной внутренней ошибки. "
                "Основной мониторинг продолжает работать."
            )
        finally:
            auction_add_queue.task_done()


def send_auction_list():
    try:
        rows = list_active_auctions(limit=20)
    except Exception as e:
        logging.error(f"Не удалось получить список аукционов: {e}")
        send_telegram_message("❌ Не удалось сейчас прочитать список аукционов из базы.")
        return

    if not rows:
        send_telegram_message("📭 <b>Активных аукционов для напоминаний сейчас нет.</b>")
        return

    now = datetime.now(timezone.utc)
    parts = ["⏰ <b>Мои аукционы eBay UK</b> 🇬🇧\n"]
    keyboard = []
    for idx, (item_id, url, title, end_time) in enumerate(rows, 1):
        end_time = _ensure_aware_utc(end_time)
        remaining = max(0, (end_time - now).total_seconds())
        short_title = title if len(title) <= 70 else title[:67] + '…'
        parts.append(
            f"\n<b>{idx}.</b> {html_lib.escape(short_title)}\n"
            f"🕒 <b>{format_kyiv_datetime(end_time)}</b>\n"
            f"⏳ {format_remaining(remaining, with_seconds=False)}"
        )
        keyboard.append([
            {'text': f'🔗 Открыть #{idx}', 'url': url},
            {'text': f'🗑 Удалить #{idx}', 'callback_data': f'aucdel:{item_id}'},
        ])

    parts.append("\n\nНажмите 🗑 возле нужного аукциона — номер вводить вручную не нужно.")
    send_telegram_message(
        ''.join(parts),
        reply_markup={'inline_keyboard': keyboard},
        disable_preview=True,
    )


def _notify_auction_closed_early(item_id, url, title, expected_end, detected_end=None):
    # DELETE ... RETURNING делает уведомление однократным даже если два worker'а
    # почти одновременно обнаружили закрытие.
    if not delete_auction_reminder(item_id):
        return False
    safe_title = html_lib.escape(title)
    msg = (
        "🚫 <b>Аукцион eBay завершён раньше времени</b> 🇬🇧\n\n"
        f"📦 <b>{safe_title}</b>\n\n"
        f"🗓 Было сохранено окончание: <b>{format_kyiv_datetime(expected_end)}</b>\n"
    )
    if detected_end:
        msg += f"🕒 eBay показывает завершение: <b>{format_kyiv_datetime(detected_end)}</b>\n"
    msg += (
        "\nЛот больше не является активным аукционом, поэтому я автоматически удалил его "
        "из списка напоминаний."
    )
    send_telegram_message(
        msg,
        reply_markup={'inline_keyboard': [[
            {'text': '🔗 Открыть eBay', 'url': url},
            {'text': '📋 Все аукционы', 'callback_data': 'auclist'},
        ]]},
        disable_preview=True,
    )
    return True


def verify_saved_auction(item_id, url, title, expected_end, max_reserve_proxies=1, notify_time_change=True):
    """Проверяет сохранённый лот. Никогда не удаляет запись из-за network/proxy ошибки."""
    html, final_url, proxy_used = fetch_auction_page(
        url,
        max_reserve_proxies=max_reserve_proxies,
        connect_timeout=AUCTION_STATUS_CONNECT_TIMEOUT,
        read_timeout=AUCTION_STATUS_READ_TIMEOUT,
    )
    if not html:
        return 'unverified'

    parse_url = final_url if extract_ebay_item_id_any(final_url or '') else url
    parsed_id, parsed_title, parsed_end, source, status = parse_auction_page(html, parse_url)
    if proxy_used and parsed_id:
        proxy_manager.mark_success(proxy_used)
    if parsed_id and parsed_id != item_id:
        logging.warning(f"Auction status mismatch: expected {item_id}, page {parsed_id}")
        return 'unverified'

    now = datetime.now(timezone.utc)
    expected_end = _ensure_aware_utc(expected_end)

    if status == 'ended':
        # Сообщаем именно о досрочном закрытии. Если подошло обычное время конца,
        # scheduler просто удалит запись как завершённую.
        if now < expected_end - timedelta(seconds=30):
            _notify_auction_closed_early(item_id, url, title, expected_end, parsed_end)
            return 'closed'
        delete_auction_reminder(item_id)
        return 'ended_normally'

    if status == 'active' and parsed_end:
        exists, changed, old_end, new_end = update_verified_auction(
            item_id, parsed_title or title, parsed_end
        )
        if not exists:
            return 'missing'
        if changed:
            if notify_time_change:
                delta = int((new_end - old_end).total_seconds())
                direction = 'позже' if delta > 0 else 'раньше'
                send_telegram_message(
                    "🔄 <b>eBay изменил время окончания аукциона</b>\n\n"
                    f"📦 <b>{html_lib.escape(parsed_title or title)}</b>\n"
                    f"🕒 Новое время по Киеву: <b>{format_kyiv_datetime(new_end)}</b>\n"
                    f"↔️ Изменение: <b>{abs(delta)} сек. {direction}</b>\n\n"
                    "Расписание напоминаний автоматически пересчитано.",
                    reply_markup=auction_message_keyboard(item_id, url),
                    disable_preview=True,
                )
            logging.info(
                f"🔄 Auction {item_id}: end time changed {old_end.isoformat()} -> {new_end.isoformat()} ({source})"
            )
            return 'time_changed'
        return 'active'

    # Страница получена, но нет достаточного положительного подтверждения закрытия —
    # НЕ удаляем лот. Это защищает от временной смены HTML eBay.
    mark_auction_status_checked(item_id)
    return status


def auction_status_worker():
    """Низкочастотная проверка досрочно закрытых лотов и сдвига времени eBay."""
    logging.info("🛰 Worker контроля сохранённых аукционов запущен")
    db_ready_event.wait()
    while True:
        try:
            # Не создаём дополнительный трафик, когда основной монитор сам сейчас
            # испытывает проблемы с доступом к eBay.
            elapsed, _, had_success = _connection_outage_snapshot()
            healthy = is_paused or (had_success and elapsed < 180)
            if healthy:
                rows = get_auctions_for_status_check(limit=100)
                now = datetime.now(timezone.utc)
                due = []
                for item_id, url, title, end_time, last_check in rows:
                    end_time = _ensure_aware_utc(end_time)
                    remaining = (end_time - now).total_seconds()
                    interval = _status_check_interval_seconds(remaining)
                    if last_check is None or (now - _ensure_aware_utc(last_check)).total_seconds() >= interval:
                        due.append((item_id, url, title, end_time))
                    if len(due) >= AUCTION_STATUS_BATCH:
                        break

                for item_id, url, title, end_time in due:
                    verify_saved_auction(
                        item_id, url, title, end_time,
                        max_reserve_proxies=1,
                        notify_time_change=True,
                    )
                    time.sleep(random.uniform(1.0, 2.0))
        except Exception as e:
            logging.error(f"Ошибка auction status worker: {e}", exc_info=True)
        auction_wakeup_event.wait(timeout=AUCTION_STATUS_TICK)
        auction_wakeup_event.clear()


def handle_telegram_callback(callback):
    callback_id = callback.get('id')
    data = str(callback.get('data') or '')
    message = callback.get('message') or {}
    chat_id = str((message.get('chat') or {}).get('id') or '')
    if chat_id != str(TELEGRAM_CHAT_ID):
        answer_callback_query(callback_id, "Недоступно", show_alert=False)
        return

    if data == 'auclist':
        answer_callback_query(callback_id, "Открываю список…")
        send_auction_list()
        return

    m = re.fullmatch(r'aucdel:(\d{9,15})', data)
    if m:
        item_id = m.group(1)
        if delete_auction_reminder(item_id):
            answer_callback_query(callback_id, "Аукцион удалён ✅")
            edit_message_reply_markup(message.get('message_id'), auction_list_only_keyboard())
        else:
            answer_callback_query(callback_id, "Уже удалён или завершён")
        return

    answer_callback_query(callback_id)


def auction_reminder_worker():
    """Точный scheduler: PostgreSQL хранит sent-флаги и переживает deploy/restart."""
    logging.info("⏰ Планировщик auction reminders запущен")
    db_ready_event.wait()
    conn = None
    while True:
        next_sleep = AUCTION_SCHEDULER_MAX_SLEEP
        try:
            if conn is None or conn.closed:
                conn = get_db_connection()

            now = datetime.now(timezone.utc)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT item_id, url, title, end_time_utc,
                           reminder_60_sent, reminder_30_sent, reminder_10_sent, reminder_5_sent,
                           last_status_check
                    FROM auction_reminders
                    WHERE end_time_utc > %s
                    ORDER BY end_time_utc ASC
                    LIMIT 200
                    """,
                    (now,),
                )
                rows = cur.fetchall()

            for row in rows:
                item_id, url, title, end_time, sent60, sent30, sent10, sent5, last_check = row
                end_time = _ensure_aware_utc(end_time)
                remaining = (end_time - now).total_seconds()
                sent_map = {60: sent60, 30: sent30, 10: sent10, 5: sent5}

                for minutes in AUCTION_REMINDER_MINUTES:
                    if sent_map[minutes]:
                        continue
                    trigger_time = end_time - timedelta(minutes=minutes)
                    seconds_to_trigger = (trigger_time - now).total_seconds()

                    if seconds_to_trigger <= 0 < remaining:
                        # Перед важным reminder стараемся подтвердить, что лот ещё активен.
                        # Если сеть/proxy не дали проверить — НЕ задерживаем и НЕ пропускаем reminder.
                        check_is_stale = (
                            last_check is None
                            or (now - _ensure_aware_utc(last_check)).total_seconds() > 180
                        )
                        if check_is_stale:
                            verification = verify_saved_auction(
                                item_id, url, title, end_time,
                                max_reserve_proxies=1,
                                notify_time_change=True,
                            )
                            if verification in ('closed', 'ended_normally', 'missing'):
                                break
                            if verification == 'time_changed':
                                # Новое end time уже в БД — этот старый trigger не отправляем.
                                break

                        # Запись могла быть удалена/изменена verification worker'ом.
                        with conn.cursor() as cur:
                            cur.execute(
                                "SELECT end_time_utc, " + _reminder_column(minutes) +
                                " FROM auction_reminders WHERE item_id=%s",
                                (item_id,),
                            )
                            current = cur.fetchone()
                        if not current:
                            break
                        current_end, already_sent = current
                        current_end = _ensure_aware_utc(current_end)
                        if already_sent:
                            sent_map[minutes] = True
                            continue
                        if abs((current_end - end_time).total_seconds()) > 5:
                            break

                        now_send = datetime.now(timezone.utc)
                        remaining_send = max(0, (current_end - now_send).total_seconds())
                        if remaining_send <= 0:
                            break

                        safe_title = html_lib.escape(title)
                        label = '1 час' if minutes == 60 else f'{minutes} минут'
                        msg = (
                            "⏰ <b>Напоминание об аукционе eBay UK</b> 🇬🇧\n\n"
                            f"🔔 Плановое напоминание: <b>за {label}</b>\n"
                            f"📦 <b>{safe_title}</b>\n\n"
                            f"⏳ До окончания сейчас: <b>{format_remaining(remaining_send)}</b>\n"
                            f"🕒 Окончание по Киеву: <b>{format_kyiv_datetime(current_end)}</b>"
                        )
                        if send_telegram_message(
                            msg,
                            reply_markup=auction_message_keyboard(item_id, url),
                            disable_preview=True,
                        ):
                            _mark_reminder_sent(conn, item_id, minutes)
                            sent_map[minutes] = True
                            logging.info(f"📨 Auction {item_id}: отправлено reminder {minutes} мин")
                    elif seconds_to_trigger > 0:
                        next_sleep = min(next_sleep, max(1.0, seconds_to_trigger))

                next_sleep = min(next_sleep, max(1.0, remaining))

            _delete_expired_auctions(conn, datetime.now(timezone.utc))

        except Exception as e:
            logging.error(f"Ошибка auction reminder scheduler: {e}", exc_info=True)
            try:
                if conn is not None:
                    conn.rollback()
                    conn.close()
            except Exception:
                pass
            conn = None
            next_sleep = 15

        auction_wakeup_event.wait(timeout=max(1.0, min(float(next_sleep), AUCTION_SCHEDULER_MAX_SLEEP)))
        auction_wakeup_event.clear()


def telegram_listener():
    global is_paused
    logging.info("🔁 Поток слушателя команд Telegram запущен")
    last_update_id = 0
    while True:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
            params = {'offset': last_update_id + 1, 'timeout': 30}
            r = requests.get(url, params=params, timeout=35)
            if r.status_code == 200:
                updates = r.json().get('result', [])
                for update in updates:
                    last_update_id = update['update_id']

                    callback = update.get('callback_query')
                    if callback:
                        handle_telegram_callback(callback)
                        continue

                    message = update.get('message')
                    if message and str(message.get('chat', {}).get('id')) == TELEGRAM_CHAT_ID:
                        text = message.get('text', '').strip()
                        if text == '/stop':
                            is_paused = True
                            send_telegram_message("⏸ Основной мониторинг новых товаров приостановлен. Напоминания об уже сохранённых аукционах продолжают работать. Для возобновления отправьте /start")
                            logging.info("Команда /stop - пауза основного мониторинга")
                        elif text == '/start':
                            is_paused = False
                            reset_connection_watch_after_manual_resume()
                            send_telegram_message("▶ Основной мониторинг продолжает работу")
                            logging.info("Команда /start - продолжение; watchdog-таймер перезапущен")
                        elif text in ('/auctions', '/list'):
                            send_auction_list()
                        elif text.startswith('/delauction'):
                            parts = text.split(maxsplit=1)
                            if len(parts) != 2 or not re.fullmatch(r'\d{9,15}', parts[1].strip()):
                                send_telegram_message("Использование: <code>/delauction НОМЕР_ЛОТА</code>")
                            else:
                                item_id = parts[1].strip()
                                if delete_auction_reminder(item_id):
                                    send_telegram_message(f"🗑 Напоминание для лота <b>{item_id}</b> удалено.")
                                else:
                                    send_telegram_message(f"ℹ️ Активный лот <b>{item_id}</b> в списке не найден.")
                        else:
                            ebay_urls = extract_ebay_urls(text)
                            if ebay_urls:
                                accepted = 0
                                for ebay_url in ebay_urls[:20]:
                                    try:
                                        auction_add_queue.put_nowait(ebay_url)
                                        accepted += 1
                                    except queue.Full:
                                        break
                                if accepted:
                                    send_telegram_message(
                                        f"🔎 Проверяю {'ссылку' if accepted == 1 else f'{accepted} ссылок'} на активный аукцион eBay UK…\n"
                                        "Сохраню только подтверждённые торги с точным временем окончания."
                                    )
                                if accepted < len(ebay_urls[:20]):
                                    send_telegram_message("⚠️ Очередь ссылок заполнена. Остальные ссылки отправьте немного позже.")
            time.sleep(1)
        except Exception as e:
            logging.error(f"Ошибка в слушателе Telegram: {e}")
            time.sleep(5)


# ============ ЗАПРОС К EBAY ============

def _response_title(html):
    if not html:
        return ""
    match = re.search(r'<title[^>]*>(.*?)</title>', html, re.I | re.S)
    if not match:
        return ""
    return re.sub(r'\s+', ' ', match.group(1)).strip()[:160]


def _is_ebay_block_page(response):
    """Определяем именно защитную страницу eBay, а не слово robot в обычном JS."""
    text_lower = (response.text or "").lower()
    title_lower = _response_title(response.text).lower()
    final_url = str(getattr(response, 'url', '') or '').lower()

    if 'pardon our interruption' in text_lower:
        return True, 'pardon our interruption'
    if 'something about your browser made us think you were a bot' in text_lower:
        return True, 'browser made us think you were a bot'
    if 'access denied' in title_lower:
        return True, 'title: access denied'
    if ('captcha' in final_url or 'challenge' in final_url) and 'ebay' in final_url:
        return True, f'challenge url: {final_url[:120]}'

    return False, None


def _looks_like_search_results(html):
    """Не фиксируем пару на случайной HTTP 200 странице без карточек eBay."""
    if not html:
        return False
    lower = html.lower()
    return ('/itm/' in lower) and (
        's-item' in lower or
        's-card' in lower or
        'srp-river-results' in lower
    )


def _create_session(proxy, profile, timeout=None):
    # Session сохраняет cookies и TCP/TLS соединение уже найденного рабочего IP.
    if timeout is None:
        timeout = (FIXED_CONNECT_TIMEOUT, FIXED_READ_TIMEOUT)
    kwargs = {
        'headers': {'Accept-Language': 'en-GB,en;q=0.9'},
        'impersonate': profile['impersonate'],
        'verify': True,
        'timeout': timeout,
        'allow_redirects': True,
        'trust_env': False,
    }
    if proxy:
        kwargs['proxy'] = proxy
    return cffi_requests.Session(**kwargs)


def _make_request(proxy, profile, session=None, timeout=None):
    """
    Возвращает (result, html, session), где result:
      success       - нормальная выдача eBay
      blocked       - HTTP 403 / anti-bot
      rate_limited  - HTTP 429
      proxy_timeout  - connect/read timeout
      proxy_ssl      - SSL certificate / MITM proxy
      proxy_rejected - CONNECT tunnel 400/405/500/aborted: endpoint не годится для HTTPS
      proxy_error    - SOCKS/reset/прочая transport ошибка
      profile_error - неподдерживаемый fingerprint
      http_error    - прочий HTTP/неожиданный ответ
    """
    if timeout is None:
        timeout = (FIXED_CONNECT_TIMEOUT, FIXED_READ_TIMEOUT)

    own_session = session is None
    if session is None:
        try:
            session = _create_session(proxy, profile, timeout=timeout)
        except Exception as e:
            logging.error(f"Не удалось создать session для {profile['name']}: {e}")
            return 'profile_error', None, None

    try:
        # Параметр request() переопределяет timeout Session — это позволяет
        # discovery быстро отбрасывать медленные proxy, не затрагивая fixed session.
        response = session.get(EBAY_SEARCH_URL, timeout=timeout)

        title = _response_title(response.text)
        final_url = str(getattr(response, 'url', '') or '')
        body_len = len(response.content or b'')

        logging.info(
            f"🌐 eBay ответ: HTTP {response.status_code}, bytes={body_len}, "
            f"title={title!r}, final_url={final_url[:220]}"
        )

        if response.status_code == 200:
            blocked, reason = _is_ebay_block_page(response)
            if blocked:
                logging.warning(
                    f"🚫 ПОДТВЕРЖДЁННАЯ защита eBay ({reason}) "
                    f"для прокси {proxy}, профиль {profile['name']}"
                )
                if own_session:
                    close_session(session)
                return 'blocked', None, None if own_session else session

            if not _looks_like_search_results(response.text):
                logging.warning(
                    f"⚠️ HTTP 200, но выдача eBay не распознана "
                    f"(bytes={body_len}, title={title!r})"
                )
                if own_session:
                    close_session(session)
                return 'http_error', None, None if own_session else session

            logging.info(f"✅ УСПЕШНО c прокси {proxy}, профиль {profile['name']}")
            return 'success', response.text, session

        if response.status_code == 403:
            logging.warning(f"🚫 eBay HTTP 403 для прокси {proxy}, профиль {profile['name']}")
            if own_session:
                close_session(session)
            return 'blocked', None, None if own_session else session

        if response.status_code == 429:
            logging.warning(f"⏳ eBay HTTP 429 для прокси {proxy}, профиль {profile['name']}")
            if own_session:
                close_session(session)
            return 'rate_limited', None, None if own_session else session

        if response.status_code == 407:
            logging.warning(f"🔐 Прокси требует авторизацию: {proxy}")
            if own_session:
                close_session(session)
            return 'proxy_error', None, None if own_session else session

        logging.warning(
            f"⚠️ НЕУДАЧА: HTTP {response.status_code} "
            f"для прокси {proxy}, профиль {profile['name']}"
        )
        if own_session:
            close_session(session)
        return 'http_error', None, None if own_session else session

    except Exception as e:
        error_msg = str(e)
        low = error_msg.lower()
        logging.error(
            f"❌ ОШИБКА для прокси {proxy}, "
            f"профиль {profile['name']}: {error_msg}"
        )

        if 'not supported' in low:
            disable_profile(profile['name'])
            if own_session:
                close_session(session)
            return 'profile_error', None, None

        if own_session:
            close_session(session)

        if 'curl: (28)' in low or 'timed out' in low:
            return 'proxy_timeout', None, None if own_session else session
        if 'curl: (60)' in low or 'certificate' in low or 'self signed' in low:
            return 'proxy_ssl', None, None if own_session else session
        if (
            'connect tunnel failed' in low or
            'proxy connect aborted' in low or
            re.search(r'connect[^\n]*(?:response )?(?:400|405|500|501|502|503)', low)
        ):
            return 'proxy_rejected', None, None if own_session else session
        return 'proxy_error', None, None if own_session else session


def _probe_proxy(proxy, profile, timeout):
    """Одна discovery-проверка. У каждой задачи своя Session — thread-safe."""
    return _make_request(
        proxy,
        profile,
        session=None,
        timeout=timeout,
    )


def _cleanup_late_probe_future(future, proxy):
    """Закрывает/учитывает probe, который уже был в полёте, когда другой proxy победил."""
    if future.cancelled():
        return
    try:
        result, _, session = future.result()
    except Exception as e:
        logging.debug(f"Фоновый probe {proxy} завершился исключением: {e}")
        return

    try:
        if result == 'success':
            proxy_manager.mark_success(proxy)
            logging.info(f"🟢 Запомнен запасной успешный proxy {proxy} (late probe)")
        elif result != 'profile_error':
            proxy_manager.mark_failure(proxy, result, reason=f'{result} (late probe)')
    finally:
        close_session(session)


def fetch_ebay_html_with_fixed_pair():
    global fixed_proxy, fixed_profile, fixed_session

    # 1) Максимально долго держим реально рабочую session.
    if fixed_proxy is not None and fixed_profile is not None:
        logging.info(
            f"🔁 Используем зафиксированную пару: "
            f"proxy {fixed_proxy}, профиль {fixed_profile['name']}"
        )

        old_proxy = fixed_proxy
        old_profile = fixed_profile
        result, html, returned_session = _make_request(
            old_proxy,
            old_profile,
            session=fixed_session,
            timeout=(FIXED_CONNECT_TIMEOUT, FIXED_READ_TIMEOUT),
        )

        if result == 'success':
            fixed_session = returned_session
            proxy_manager.mark_success(old_proxy)
            record_ebay_success()
            return html

        # Частая реальная ситуация: умерло только старое TCP/TLS соединение Session,
        # а сам proxy всё ещё жив. Для transport-ошибки один раз создаём НОВУЮ session
        # на том же успешном IP, прежде чем ротировать proxy.
        if result in ('proxy_timeout', 'proxy_error') and proxy_manager.is_recent_good(old_proxy):
            logging.info(
                f"♻️ Недавно успешный proxy {old_proxy}: "
                "пересоздаём session и даём один быстрый шанс"
            )
            close_session(fixed_session)
            fixed_session = None
            time.sleep(random.uniform(0.4, 0.8))

            retry_result, retry_html, retry_session = _make_request(
                old_proxy,
                old_profile,
                session=None,
                timeout=(FIXED_RECOVERY_CONNECT_TIMEOUT, FIXED_RECOVERY_READ_TIMEOUT),
            )
            if retry_result == 'success':
                fixed_proxy = old_proxy
                fixed_profile = old_profile
                fixed_session = retry_session
                proxy_manager.mark_success(old_proxy)
                record_ebay_success()
                logging.info("✅ Proxy восстановился после пересоздания session")
                return retry_html

            close_session(retry_session)
            result = retry_result

        close_session(fixed_session)
        fixed_session = None
        fixed_proxy = None
        fixed_profile = None

        if result == 'profile_error':
            logging.warning(
                f"Профиль {old_profile['name']} недоступен; proxy {old_proxy} не штрафуем"
            )
        else:
            proxy_manager.mark_failure(
                old_proxy,
                result,
                reason=f'{result} на ранее успешной fixed session',
            )

        logging.info("Ищем новую рабочую пару...")

    # 2) Discovery: 3 уникальных IP параллельно. Это главное ускорение.
    started = time.monotonic()
    tried_hosts = set()
    attempts = 0

    while attempts < MAX_SEARCH_ATTEMPTS:
        elapsed = time.monotonic() - started
        if elapsed >= SEARCH_TIME_BUDGET:
            logging.warning(
                f"⏱ Достигнут лимит discovery {SEARCH_TIME_BUDGET} сек.; "
                "останавливаем этот цикл"
            )
            break

        profile = get_preferred_profile()
        if profile is None:
            logging.error("Нет поддерживаемого browser-профиля curl_cffi")
            return None

        remaining = MAX_SEARCH_ATTEMPTS - attempts
        batch_size = min(PROBE_CONCURRENCY, remaining)
        batch = proxy_manager.get_candidate_batch(batch_size, tried_hosts=tried_hosts)

        if not batch:
            # Возможно, истёк короткий cooldown у старого good proxy или ProxyScrape
            # уже обновил пул. Один раз очищаем только per-cycle запрет повторных IP,
            # но не реальные cooldown.
            if tried_hosts:
                logging.info("♻️ Новых уникальных IP сейчас нет; обновляем pool и пересчитываем кандидатов")
                proxy_manager.refresh_proxies(force=True)
                tried_hosts.clear()
                time.sleep(random.uniform(0.8, 1.4))
                batch = proxy_manager.get_candidate_batch(batch_size, tried_hosts=tried_hosts)

            if not batch:
                logging.warning("Нет доступных proxy; короткая пауза")
                time.sleep(random.uniform(2.0, 3.0))
                continue

        for proxy in batch:
            tried_hosts.add(_proxy_host(proxy))
            attempts += 1
            logging.info(
                f"🔍 Probe {attempts}/{MAX_SEARCH_ATTEMPTS}: proxy {proxy}, "
                f"профиль {profile['name']}"
            )

        # Первая половина discovery — строгий fast lane. Если за ~45 сек победителя
        # нет, немного расширяем timeout: это лучше, чем сразу держать каждый плохой
        # batch по 18+ секунд из-за одного медленно отдающего HTML proxy.
        elapsed_now = time.monotonic() - started
        if elapsed_now < PROBE_FALLBACK_AFTER:
            probe_timeout = (PROBE_CONNECT_TIMEOUT, PROBE_READ_TIMEOUT)
        else:
            probe_timeout = (PROBE_FALLBACK_CONNECT_TIMEOUT, PROBE_FALLBACK_READ_TIMEOUT)

        # Первый успешный ответ выигрывает сразу. Раньше мы ждали весь batch и
        # могли потерять ещё 8-15 сек из-за соседнего "полуживого" proxy, хотя
        # рабочий HTTP 200 уже был получен. Уже стартовавшие 1-2 probe не бросаем:
        # они тихо завершаются, Session закрываются callback'ом, а их результат
        # попадает в health-score как backup/ошибка.
        executor = ThreadPoolExecutor(max_workers=len(batch), thread_name_prefix='proxy-probe')
        future_to_proxy = {
            executor.submit(_probe_proxy, proxy, profile, probe_timeout): proxy
            for proxy in batch
        }
        processed = set()
        winner = None

        for future in as_completed(future_to_proxy):
            proxy = future_to_proxy[future]
            processed.add(future)
            try:
                result, html, session = future.result()
            except Exception as e:
                logging.error(f"Ошибка probe worker для {proxy}: {e}")
                result, html, session = 'proxy_error', None, None

            if result == 'success':
                proxy_manager.mark_success(proxy)
                winner = (proxy, html, session)
                break
            elif result == 'profile_error':
                close_session(session)
            else:
                close_session(session)
                proxy_manager.mark_failure(proxy, result, reason=result)

        if winner is not None:
            # Не ждём медленных соседей победителя. Те, что уже работают, получат
            # callback; ещё не стартовавшие (редко при batch<=workers) отменяем.
            for future, proxy in future_to_proxy.items():
                if future in processed:
                    continue
                if future.done():
                    _cleanup_late_probe_future(future, proxy)
                elif not future.cancel():
                    future.add_done_callback(
                        lambda f, p=proxy: _cleanup_late_probe_future(f, p)
                    )
            executor.shutdown(wait=False, cancel_futures=True)

            winner_proxy, winner_html, winner_session = winner
            fixed_proxy = winner_proxy
            fixed_profile = profile
            fixed_session = winner_session
            record_ebay_success()
            logging.info(
                f"✅ Найдена рабочая пара: proxy {winner_proxy}, профиль {profile['name']}; "
                f"проверено {attempts} proxy за {time.monotonic() - started:.1f} сек."
            )
            return winner_html

        # Победителя нет — все futures уже завершились через as_completed().
        executor.shutdown(wait=True)

        # Не устраиваем серию мгновенных batch после быстрых 403/CONNECT 400.
        time.sleep(random.uniform(0.45, 0.9))

    logging.error(
        f"❌ В этом цикле рабочий proxy не найден: "
        f"проверено {attempts}, время {time.monotonic() - started:.1f} сек."
    )
    return None


def fetch_ebay_html_with_retry():
    return fetch_ebay_html_with_fixed_pair()

# ============ ПАРСИНГ ============
def extract_item_id(url):
    if not url or '/itm/' not in url:
        return None
    try:
        return url.split('/itm/')[1].split('?')[0]
    except IndexError:
        return None

def clean_title(title):
    if not title: return ""
    title = re.sub(r'(?i)new\s*listing', '', title)
    title = re.sub(r'(?i)\blisting\b', '', title)
    title = re.sub(r'(?i)\bnew\b', '', title)
    title = re.sub(r'[^\w\s£€$]', ' ', title)
    title = re.sub(r'\s+', ' ', title).strip()
    return title

def is_gbp_price(text):
    if not text: return False
    if re.search(r'£|\bGBP\b', text, re.I):
        return True
    if re.search(r'[$€]|USD|EUR', text, re.I):
        return False
    return re.search(r'\d', text) is not None

def extract_range_price(card):
    price_spans = card.select('span.s-card__price, span.s-item__price, [class*="price"]')
    for i in range(len(price_spans) - 2):
        first = price_spans[i].get_text(strip=True)
        middle = price_spans[i+1].get_text(strip=True).lower()
        third = price_spans[i+2].get_text(strip=True)
        if 'to' in middle and re.search(r'[£€$]', first) and re.search(r'[£€$]', third):
            return f"{first} до {third}"
    to_elem = card.find(string=re.compile(r'\bto\b', re.I))
    if to_elem:
        parent = to_elem.find_parent()
        if parent:
            prev_price = None
            next_price = None
            for sibling in parent.previous_siblings:
                if hasattr(sibling, 'get_text'):
                    txt = sibling.get_text(strip=True)
                    if re.search(r'[£€$]\s*[\d,]+\.?\d*', txt):
                        prev_price = txt
                        break
            for sibling in parent.next_siblings:
                if hasattr(sibling, 'get_text'):
                    txt = sibling.get_text(strip=True)
                    if re.search(r'[£€$]\s*[\d,]+\.?\d*', txt):
                        next_price = txt
                        break
            if prev_price and next_price:
                return f"{prev_price} до {next_price}"
    return None

def extract_price_jsonld(card, url=None, soup=None):
    range_price = extract_range_price(card)
    if range_price:
        return range_price

    script = card.find('script', type='application/ld+json')
    candidates = []
    if script and script.string:
        try:
            data = json.loads(script.string)
            if isinstance(data, dict):
                offers = data.get('offers')
                if isinstance(offers, dict):
                    price = offers.get('price')
                    currency = offers.get('priceCurrency', '')
                    if price and price != '0':
                        candidates.append((price, currency))
                elif isinstance(offers, list):
                    for off in offers:
                        price = off.get('price')
                        currency = off.get('priceCurrency', '')
                        if price and price != '0':
                            candidates.append((price, currency))
        except:
            pass
    if soup and url:
        for script in soup.find_all('script', type='application/ld+json'):
            if not script.string:
                continue
            try:
                data = json.loads(script.string)
                if isinstance(data, dict) and data.get('url') == url:
                    offers = data.get('offers')
                    if isinstance(offers, dict):
                        price = offers.get('price')
                        currency = offers.get('priceCurrency', '')
                        if price and price != '0':
                            candidates.append((price, currency))
            except:
                continue
    for price, curr in candidates:
        if curr == 'GBP' or (curr == '' and str(price).startswith('£')):
            return f"£{price}"
    for price, curr in candidates:
        if curr:
            return f"{curr} {price}"
        else:
            return str(price)
    return None

def extract_price_css(card):
    range_price = extract_range_price(card)
    if range_price:
        return range_price

    candidates = []
    selectors = ['span.s-item__price', '[data-testid="item-price"]', '.s-item__detail .s-item__price']
    for sel in selectors:
        for elem in card.select(sel):
            text = elem.get_text(strip=True)
            if text:
                candidates.append(text)
    for elem in card.select('[class*="price"]'):
        text = elem.get_text(strip=True)
        if text:
            candidates.append(text)
    for cand in candidates:
        if is_gbp_price(cand):
            parts = cand.split()
            for p in parts:
                if is_gbp_price(p):
                    return p
            return cand
    if candidates:
        return candidates[0]
    return None

def extract_shipping(card, item_price=None, range_prices=None):
    for elem in card.select('.su-styled-text.secondary.large'):
        text = elem.get_text(strip=True)
        text_lower = text.lower()
        if 'delivery' in text_lower or 'shipping' in text_lower:
            if 'free' in text_lower:
                return "Бесплатно"
            match = re.search(r'([+]\s*)?([£€$]\s*[\d,]+\.?\d*)', text)
            if match:
                price_candidate = match.group(2)
                if range_prices and any(price_candidate in p or p in price_candidate for p in range_prices):
                    pass
                elif item_price and price_candidate == item_price:
                    pass
                else:
                    return price_candidate
            if len(text) > 3:
                return text

    html_lower = str(card).lower()
    if re.search(r'free\s+delivery', html_lower) or re.search(r'free\s+shipping', html_lower):
        return "Бесплатно"

    script = card.find('script', type='application/ld+json')
    if script and script.string:
        try:
            data = json.loads(script.string)
            if isinstance(data, dict):
                offers = data.get('offers')
                if isinstance(offers, dict):
                    shipping = offers.get('shippingCost')
                    if shipping is not None:
                        if shipping == 0 or str(shipping) == '0':
                            return "Бесплатно"
                        if isinstance(shipping, (int, float)):
                            currency = offers.get('priceCurrency', '')
                            amount = f"{currency} {shipping}" if currency else str(shipping)
                            if range_prices and any(amount in p or p in amount for p in range_prices):
                                pass
                            elif item_price and str(shipping) == str(item_price) and currency == 'GBP':
                                pass
                            else:
                                return amount
                elif isinstance(offers, list) and len(offers) > 0:
                    first = offers[0]
                    shipping = first.get('shippingCost')
                    if shipping is not None:
                        if shipping == 0 or str(shipping) == '0':
                            return "Бесплатно"
                        if isinstance(shipping, (int, float)):
                            currency = first.get('priceCurrency', '')
                            amount = f"{currency} {shipping}" if currency else str(shipping)
                            if range_prices and any(amount in p or p in amount for p in range_prices):
                                pass
                            elif item_price and str(shipping) == str(item_price) and currency == 'GBP':
                                pass
                            else:
                                return amount
        except:
            pass

    shipping_selectors = [
        'span.s-item__shipping', 'div.s-item__shipping',
        'span.s-item__logisticsCost', 'span.s-item__delivery',
        'span.su-styled-text', '.su-styled-text.secondary.large',
        '[class*="shippingCost"]', '[class*="delivery"]',
        '.s-item__detail--shipping', '.s-item__delivery-costs'
    ]
    for sel in shipping_selectors:
        for elem in card.select(sel):
            text = elem.get_text(strip=True)
            text = re.sub(r'\s+', ' ', text)
            if not text:
                continue
            if re.search(r'(?i)(buy it now|best offer|make offer|watch|add to cart)', text):
                continue
            if re.search(r'\bfree\b', text.lower()):
                return "Бесплатно"
            match = re.search(r'([+]\s*)?([£€$]\s*[\d,]+\.?\d*)', text)
            if match:
                price_candidate = match.group(2)
                if range_prices and any(price_candidate in p or p in price_candidate for p in range_prices):
                    continue
                if item_price and price_candidate == item_price:
                    continue
                return price_candidate
            if re.search(r'(?i)(delivery|shipping)', text) and len(text) > 5:
                if len(text) > 10 or re.search(r'\d', text):
                    return text

    match = re.search(r'\+?\s*([£€$]\s*[\d,]+\.?\d*)\s*(delivery|shipping)', html_lower)
    if match:
        pc = match.group(1)
        if range_prices and any(pc in p or p in pc for p in range_prices):
            pass
        else:
            if not (item_price and pc == item_price):
                return pc
    match = re.search(r'shipping:\s*([£€$]\s*[\d,]+\.?\d*)', html_lower)
    if match:
        pc = match.group(1)
        if range_prices and any(pc in p or p in pc for p in range_prices):
            pass
        else:
            if not (item_price and pc == item_price):
                return pc
    match = re.search(r'(delivery in\s+\d+[-\s]*\d*\s*(days?|weeks?|business days?|working days?))', html_lower)
    if match:
        return match.group(1).strip()
    match = re.search(r'(delivery time\s*:\s*[\w\s\d-]+)', html_lower)
    if match:
        return match.group(1).strip()
    match = re.search(r'(shipping in\s+\d+[-\s]*\d*\s*(days?|weeks?))', html_lower)
    if match:
        return match.group(1).strip()
    return None

def extract_best_offer(card):
    text = card.get_text()
    if re.search(r'or\s+best\s+offer', text, re.I):
        return True
    best_offer_selectors = [
        '.s-item__best-offer', '.s-item__detail--best-offer', '.s-item__bonus',
        '[class*="bestOffer"]', '[class*="best-offer"]'
    ]
    for sel in best_offer_selectors:
        if card.select_one(sel):
            return True
    if card.select_one('[data-best-offer="true"]'):
        return True
    return False

def extract_auction(card):
    if card.select_one('.su-styled-text.secondary.large'):
        elem = card.select_one('.su-styled-text.secondary.large')
        text = elem.get_text(strip=True).lower()
        if 'bid' in text:
            return True
    auction_selectors = [
        '.s-item__bid-count', '.s-item__bids', '[class*="bidCount"]',
        '[class*="bids"]', '[class*="bid-count"]', '.vi-bidrev',
        '.s-item__detail--bid-count', '[data-testid="bid-count"]', '.bidCount',
        'span.bids', '.s-item__auction', '.auction-badge', '.s-item__bid-count__text',
        '.bid-count', '.bids-count'
    ]
    for sel in auction_selectors:
        found = card.select_one(sel)
        if found:
            txt = found.get_text(strip=True).lower()
            if 'bid' in txt or txt.isdigit():
                return True
    full_text = card.get_text().lower()
    if re.search(r'\d+\s+bids?\b', full_text):
        return True
    if re.search(r'\bplace\s+bid\b', full_text):
        return True
    if 'bids' in full_text and 'buy it now' not in full_text:
        return True
    if card.select_one('a[href*="bid"]'):
        return True
    if card.select_one('[data-auction="true"]'):
        return True
    if card.select_one('[data-testid*="auction"]'):
        return True
    return False

def extract_buy_it_now_info(card):
    buy_it_now_elem = card.find(string=re.compile(r'Buy It Now', re.I))
    if not buy_it_now_elem:
        return False, None
    price_spans = card.select('span.s-card__price, span.s-item__price, [class*="price"]')
    if len(price_spans) >= 2:
        second_price = price_spans[1].get_text(strip=True)
        if re.search(r'[£€$]', second_price):
            return True, second_price
    parent = buy_it_now_elem.find_parent()
    if parent:
        price_elem = parent.find_next('span', class_=re.compile(r'price'))
        if price_elem:
            price_text = price_elem.get_text(strip=True)
            if re.search(r'[£€$]', price_text):
                return True, price_text
    return True, None

def parse_ebay_listings(html, max_items=MAX_ITEMS):
    if not html:
        return {}
    soup = BeautifulSoup(html, 'html.parser')
    cards = soup.select('li.s-item')
    if not cards:
        cards = soup.select('.s-item')
    if not cards:
        return parse_ebay_listings_fallback(soup, max_items)
    items = {}
    processed = 0
    for card in cards:
        if processed >= max_items:
            break
        link = card.select_one('a.s-item__link')
        if not link:
            continue
        url = link.get('href')
        if not url or '/itm/' not in url:
            continue
        if url.startswith('/'):
            url = 'https://www.ebay.co.uk' + url
        item_id = extract_item_id(url)
        if not item_id:
            continue
        title_elem = (card.select_one('div.s-item__title span[role="heading"]') or
                      card.select_one('span[role="heading"]') or
                      card.select_one('div.s-item__title') or link)
        title = clean_title(title_elem.get_text(strip=True) if title_elem else '')
        if not title:
            title = clean_title(link.get_text(strip=True))
            if not title:
                continue
        price = extract_price_jsonld(card, url, soup) or extract_price_css(card)
        range_prices = []
        if price and ' до ' in price:
            parts = price.split(' до ')
            if len(parts) == 2:
                range_prices = [parts[0].strip(), parts[1].strip()]
        if price and not is_gbp_price(price):
            price = None
        shipping = extract_shipping(card, item_price=price, range_prices=range_prices)
        best_offer = extract_best_offer(card)
        auction = extract_auction(card)
        has_bin, bin_price = extract_buy_it_now_info(card)
        items[item_id] = {
            'url': url,
            'title': title,
            'price': price,
            'shipping': shipping,
            'best_offer': best_offer,
            'auction': auction,
            'has_buy_it_now': has_bin,
            'buy_it_now_price': bin_price
        }
        processed += 1
    logging.info(f"Обработано товаров: {len(items)}")
    return items

def parse_ebay_listings_fallback(soup, max_items):
    items = {}
    links = soup.find_all('a', href=True)
    itm_links = [link for link in links if '/itm/' in link['href']]
    itm_links = itm_links[:max_items]
    for link in itm_links:
        url = link.get('href')
        if url.startswith('/'):
            url = 'https://www.ebay.co.uk' + url
        item_id = extract_item_id(url)
        if not item_id:
            continue
        title = clean_title(link.get_text(strip=True))
        if not title:
            continue
        price = None
        shipping = None
        best_offer = False
        auction = False
        has_bin = False
        bin_price = None
        parent = link.parent
        for _ in range(5):
            if parent:
                price = extract_price_jsonld(parent, url) or extract_price_css(parent)
                if price and not is_gbp_price(price):
                    price = None
                shipping = extract_shipping(parent, item_price=price)
                best_offer = extract_best_offer(parent)
                auction = extract_auction(parent)
                has_bin, bin_price = extract_buy_it_now_info(parent)
                if price or shipping or best_offer or auction or has_bin:
                    break
                parent = parent.parent
        items[item_id] = {
            'url': url,
            'title': title,
            'price': price,
            'shipping': shipping,
            'best_offer': best_offer,
            'auction': auction,
            'has_buy_it_now': has_bin,
            'buy_it_now_price': bin_price
        }
    return items

def perform_initial_snapshot():
    logging.info("Начальный снимок...")
    html = fetch_ebay_html_with_retry()
    if not html:
        return False
    items = parse_ebay_listings(html, max_items=50)
    if not items:
        return False
    add_seen_ids_batch(list(items.keys()))
    logging.info(f"Снимок: {len(items)} товаров")
    return True

def calculate_total_price(price_str, shipping_str, buy_it_now_price_str=None, is_auction=False):
    if not price_str or price_str == "Цена не указана (не GBP)" or "до" in price_str:
        return None

    price_num = None
    if is_auction and buy_it_now_price_str:
        match = re.search(r'([\d,]+\.?\d*)', buy_it_now_price_str.replace(',', ''))
        if match:
            price_num = float(match.group(1))
    if price_num is None:
        match = re.search(r'([\d,]+\.?\d*)', price_str.replace(',', ''))
        if match:
            price_num = float(match.group(1))
    if price_num is None:
        return None

    shipping_num = 0.0
    if shipping_str and shipping_str != "Бесплатно" and shipping_str != "не указана" and shipping_str is not None:
        match = re.search(r'([\d,]+\.?\d*)', shipping_str.replace(',', ''))
        if match:
            shipping_num = float(match.group(1))

    total_gbp = price_num + shipping_num
    total_uah = int(total_gbp * GBP_TO_UAH) + EXTRA_DELIVERY_COST
    return total_uah

def check_and_send_new_items():
    seen = get_seen_ids()
    logging.info(f"В базе {len(seen)} товаров")
    html = fetch_ebay_html_with_retry()
    if not html:
        logging.warning("Не удалось загрузить страницу, проверка пропущена")
        return False
    current = parse_ebay_listings(html)
    new = []
    for item_id, data in current.items():
        if item_id not in seen:
            new.append({'id': item_id, **data})
            logging.info(f"НОВЫЙ: {data['title'][:50]}... цена: {data['price']}, доставка: {data.get('shipping')}, best_offer: {data.get('best_offer')}, auction: {data.get('auction')}, has_buy_it_now: {data.get('has_buy_it_now')}")
    if new:
        for item in new:
            msg = f"🇬🇧 <b>НОВЫЙ ТОВАР Англия</b> 🇬🇧\n\n<b>{item['title']}</b>\n\n"
            if item['price']:
                msg += f"💰 Цена: {item['price']}\n"
            else:
                msg += f"💰 Цена не указана (не GBP)\n"
            if item['shipping']:
                msg += f"🚚 Доставка: {item['shipping']}\n"
            else:
                msg += f"🚚 Доставка: не указана\n"
            if item.get('best_offer', False):
                msg += f"✅ Сделать предложение (Best Offer)\n"
            if item.get('auction', False):
                if item.get('has_buy_it_now', False) and item.get('buy_it_now_price'):
                    msg += f"⏰ Аукцион / Buy It Now цена: {item['buy_it_now_price']}\n"
                elif item.get('has_buy_it_now', False):
                    msg += f"⏰ Аукцион / Buy It Now\n"
                else:
                    msg += f"⏰ Аукцион\n"
            if not item.get('auction', False) or (item.get('auction', False) and item.get('has_buy_it_now', False)):
                total = calculate_total_price(
                    item['price'],
                    item['shipping'],
                    item.get('buy_it_now_price'),
                    is_auction=item.get('auction', False)
                )
                if total is not None:
                    msg += f"\nЗа все (с доставкой в Украину): <b>{total}грн</b>"
            msg += f"\n\n🔗 <a href='{item['url']}'>Ссылка на товар</a>"
            send_telegram_message(msg)
            add_seen_ids_batch([item['id']])
            time.sleep(1)
    else:
        logging.info("Новых нет")
    return True

def bot_worker():
    global is_paused
    logging.info("🤖 Бот-воркер запущен")
    init_db()
    db_ready_event.set()
    if is_db_empty():
        if not perform_initial_snapshot():
            send_telegram_message("❌ Ошибка инициализации")
            return
        send_telegram_message("✅ Бот запущен, начальный снимок сделан")
    else:
        send_telegram_message("✅ Бот перезапущен")
    while True:
        if is_paused:
            time.sleep(2)
            continue
        try:
            success = check_and_send_new_items()
            if success:
                wait = random.uniform(40, 52)
                logging.info(f"✅ Успешная проверка. Следующая через {wait:.0f} секунд.")
            else:
                # После неудачного поискового цикла не начинаем новый burst через 2 сек.
                # За это время ProxyScrape успеет обновить часть бесплатного пула.
                wait = random.uniform(25, 40)
                logging.info(f"⚠️ Ошибка при проверке. Повтор через {wait:.1f} секунд.")
            time.sleep(wait)
        except Exception as e:
            logging.error(f"Ошибка в основном цикле: {e}", exc_info=True)
            time.sleep(5)

@app.route('/')
def index():
    return "eBay бот работает (Великобритания, adaptive parallel UK v5)"

@app.route('/health')
def health():
    return "OK", 200

if __name__ == "__main__":
    send_telegram_message(
        "🚀 Бот запущен (Великобритания, adaptive parallel UK v5).\n"
        "Команды: /stop /start /list (/auctions) /delauction НОМЕР_ЛОТА\n"
        "Можно просто отправить ссылку на eBay-аукцион — бот сохранит точное время и напомнит заранее.",
        reply_markup=auction_list_only_keyboard(),
    )
    threading.Thread(target=telegram_listener, daemon=True).start()
    threading.Thread(target=connection_watchdog, daemon=True).start()
    threading.Thread(target=auction_link_worker, daemon=True).start()
    threading.Thread(target=auction_reminder_worker, daemon=True).start()
    threading.Thread(target=auction_status_worker, daemon=True).start()
    worker_thread = threading.Thread(target=bot_worker, daemon=False)
    worker_thread.start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
