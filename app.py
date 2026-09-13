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
import hashlib
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import requests
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from bs4 import BeautifulSoup, Tag
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
# Успешные проверки: нижняя граница берётся из Render.
# При CHECK_INTERVAL=15 фактическая пауза будет 15-27 сек.
# Ниже 15 сек. код не позволяет опускаться: это новый безопасный нижний предел,
# а случайный jitter +12 сек. сохраняет непостоянный ритм запросов к eBay.
CHECK_INTERVAL = max(15, int(os.getenv("CHECK_INTERVAL", "40")))
DATABASE_URL = os.getenv("DATABASE_URL")
PROXY_LIST_URL = os.getenv("PROXY_LIST")
# ProxyScrape обновляет бесплатный список примерно раз в минуту.
PROXY_REFRESH_INTERVAL = 60

# PostgreSQL/Aiven: короткие сетевые/SQL timeout защищают worker-ы от долгого зависания
# при временном сетевом сбое. Никаких ручных изменений в Aiven не требуется.
DB_CONNECT_TIMEOUT = max(3, int(os.getenv("DB_CONNECT_TIMEOUT", "5")))
DB_STATEMENT_TIMEOUT_MS = max(5000, int(os.getenv("DB_STATEMENT_TIMEOUT_MS", "10000")))
DB_LOCK_TIMEOUT_MS = max(1000, int(os.getenv("DB_LOCK_TIMEOUT_MS", "3000")))

# Render делает zero-downtime deploy: новая и старая копия некоторое время живут одновременно.
# Один session-level advisory lock в PostgreSQL гарантирует, что фоновые worker-ы активны
# только в ОДНОЙ копии приложения. Ключ стабильный и не требует таблицы/настроек в Aiven.
LEADER_LOCK_KEY = int.from_bytes(
    hashlib.sha256(b"ebay-uk-telegram-bot-single-leader-v6").digest()[:8],
    byteorder="big", signed=True,
)
LEADER_RETRY_INTERVAL = max(2, int(os.getenv("LEADER_RETRY_INTERVAL", "5")))
LEADER_HEALTH_INTERVAL = max(5, int(os.getenv("LEADER_HEALTH_INTERVAL", "10")))

# Discovery запускается только когда уже нет рабочей fixed-session. Сначала оставляем
# привычные 3 параллельные проверки; если за короткое время победителя нет, повышаем
# только до 4. Это заметно ускоряет аварийный поиск, не превращая его в агрессивный burst.
PROBE_CONCURRENCY = max(1, min(int(os.getenv("PROBE_CONCURRENCY", "3")), 4))
PROBE_ESCALATED_CONCURRENCY = max(
    PROBE_CONCURRENCY,
    min(int(os.getenv("PROBE_ESCALATED_CONCURRENCY", "4")), 4),
)
PROBE_ESCALATE_AFTER = max(5.0, float(os.getenv("PROBE_ESCALATE_AFTER", "15")))
PROBE_CONNECT_TIMEOUT = float(os.getenv("PROBE_CONNECT_TIMEOUT", "3.5"))
# В старой версии после 45 сек. timeout искусственно увеличивался до 4.5/12 и один
# полуживой proxy мог держать целый batch 11+ секунд. При сотнях кандидатов выгоднее
# продолжать быстро перебирать пул тем же строгим timeout и не тормозить вторую половину.
PROBE_READ_TIMEOUT = float(os.getenv("PROBE_READ_TIMEOUT", "8"))
PROBE_BATCH_PAUSE_MIN = max(0.10, float(os.getenv("PROBE_BATCH_PAUSE_MIN", "0.25")))
PROBE_BATCH_PAUSE_MAX = max(PROBE_BATCH_PAUSE_MIN, float(os.getenv("PROBE_BATCH_PAUSE_MAX", "0.55")))

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
# Scheduler обычно просыпается не реже раза в минуту. Если Render был перезапущен и
# точный порог уже прошёл, старые 60/30/10-минутные сообщения не отправляем пачкой:
# посылаем только одно наиболее актуальное напоминание.
AUCTION_REMINDER_LATE_GRACE = max(30, int(os.getenv("AUCTION_REMINDER_LATE_GRACE", "90")))
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

# Один аварийный discovery-цикл не должен держать монитор без свежего pool слишком долго.
# За 75 сек. с adaptive 3->4 workers успеваем проверить значительно больше адресов,
# затем делаем лишь короткую паузу, обновляем ProxyScrape и продолжаем поиск.
SEARCH_TIME_BUDGET = max(45, int(os.getenv("SEARCH_TIME_BUDGET", "75")))
MAX_SEARCH_ATTEMPTS = max(60, int(os.getenv("MAX_SEARCH_ATTEMPTS", "120")))
FAILED_SEARCH_RETRY_MIN = max(3.0, float(os.getenv("FAILED_SEARCH_RETRY_MIN", "6")))
FAILED_SEARCH_RETRY_MAX = max(FAILED_SEARCH_RETRY_MIN, float(os.getenv("FAILED_SEARCH_RETRY_MAX", "10")))

# Успешные proxy запоминаем и относим к ним мягче после единичного сбоя.
GOOD_PROXY_MEMORY = 60 * 60
SSL_PROXY_COOLDOWN = 60 * 60

# Страница eBay запрашивается с _ipg=60; обрабатываем все 60 результатов первой страницы.
# Это снижает риск пропустить товар при всплеске новых объявлений между проверками.
MAX_ITEMS = 60
RETRY_DELAY = 2
GBP_TO_UAH = 60
EXTRA_DELIVERY_COST = 120

def normalize_ebay_search_url(raw_url):
    """
    Убираем конфликтующие/дублированные параметры из URL Render.
    Для ebay.co.uk: LH_PrefLoc=1 = UK Only.
    _ipg=60 достаточно: код анализирует всю первую страницу (до MAX_ITEMS=60),
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
auction_reminder_wakeup_event = threading.Event()
auction_status_wakeup_event = threading.Event()
db_ready_event = threading.Event()
leader_active_event = threading.Event()

# Счётчик seen_items загружается из PostgreSQL один раз при старте leader-копии,
# а затем увеличивается только на реально вставленное число новых item_id.
# Так в логах снова видно общее количество товаров без SELECT COUNT(*) каждые 15-27 сек.
seen_count_lock = threading.Lock()
seen_count_cache = None

def wake_auction_workers():
    # Раздельные Event исключают редкую гонку, когда один worker очищает общий сигнал,
    # предназначенный другому.
    auction_reminder_wakeup_event.set()
    auction_status_wakeup_event.set()

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
        # Недавно успешные адреса остаются приоритетными, но proxy с накопившейся
        # серией 3+ ошибок больше не должен бесконечно вытеснять свежие кандидаты.
        last_ok = self.last_success_at.get(proxy, 0)
        age_ok = now - last_ok if last_ok else 10**9
        recent_bonus = 0.0
        if age_ok <= 5 * 60:
            recent_bonus = 100.0
        elif age_ok <= 30 * 60:
            recent_bonus = 60.0
        elif age_ok <= GOOD_PROXY_MEMORY:
            recent_bonus = 30.0

        streak = self.fail_streak.get(proxy, 0)
        success_bonus = self.success_score.get(proxy, 0) * 8.0
        fail_penalty = streak * 4.0

        # После 3-й подряд ошибки мягко снижаем исторический бонус. Proxy не банится
        # и не исчезает из пула, но свежие адреса получают реальный шанс провериться.
        unstable_penalty = max(0, streak - 2) * 20.0

        # По UK-логам реальные победители чаще HTTP. Это лишь мягкий приоритет,
        # SOCKS5 по-прежнему участвует в поиске.
        scheme_bonus = 1.2 if _proxy_scheme(proxy) in ('http', 'https') else 0.0

        # Давно не пробовавшиеся адреса немного выше только что проверенных.
        idle = now - self.last_used.get(proxy, 0)
        idle_bonus = min(4.0, idle / 60.0) if idle < 10**8 else 4.0

        return (
            recent_bonus + success_bonus + scheme_bonus + idle_bonus
            - fail_penalty - unstable_penalty + random.uniform(0, 2.0)
        )

    def get_candidate_batch(self, batch_size, tried_hosts=None, preferred_scheme=None):
        """Выдаёт несколько proxy с уникальными IP и разумным mix HTTP/SOCKS5.

        Раньше небольшой bonus HTTP приводил к тому, что при большом пуле первые десятки
        probe могли почти целиком состоять из HTTP, хотя в ProxyScrape SOCKS5 было больше.
        Теперь недавно успешные proxy всё равно имеют абсолютный приоритет, а среди новых
        кандидатов в batch>=3 резервируем один слот под SOCKS5, если он доступен.
        Так мы реально используем весь большой пул, не повышая число одновременных запросов.
        """
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

            def add_candidate(p):
                if len(batch) >= batch_size:
                    return False
                host = _proxy_host(p)
                if not host or host in batch_hosts:
                    return False
                batch.append(p)
                batch_hosts.add(host)
                self.last_used[p] = now
                return True

            # 1) Стабильный недавно успешный proxy важнее protocol mix.
            # Но после 3+ подряд ошибок он теряет абсолютный приоритет и дальше
            # конкурирует с остальными по score — это не даёт старой плохой истории
            # занимать все worker-слоты в тяжёлом discovery.
            for p in usable:
                if self._is_recent_good_locked(p, now) and self.fail_streak.get(p, 0) <= 2:
                    add_candidate(p)
                    if len(batch) >= batch_size:
                        return batch

            # 2) Rolling-discovery иногда просит всего один новый слот. Если текущий
            # in-flight набор остался без SOCKS5, preferred_scheme='socks5' не даёт
            # HTTP-бонусу снова вытеснить весь SOCKS5-пул.
            if preferred_scheme and len(batch) < batch_size:
                # ВАЖНО: preferred_scheme означает "зарезервировать ОДИН слот",
                # а не заполнить им весь batch. В v6.3 этот цикл мог набрать сразу
                # 3 SOCKS5, что видно по логам и ухудшало поиск из-за множества
                # SSL/MITM-ошибок бесплатных SOCKS5.
                for p in usable:
                    if _proxy_scheme(p) == preferred_scheme and add_candidate(p):
                        break

            # 3) Для новых адресов используем примерно 3:1 HTTP:SOCKS5 (2:1 при batch=3).
            # Это сохраняет приоритет HTTP по реальным UK-логам, но не оставляет 150-200
            # SOCKS5 вообще непроверенными до истечения discovery budget.
            remaining = batch_size - len(batch)
            if remaining > 0:
                selected_socks = sum(1 for p in batch if _proxy_scheme(p) == 'socks5')
                want_socks_total = 1 if batch_size >= 3 else 0
                need_socks = max(0, want_socks_total - selected_socks)

                if need_socks:
                    for p in usable:
                        if _proxy_scheme(p) == 'socks5' and add_candidate(p):
                            need_socks -= 1
                            if need_socks <= 0 or len(batch) >= batch_size:
                                break

            # 4) Остальные слоты в первую очередь HTTP/HTTPS, затем любой protocol.
            if len(batch) < batch_size:
                for p in usable:
                    if _proxy_scheme(p) in ('http', 'https'):
                        add_candidate(p)
                        if len(batch) >= batch_size:
                            break

            if len(batch) < batch_size:
                for p in usable:
                    add_candidate(p)
                    if len(batch) >= batch_size:
                        break

            return batch

    def get_due_reprobe_candidate(self, tried_proxies, reprobed_proxies, inflight_hosts=None):
        """Возвращает один недавно успешный proxy для повторной проверки в ТОМ ЖЕ discovery.

        Это решает сценарий из реального лога: хороший proxy временно упал, получил
        короткий cooldown, был проверен в начале 75-секундного discovery, затем успел
        восстановиться, но старый tried_hosts уже не позволял попробовать его снова.

        Ограничения безопасности:
        - только proxy, который реально был успешным в последний час;
        - только если он уже пробовался в этом discovery и после этого успел выйти из cooldown;
        - максимум ОДИН re-probe для конкретного proxy за discovery;
        - только при fail_streak 1-2 (нестабильные 3+ не получают быстрый повтор);
        - не запускаем второй запрос на тот же host, пока первый ещё in-flight.
        """
        tried_proxies = set(tried_proxies or ())
        reprobed_proxies = set(reprobed_proxies or ())
        inflight_hosts = set(inflight_hosts or ())
        if not tried_proxies:
            return None

        now = time.time()
        with self.lock:
            self._cleanup_bad_locked()
            candidates = []
            for p in tried_proxies:
                if p in reprobed_proxies:
                    continue
                if not self._is_recent_good_locked(p, now):
                    continue
                streak = self.fail_streak.get(p, 0)
                if streak < 1 or streak > 2:
                    continue
                if self.bad_until.get(p, 0) > now:
                    continue
                host = _proxy_host(p)
                if not host or host in inflight_hosts:
                    continue
                if self.host_bad_until.get(host, 0) > now:
                    continue
                candidates.append(p)

            if not candidates:
                return None

            candidates.sort(key=lambda p: self._candidate_score_locked(p, now), reverse=True)
            proxy = candidates[0]
            self.last_used[proxy] = now
            return proxy

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
def get_db_connection(application_name='ebay_uk_bot'):
    """Короткие DB-timeout без изменений в панели Aiven."""
    return psycopg2.connect(
        DATABASE_URL,
        connect_timeout=DB_CONNECT_TIMEOUT,
        application_name=application_name,
        options=(
            f"-c statement_timeout={DB_STATEMENT_TIMEOUT_MS} "
            f"-c lock_timeout={DB_LOCK_TIMEOUT_MS}"
        ),
    )


def init_db():
    with get_db_connection('ebay_uk_bot_init') as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS seen_items (item_id TEXT PRIMARY KEY, first_seen TIMESTAMP)")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bot_state (
                    state_key TEXT PRIMARY KEY,
                    state_value TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
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
            cur.execute(
                "ALTER TABLE auction_reminders "
                "ADD COLUMN IF NOT EXISTS last_status_check TIMESTAMPTZ NULL"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_auction_reminders_end_time "
                "ON auction_reminders (end_time_utc)"
            )
        conn.commit()


def get_bot_state(key, default=None):
    with get_db_connection('ebay_uk_bot_state_read') as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT state_value FROM bot_state WHERE state_key=%s", (key,))
            row = cur.fetchone()
            return row[0] if row else default


def set_bot_state(key, value):
    with get_db_connection('ebay_uk_bot_state_write') as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bot_state (state_key, state_value, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (state_key) DO UPDATE
                SET state_value=EXCLUDED.state_value, updated_at=NOW()
                """,
                (key, str(value)),
            )
        conn.commit()


def initialize_seen_count_cache():
    """Один раз получает точное количество seen_items при старте leader-копии."""
    global seen_count_cache
    try:
        with get_db_connection('ebay_uk_seen_count_init') as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM seen_items")
                count = int(cur.fetchone()[0])
        with seen_count_lock:
            seen_count_cache = count
        logging.info(f"📚 В базе seen_items: {count} товаров")
        return count
    except Exception as e:
        # Сбой счётчика не должен останавливать мониторинг. Все основные операции БД
        # продолжают работать как раньше; просто временно не показываем число.
        logging.warning(f"Не удалось получить количество seen_items: {e}")
        with seen_count_lock:
            seen_count_cache = None
        return None


def get_seen_count_cached():
    with seen_count_lock:
        return seen_count_cache


def _increment_seen_count_cache(delta):
    global seen_count_cache
    if not delta:
        return get_seen_count_cached()
    with seen_count_lock:
        if seen_count_cache is None:
            return None
        seen_count_cache += int(delta)
        return seen_count_cache


def claim_new_seen_ids(item_ids):
    """Атомарно резервирует только действительно новые eBay item_id.

    PRIMARY KEY + ON CONFLICT DO NOTHING защищает от повторной отправки даже если
    две Render-копии на несколько секунд пересеклись во время deploy.
    """
    unique_ids = list(dict.fromkeys(str(x) for x in item_ids if x))
    if not unique_ids:
        return set()
    with get_db_connection('ebay_uk_seen_claim') as conn:
        with conn.cursor() as cur:
            data = [(item_id, datetime.now(timezone.utc).replace(tzinfo=None)) for item_id in unique_ids]
            returned = execute_values(
                cur,
                "INSERT INTO seen_items (item_id, first_seen) VALUES %s "
                "ON CONFLICT (item_id) DO NOTHING RETURNING item_id",
                data,
                fetch=True,
            )
        conn.commit()
    claimed = {row[0] for row in returned}
    total = _increment_seen_count_cache(len(claimed))
    if claimed and total is not None:
        logging.info(f"📚 В базе seen_items: {total} товаров (+{len(claimed)})")
    return claimed


def add_seen_ids_batch(item_ids):
    # Для начального snapshot: помечаем текущую выдачу увиденной, но ничего не отправляем.
    claim_new_seen_ids(item_ids)


def is_db_empty():
    cached = get_seen_count_cached()
    if cached is not None:
        return cached == 0
    with get_db_connection('ebay_uk_db_empty') as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT NOT EXISTS (SELECT 1 FROM seen_items LIMIT 1)")
            return bool(cur.fetchone()[0])


# ============ SINGLE LEADER ДЛЯ RENDER ============
def try_acquire_leader_lock():
    """Возвращает отдельное соединение, которое держит PostgreSQL advisory lock."""
    conn = None
    try:
        conn = get_db_connection('ebay_uk_leader_lock')
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (LEADER_LOCK_KEY,))
            acquired = bool(cur.fetchone()[0])
        if acquired:
            return conn
    except Exception as e:
        logging.warning(f"Не удалось проверить leader-lock: {e}")
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
    return None


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
    wake_auction_workers()


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
    wake_auction_workers()
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
        wake_auction_workers()
    return True, changed, old_end, new_end_time_utc


def get_auctions_for_status_check(limit=100):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT item_id, url, title, end_time_utc, last_status_check
                FROM auction_reminders
                WHERE end_time_utc > NOW()
                ORDER BY end_time_utc ASC,
                         COALESCE(last_status_check, TIMESTAMPTZ '1970-01-01') ASC
                LIMIT %s
                """,
                (limit,),
            )
            return cur.fetchall()


def _status_check_interval_seconds(remaining):
    # После 5-минутного reminder запись удаляется, поэтому сверхчастые проверки
    # в последние минуты больше не нужны. До этого момента контроль остаётся лёгким.
    if remaining <= 5 * 60:
        return None
    if remaining <= 10 * 60:
        return 120
    if remaining <= 30 * 60:
        return 300
    if remaining <= 60 * 60:
        return 600
    if remaining <= 6 * 3600:
        return 900
    if remaining <= 24 * 3600:
        return 1800
    return 3600


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


def _mark_reminders_sent(conn, item_id, minutes_list):
    minutes_list = [m for m in AUCTION_REMINDER_MINUTES if m in set(minutes_list)]
    if not minutes_list:
        return
    assignments = ", ".join(f"{_reminder_column(m)} = TRUE" for m in minutes_list)
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE auction_reminders SET {assignments}, updated_at = NOW() WHERE item_id = %s",
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
def _telegram_post(method, payload, timeout=10, max_attempts=3):
    """Аккуратные retry без агрессивного спама Telegram.

    Повторяем только случаи, где повтор относительно безопасен: явный 429/5xx и
    ConnectTimeout (соединение не установлено). При ReadTimeout/ConnectionError ответ
    мог потеряться уже ПОСЛЕ принятия сообщения Telegram, поэтому автоматический
    немедленный дубль не делаем.
    """
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    attempts = max(1, int(max_attempts))
    for attempt in range(1, attempts + 1):
        try:
            r = requests.post(url, json=payload, timeout=timeout)
        except requests.exceptions.ConnectTimeout as e:
            logging.warning(f"Telegram connect timeout ({method}), попытка {attempt}/{attempts}: {e}")
            if attempt < attempts:
                time.sleep(min(2.0, 0.5 * attempt))
                continue
            return None
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
            logging.error(
                f"Telegram {method}: неоднозначная сетевая ошибка, без немедленного retry "
                f"во избежание дубля: {e}"
            )
            return None
        except Exception as e:
            logging.error(f"Telegram {method}: ошибка: {e}")
            return None

        if r.status_code == 200:
            try:
                return r.json()
            except Exception:
                return {'ok': True}

        if r.status_code == 429 and attempt < attempts:
            retry_after = 1
            try:
                retry_after = int((r.json().get('parameters') or {}).get('retry_after') or 1)
            except Exception:
                pass
            wait = max(1, min(retry_after, 15))
            logging.warning(f"Telegram 429, повтор через {wait} сек. ({attempt}/{attempts})")
            time.sleep(wait)
            continue

        if 500 <= r.status_code <= 599 and attempt < attempts:
            wait = min(3.0, float(attempt))
            logging.warning(f"Telegram HTTP {r.status_code}, повтор через {wait:.1f} сек.")
            time.sleep(wait)
            continue

        logging.error(f"Ошибка Telegram {method}: HTTP {r.status_code}: {r.text[:500]}")
        return None
    return None


def send_telegram_message(message, parse_mode='HTML', reply_markup=None, disable_preview=False):
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message,
        'parse_mode': parse_mode,
        'disable_web_page_preview': disable_preview,
    }
    if reply_markup is not None:
        payload['reply_markup'] = reply_markup
    result = _telegram_post('sendMessage', payload, timeout=10, max_attempts=3)
    return bool(result and result.get('ok', True))


def answer_callback_query(callback_query_id, text=None, show_alert=False):
    if not callback_query_id:
        return False
    payload = {'callback_query_id': callback_query_id, 'show_alert': bool(show_alert)}
    if text:
        payload['text'] = str(text)[:200]
    result = _telegram_post('answerCallbackQuery', payload, timeout=8, max_attempts=2)
    return bool(result and result.get('ok', True))


def edit_message_reply_markup(message_id, reply_markup=None):
    if not message_id:
        return False
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'message_id': message_id,
        'reply_markup': reply_markup or {'inline_keyboard': []},
    }
    result = _telegram_post('editMessageReplyMarkup', payload, timeout=8, max_attempts=2)
    return bool(result and result.get('ok', True))


def auction_message_keyboard(item_id, url, include_list=True):
    rows = [[{'text': '🔗 Открыть eBay', 'url': url}]]
    bottom = [{'text': '❌ Удалить', 'callback_data': f'aucdel:{item_id}'}]
    if include_list:
        bottom.append({'text': '📋 Все аукционы', 'callback_data': 'auclist'})
    rows.append(bottom)
    return {'inline_keyboard': rows}


def auction_list_only_keyboard():
    return {'inline_keyboard': [[{'text': '📋 Все аукционы', 'callback_data': 'auclist'}]]}


def auction_open_list_keyboard(url):
    return {'inline_keyboard': [[
        {'text': '🔗 Открыть eBay', 'url': url},
        {'text': '📋 Все аукционы', 'callback_data': 'auclist'},
    ]]}

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
    safe_title = html_lib.escape(title)

    # Если до конца уже 5 минут или меньше, плановых reminders впереди нет.
    # Не создаём лишнюю запись/нагрузку: сразу показываем точное время и ссылку.
    if remaining <= 5 * 60:
        send_telegram_message(
            "⚠️ <b>До окончания аукциона осталось меньше 5 минут</b> 🇬🇧\n\n"
            f"📦 <b>{safe_title}</b>\n\n"
            f"🕒 Окончание по Киеву: <b>{format_kyiv_datetime(end_time_utc)}</b>\n"
            f"⏳ Осталось: <b>{format_remaining(remaining)}</b>\n\n"
            "В список напоминаний не добавляю: последний плановый порог 5 минут уже наступил.",
            reply_markup=auction_open_list_keyboard(canonical_url),
            disable_preview=True,
        )
        return

    save_auction_reminder(item_id, canonical_url, title, end_time_utc)
    mark_auction_status_checked(item_id)

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
            {'text': f'❌ Удалить #{idx}', 'callback_data': f'aucdel:{item_id}'},
        ])

    parts.append("\n\nНажмите ❌ возле нужного аукциона — номер вводить вручную не нужно.")
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
    """Лёгкая проверка досрочно закрытых лотов до 5-минутного reminder."""
    logging.info("🛰 Worker контроля сохранённых аукционов запущен")
    db_ready_event.wait()
    while True:
        try:
            # Когда основной монитор уже имеет проблемы с eBay, не добавляем лишний трафик.
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
                    if interval is None:
                        # После 5 минут запись должен завершить reminder worker.
                        continue
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
                    time.sleep(random.uniform(0.8, 1.4))
        except Exception as e:
            logging.error(f"Ошибка auction status worker: {e}", exc_info=True)
        auction_status_wakeup_event.wait(timeout=AUCTION_STATUS_TICK)
        auction_status_wakeup_event.clear()


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
    """Scheduler reminders 60/30/10/5 мин. После успешного 5-мин. сообщения запись удаляется."""
    logging.info("⏰ Планировщик auction reminders запущен")
    db_ready_event.wait()
    conn = None
    while True:
        next_sleep = AUCTION_SCHEDULER_MAX_SLEEP
        try:
            if conn is None or conn.closed:
                conn = get_db_connection('ebay_uk_auction_scheduler')

            now = datetime.now(timezone.utc)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT item_id, url, title, end_time_utc,
                           reminder_60_sent, reminder_30_sent, reminder_10_sent, reminder_5_sent
                    FROM auction_reminders
                    WHERE end_time_utc > %s
                    ORDER BY end_time_utc ASC
                    LIMIT 200
                    """,
                    (now,),
                )
                rows = cur.fetchall()

            for row in rows:
                item_id, url, title, end_time, sent60, sent30, sent10, sent5 = row
                end_time = _ensure_aware_utc(end_time)
                remaining = (end_time - now).total_seconds()
                if remaining <= 0:
                    continue

                sent_map = {60: sent60, 30: sent30, 10: sent10, 5: sent5}
                due_minutes = []
                for minutes in AUCTION_REMINDER_MINUTES:
                    if sent_map[minutes]:
                        continue
                    trigger_time = end_time - timedelta(minutes=minutes)
                    seconds_to_trigger = (trigger_time - now).total_seconds()
                    if seconds_to_trigger <= 0:
                        due_minutes.append(minutes)
                    else:
                        next_sleep = min(next_sleep, max(1.0, seconds_to_trigger))

                if not due_minutes:
                    next_sleep = min(next_sleep, max(1.0, remaining))
                    continue

                # Перечитываем строку перед отправкой: status-worker мог изменить время/удалить лот.
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT end_time_utc, reminder_60_sent, reminder_30_sent,
                               reminder_10_sent, reminder_5_sent, title, url
                        FROM auction_reminders WHERE item_id=%s
                        """,
                        (item_id,),
                    )
                    current = cur.fetchone()
                if not current:
                    continue

                current_end, c60, c30, c10, c5, current_title, current_url = current
                current_end = _ensure_aware_utc(current_end)
                if abs((current_end - end_time).total_seconds()) > 5:
                    # Время изменилось — новый цикл пересчитает пороги.
                    auction_reminder_wakeup_event.set()
                    continue

                now_send = datetime.now(timezone.utc)
                remaining_send = (current_end - now_send).total_seconds()
                if remaining_send <= 0:
                    continue

                current_sent = {60: c60, 30: c30, 10: c10, 5: c5}
                due_now = [
                    m for m in AUCTION_REMINDER_MINUTES
                    if not current_sent[m]
                    and now_send >= current_end - timedelta(minutes=m)
                ]
                if not due_now:
                    continue

                # Если во время restart/короткого сбоя пропущено несколько порогов,
                # НЕ отправляем пачку старых сообщений. Берём только самый актуальный.
                latest_minutes = min(due_now)
                trigger_time = current_end - timedelta(minutes=latest_minutes)
                lateness = max(0.0, (now_send - trigger_time).total_seconds())
                safe_title = html_lib.escape(current_title or title)
                label = '1 час' if latest_minutes == 60 else f'{latest_minutes} минут'

                if lateness <= AUCTION_REMINDER_LATE_GRACE:
                    heading = f"🔔 Плановое напоминание: <b>за {label}</b>"
                else:
                    heading = (
                        "⚠️ <b>Актуальное напоминание после временной паузы</b>\n"
                        "Старые пропущенные пороги не дублирую."
                    )

                msg = (
                    "⏰ <b>Напоминание об аукционе eBay UK</b> 🇬🇧\n\n"
                    f"{heading}\n"
                    f"📦 <b>{safe_title}</b>\n\n"
                    f"⏳ До окончания сейчас: <b>{format_remaining(remaining_send)}</b>\n"
                    f"🕒 Окончание по Киеву: <b>{format_kyiv_datetime(current_end)}</b>"
                )

                # На 5 мин это последнее сообщение: после успешной доставки удаляем запись,
                # поэтому дальше никаких status-check и reminders этот аукцион не создаёт.
                is_final_five = latest_minutes == 5
                keyboard = (
                    auction_open_list_keyboard(current_url)
                    if is_final_five
                    else auction_message_keyboard(item_id, current_url)
                )

                if send_telegram_message(msg, reply_markup=keyboard, disable_preview=True):
                    if is_final_five:
                        delete_auction_reminder(item_id)
                        logging.info(f"📨 Auction {item_id}: отправлено финальное reminder 5 мин; запись удалена")
                    else:
                        _mark_reminders_sent(conn, item_id, due_now)
                        logging.info(
                            f"📨 Auction {item_id}: отправлено актуальное reminder {latest_minutes} мин; "
                            f"закрыты пороги {sorted(due_now, reverse=True)}"
                        )
                else:
                    # Для аукциона лучше повторить позже, чем навсегда потерять важное reminder.
                    next_sleep = min(next_sleep, 15)

                next_sleep = min(next_sleep, max(1.0, remaining_send))

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

        auction_reminder_wakeup_event.wait(
            timeout=max(1.0, min(float(next_sleep), AUCTION_SCHEDULER_MAX_SLEEP))
        )
        auction_reminder_wakeup_event.clear()


def telegram_listener():
    global is_paused
    logging.info("🔁 Поток слушателя команд Telegram запущен")
    try:
        last_update_id = int(get_bot_state('telegram_last_update_id', '0') or 0)
    except Exception as e:
        logging.warning(f"Не удалось прочитать telegram_last_update_id, начинаем с 0: {e}")
        last_update_id = 0

    while True:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
            params = {'offset': last_update_id + 1, 'timeout': 30}
            r = requests.get(url, params=params, timeout=35)
            if r.status_code != 200:
                logging.warning(f"Telegram getUpdates HTTP {r.status_code}: {r.text[:300]}")
                time.sleep(3)
                continue

            updates = r.json().get('result', [])
            for update in updates:
                update_id = int(update.get('update_id', 0))
                if update_id <= last_update_id:
                    continue

                try:
                    callback = update.get('callback_query')
                    if callback:
                        handle_telegram_callback(callback)
                    else:
                        message = update.get('message')
                        if message and str(message.get('chat', {}).get('id')) == str(TELEGRAM_CHAT_ID):
                            text = message.get('text', '').strip()
                            if text == '/stop':
                                is_paused = True
                                send_telegram_message(
                                    "⏸ Основной мониторинг новых товаров приостановлен. "
                                    "Напоминания об уже сохранённых аукционах продолжают работать. "
                                    "Для возобновления отправьте /start"
                                )
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
                                        send_telegram_message(f"❌ Напоминание для лота <b>{item_id}</b> удалено.")
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
                                            f"🔎 Проверяю {'ссылку' if accepted == 1 else f'{accepted} ссылок'} "
                                            "на активный аукцион eBay UK…\n"
                                            "Сохраню только подтверждённые торги с точным временем окончания."
                                        )
                                    if accepted < len(ebay_urls[:20]):
                                        send_telegram_message(
                                            "⚠️ Очередь ссылок заполнена. Остальные ссылки отправьте немного позже."
                                        )
                except Exception as e:
                    logging.error(f"Ошибка обработки Telegram update {update_id}: {e}", exc_info=True)
                finally:
                    # Подтверждаем обработанный update в нашей БД. Это не связано с seen_items:
                    # здесь защищаем только входящие команды/кнопки от повторного проигрывания после deploy.
                    try:
                        set_bot_state('telegram_last_update_id', update_id)
                        last_update_id = update_id
                    except Exception as e:
                        logging.error(f"Не удалось сохранить Telegram update_id={update_id}: {e}")
                        # Не повышаем offset в памяти без БД: лучше повторить одну команду,
                        # чем потерять входящее действие пользователя при аварийном restart.
                        break
            time.sleep(0.5)
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

    # 2) Rolling discovery: сначала 3, затем максимум 4 уникальных IP параллельно.
    # Главное отличие от batch-модели: как только один плохой proxy завершился, его слот
    # немедленно получает следующий кандидат. Мы больше не ждём самый медленный proxy
    # в группе, пока остальные worker-ы простаивают.
    started = time.monotonic()
    tried_hosts = set()
    tried_proxies = set()
    reprobed_proxies = set()
    attempts = 0
    refreshed_after_exhaustion = False
    profile = get_preferred_profile()
    if profile is None:
        logging.error("Нет поддерживаемого browser-профиля curl_cffi")
        return None

    executor = ThreadPoolExecutor(
        max_workers=PROBE_ESCALATED_CONCURRENCY,
        thread_name_prefix='proxy-probe',
    )
    future_to_proxy = {}

    def desired_concurrency():
        elapsed_now = time.monotonic() - started
        return (
            PROBE_ESCALATED_CONCURRENCY
            if elapsed_now >= PROBE_ESCALATE_AFTER
            else PROBE_CONCURRENCY
        )

    def submit_more():
        """Поддерживает нужное число in-flight probe без превышения concurrency."""
        nonlocal attempts, refreshed_after_exhaustion

        if attempts >= MAX_SEARCH_ATTEMPTS:
            return False
        elapsed_now = time.monotonic() - started
        if elapsed_now >= SEARCH_TIME_BUDGET:
            return False

        desired = desired_concurrency()
        need = max(0, min(desired - len(future_to_proxy), MAX_SEARCH_ATTEMPTS - attempts))
        if need <= 0:
            return True

        # В rolling-режиме после завершения SOCKS5 часто освобождается один слот.
        # Поддерживаем хотя бы один SOCKS5 in-flight при concurrency>=3, если он доступен.
        inflight_socks = sum(
            1 for p in future_to_proxy.values()
            if _proxy_scheme(p) == 'socks5'
        )
        preferred_scheme = 'socks5' if desired >= 3 and inflight_socks == 0 else None

        batch = []

        # Один контролируемый re-probe недавно успешного proxy после окончания его
        # короткого cooldown прямо внутри текущего длинного discovery. Это не увеличивает
        # concurrency: re-probe занимает только уже свободный worker-слот.
        inflight_hosts = {_proxy_host(p) for p in future_to_proxy.values()}
        reprobe = proxy_manager.get_due_reprobe_candidate(
            tried_proxies=tried_proxies,
            reprobed_proxies=reprobed_proxies,
            inflight_hosts=inflight_hosts,
        )
        if reprobe is not None and need > 0:
            reprobed_proxies.add(reprobe)
            batch.append(reprobe)
            logging.info(
                f"♻️ Re-probe недавно успешного proxy после cooldown: {reprobe}"
            )

        remaining_need = need - len(batch)
        if remaining_need > 0:
            batch.extend(
                proxy_manager.get_candidate_batch(
                    remaining_need,
                    tried_hosts=tried_hosts,
                    preferred_scheme=preferred_scheme,
                )
            )

        if not batch and not future_to_proxy and not refreshed_after_exhaustion:
            # Если реально закончились ещё не пробованные доступные IP, один раз берём
            # свежий снимок ProxyScrape. Cooldown плохих endpoints при этом сохраняется.
            logging.info("♻️ Доступные уникальные IP исчерпаны; обновляем ProxyScrape и продолжаем")
            proxy_manager.refresh_proxies(force=True)
            refreshed_after_exhaustion = True
            # Старые hosts не очищаем: свежий ProxyScrape может добавить новые IP,
            # и именно их нужно попробовать. Уже проверенные адреса не должны идти
            # по третьему кругу; для recently-good существует отдельный one-shot re-probe.
            batch = proxy_manager.get_candidate_batch(
                need,
                tried_hosts=tried_hosts,
                preferred_scheme=preferred_scheme,
            )

        if not batch:
            return False

        for proxy in batch:
            if attempts >= MAX_SEARCH_ATTEMPTS:
                break
            tried_hosts.add(_proxy_host(proxy))
            tried_proxies.add(proxy)
            attempts += 1
            probe_kind = "Re-probe" if proxy in reprobed_proxies else "Probe"
            logging.info(
                f"🔍 {probe_kind} {attempts}/{MAX_SEARCH_ATTEMPTS}: proxy {proxy}, "
                f"профиль {profile['name']}"
            )
            future = executor.submit(
                _probe_proxy,
                proxy,
                profile,
                (PROBE_CONNECT_TIMEOUT, PROBE_READ_TIMEOUT),
            )
            future_to_proxy[future] = proxy
        return bool(batch)

    submit_more()

    try:
        while future_to_proxy or attempts < MAX_SEARCH_ATTEMPTS:
            elapsed = time.monotonic() - started
            if elapsed >= SEARCH_TIME_BUDGET:
                logging.warning(
                    f"⏱ Достигнут лимит discovery {SEARCH_TIME_BUDGET} сек.; "
                    "завершаем текущий активный поиск"
                )
                break

            # Через 15 сек. разрешается 4-й worker. Если сейчас свободен слот — заполняем.
            submit_more()

            if not future_to_proxy:
                # Нет ни одного доступного кандидата прямо сейчас. Не крутим CPU и не
                # штурмуем ProxyScrape; через короткий jitter попробуем снова.
                time.sleep(random.uniform(0.8, 1.3))
                if not submit_more():
                    break
                continue

            remaining_budget = max(0.05, SEARCH_TIME_BUDGET - (time.monotonic() - started))
            done, _ = wait(
                tuple(future_to_proxy.keys()),
                timeout=min(1.0, remaining_budget),
                return_when=FIRST_COMPLETED,
            )
            if not done:
                continue

            winner = None
            completed_results = []
            for future in done:
                proxy = future_to_proxy.pop(future, None)
                if proxy is None:
                    continue
                try:
                    result, html, session = future.result()
                except Exception as e:
                    logging.error(f"Ошибка probe worker для {proxy}: {e}")
                    result, html, session = 'proxy_error', None, None

                completed_results.append(result)
                if result == 'success' and winner is None:
                    proxy_manager.mark_success(proxy)
                    winner = (proxy, html, session)
                elif result == 'profile_error':
                    close_session(session)
                    fallback_profile = get_preferred_profile()
                    if fallback_profile is not None:
                        profile = fallback_profile
                else:
                    close_session(session)
                    proxy_manager.mark_failure(proxy, result, reason=result)

            if winner is not None:
                # Остальные 1-3 probe уже запущены. Не ждём их: они завершатся в фоне,
                # закроют Session и могут запомниться как резервный успешный proxy.
                for future, proxy in list(future_to_proxy.items()):
                    if future.done():
                        _cleanup_late_probe_future(future, proxy)
                    elif not future.cancel():
                        future.add_done_callback(
                            lambda f, p=proxy: _cleanup_late_probe_future(f, p)
                        )
                future_to_proxy.clear()
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

            # Даже если 2-3 proxy упали почти одновременно, не запускаем замену абсолютно
            # мгновенно. Небольшой jitter сохраняет безопасный ритм, но не блокирует все
            # worker-ы до завершения самого медленного соседа, как делал старый batch.
            if completed_results:
                time.sleep(random.uniform(PROBE_BATCH_PAUSE_MIN, PROBE_BATCH_PAUSE_MAX))
            submit_more()

    finally:
        # При исчерпании budget не ждём до 8 сек. оставшиеся плохие соединения.
        # Уже работающие futures получают cleanup callback; новые больше не запускаются.
        for future, proxy in list(future_to_proxy.items()):
            if future.done():
                _cleanup_late_probe_future(future, proxy)
            elif not future.cancel():
                future.add_done_callback(
                    lambda f, p=proxy: _cleanup_late_probe_future(f, p)
                )
        executor.shutdown(wait=False, cancel_futures=True)

    logging.error(
        f"❌ В этом цикле рабочий proxy не найден: "
        f"проверено {attempts}, время {time.monotonic() - started:.1f} сек."
    )
    # Перед коротким следующим циклом берём максимально свежий снимок ProxyScrape.
    # Реальные cooldown сохраняются, поэтому только что плохие proxy не пойдут по кругу.
    proxy_manager.refresh_proxies(force=True)
    return None


def fetch_ebay_html_with_retry():
    return fetch_ebay_html_with_fixed_pair()

# ============ ПАРСИНГ ============
def extract_item_id(url):
    """Извлекает публичный numeric eBay item id из старого и нового URL.

    Поддерживает оба варианта:
      /itm/123456789012
      /itm/some-title/123456789012
    Не принимает текстовый slug за item_id.
    """
    if not url or '/itm/' not in url:
        return None
    m = re.search(r'/itm/(?:[^/?#]+/)?(\d{8,15})(?:[/?#]|$)', str(url), re.I)
    return m.group(1) if m else None

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

def _main_search_result_cards(soup):
    """Возвращает только карточки ОСНОВНОЙ поисковой выдачи eBay.

    eBay сейчас A/B-тестирует несколько DOM-разметок. На разных proxy один и тот же
    поиск может прийти как старый ``li.s-item`` либо как новый ``li.s-card`` /
    ``div.su-card-container``. Поэтому нельзя считать отсутствие ``s-item`` ошибкой.

    При этом мы по-прежнему НЕ сканируем всю страницу по всем ``/itm/`` ссылкам:
    поиск ограничен основным SRP-контейнером, чтобы не захватывать рекомендации,
    карусели и посторонние блоки. MAX_ITEMS остаётся только верхним пределом.
    """
    root = (
        soup.select_one('#srp-river-results')
        or soup.select_one('.srp-river-results')
        or soup.select_one('ul.srp-results')
    )
    if root is None:
        logging.warning(
            "⚠️ Основной контейнер поисковой выдачи eBay не найден; "
            "глобальный fallback по всем /itm/ ссылкам отключён"
        )
        return None

    cards = []
    seen_nodes = set()
    rewrite_boundary_seen = False
    layout_counts = {'s-item': 0, 's-card': 0, 'su-card-container': 0, 'data-viewport': 0}

    def add_card(node, layout):
        key = id(node)
        if key in seen_nodes:
            return
        # Карточка должна содержать реальную ссылку на item. Это отсекает служебные
        # placeholders, заголовки и пустые контейнеры с похожими CSS-классами.
        if not node.select_one('a[href*="/itm/"]'):
            return
        seen_nodes.add(key)
        cards.append(node)
        layout_counts[layout] = layout_counts.get(layout, 0) + 1

    # Идём в DOM-порядке, чтобы остановиться ДО блока расширенных результатов.
    for node in root.descendants:
        if not isinstance(node, Tag):
            continue
        classes = set(node.get('class') or [])
        if 'srp-river-answer--REWRITE_START' in classes:
            rewrite_boundary_seen = True
            break

        if 's-item' in classes:
            add_card(node, 's-item')
        elif 's-card' in classes:
            add_card(node, 's-card')
        elif 'su-card-container' in classes:
            add_card(node, 'su-card-container')
        elif node.name == 'li' and node.has_attr('data-viewport'):
            # Дополнительный безопасный layout-fallback, который встречается в A/B DOM.
            add_card(node, 'data-viewport')

    # Если eBay снова поменял имена card-классов, используем ТОЛЬКО ссылки внутри
    # основного root и поднимаемся к ближайшему небольшому контейнеру с ценой/заголовком.
    # Это намного безопаснее старого fallback по всем /itm/ ссылкам всей страницы.
    if not cards:
        fallback_seen_ids = set()
        for node in root.descendants:
            if not isinstance(node, Tag):
                continue
            classes = set(node.get('class') or [])
            if 'srp-river-answer--REWRITE_START' in classes:
                rewrite_boundary_seen = True
                break
            if node.name != 'a':
                continue
            href = node.get('href') or ''
            item_id = extract_item_id(href)
            if not item_id or item_id in fallback_seen_ids:
                continue

            parent = node
            chosen = None
            for _ in range(8):
                parent = parent.parent
                if parent is None or parent is root:
                    break
                if not isinstance(parent, Tag):
                    continue
                text = parent.get_text(' ', strip=True)
                has_price = bool(
                    parent.select_one('.s-card__price, .s-item__price, [class*="price"]')
                    or re.search(r'£\s*[\d,.]+', text)
                )
                has_title = bool(
                    parent.select_one('.s-card__title, .s-item__title, [role="heading"]')
                    or len(node.get_text(' ', strip=True)) >= 4
                )
                if has_price and has_title:
                    chosen = parent
                    break
            if chosen is not None:
                cards.append(chosen)
                fallback_seen_ids.add(item_id)

        if cards:
            logging.warning(
                f"⚠️ eBay использует неизвестный card-layout; безопасный fallback "
                f"внутри основного контейнера распознал {len(cards)} карточек"
            )

    if not cards:
        logging.warning(
            "⚠️ Основной контейнер eBay найден, но карточки выдачи не распознаны "
            "ни как s-item, ни как s-card/su-card-container; БД не изменяем"
        )
        return None

    if rewrite_boundary_seen:
        logging.info(
            f"🛡 Обнаружена граница расширенных результатов eBay; "
            f"учитываем только {len(cards)} карточек до неё"
        )

    active_layouts = ', '.join(f'{k}={v}' for k, v in layout_counts.items() if v)
    if active_layouts:
        logging.info(f"🧩 Разметка eBay: {active_layouts}; карточек-кандидатов {len(cards)}")

    return cards


def _listing_link(card):
    """Ссылка item для старой и новой SRP-разметки eBay."""
    selectors = (
        'a.s-card__link[href*="/itm/"]',
        'a.s-item__link[href*="/itm/"]',
        'a.su-link[href*="/itm/"]',
        'a[href*="/itm/"]',
    )
    for selector in selectors:
        link = card.select_one(selector)
        if link and extract_item_id(link.get('href') or ''):
            return link
    return None


def parse_ebay_listings(html, max_items=MAX_ITEMS):
    if not html:
        return None
    soup = BeautifulSoup(html, 'html.parser')
    cards = _main_search_result_cards(soup)
    if cards is None:
        return None

    items = {}
    for card in cards:
        if len(items) >= max_items:
            break

        link = _listing_link(card)
        if not link:
            continue
        url = link.get('href')
        if not url:
            continue
        if url.startswith('/'):
            url = 'https://www.ebay.co.uk' + url
        item_id = extract_item_id(url)
        if not item_id or item_id in items:
            continue

        title_elem = (
            card.select_one('div.s-card__title')
            or card.select_one('.s-card__title')
            or card.select_one('div.s-item__title span[role="heading"]')
            or card.select_one('div.s-item__title')
            or card.select_one('[role="heading"]')
            or link
        )
        title = clean_title(title_elem.get_text(' ', strip=True) if title_elem else '')
        if not title:
            title = clean_title(link.get_text(' ', strip=True))
        if not title:
            continue
        if title.strip().lower() in {'shop on ebay', 'opens in a new window or tab'}:
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
            'buy_it_now_price': bin_price,
        }

    logging.info(f"Обработано товаров основной выдачи: {len(items)}")
    return items

def perform_initial_snapshot():
    logging.info("Начальный снимок...")
    html = fetch_ebay_html_with_retry()
    if not html:
        return False
    items = parse_ebay_listings(html, max_items=MAX_ITEMS)
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
    html = fetch_ebay_html_with_retry()
    if not html:
        logging.warning("Не удалось загрузить страницу, проверка пропущена")
        return 'fetch_error'

    current = parse_ebay_listings(html)
    if current is None:
        logging.warning("Структура основной выдачи eBay не распознана; проверка пропущена без изменений БД")
        # Это НЕ означает, что proxy плохой: HTTP 200 уже мог быть успешным.
        # Не запускаем из-за DOM-ошибки ускоренный proxy-discovery каждые 6-10 сек.
        return 'parse_error'
    if not current:
        logging.info("Основная выдача eBay распознана, но подходящих карточек нет")
        return 'ok'

    # ВАЖНО: вместо скачивания всех ~18k seen_items атомарно пробуем вставить только
    # текущие item_id основной выдачи (до 60). PRIMARY KEY + ON CONFLICT гарантирует, что старый товар
    # никогда не станет "новым" повторно, даже при коротком overlap двух Render instances.
    claimed = claim_new_seen_ids(list(current.keys()))
    new = []
    for item_id, data in current.items():
        if item_id in claimed:
            new.append({'id': item_id, **data})
            logging.info(
                f"НОВЫЙ: {data['title'][:50]}... цена: {data['price']}, "
                f"доставка: {data.get('shipping')}, best_offer: {data.get('best_offer')}, "
                f"auction: {data.get('auction')}, has_buy_it_now: {data.get('has_buy_it_now')}"
            )

    if new:
        for item in new:
            msg = f"🇬🇧 <b>НОВЫЙ ТОВАР Англия</b> 🇬🇧\n\n<b>{item['title']}</b>\n\n"
            if item['price']:
                msg += f"💰 Цена: {item['price']}\n"
            else:
                msg += "💰 Цена не указана (не GBP)\n"
            if item['shipping']:
                msg += f"🚚 Доставка: {item['shipping']}\n"
            else:
                msg += "🚚 Доставка: не указана\n"
            if item.get('best_offer', False):
                msg += "✅ Сделать предложение (Best Offer)\n"
            if item.get('auction', False):
                if item.get('has_buy_it_now', False) and item.get('buy_it_now_price'):
                    msg += f"⏰ Аукцион / Buy It Now цена: {item['buy_it_now_price']}\n"
                elif item.get('has_buy_it_now', False):
                    msg += "⏰ Аукцион / Buy It Now\n"
                else:
                    msg += "⏰ Аукцион\n"
            if not item.get('auction', False) or (
                item.get('auction', False) and item.get('has_buy_it_now', False)
            ):
                total = calculate_total_price(
                    item['price'],
                    item['shipping'],
                    item.get('buy_it_now_price'),
                    is_auction=item.get('auction', False),
                )
                if total is not None:
                    msg += f"\nЗа все (с доставкой в Украину): <b>{total}грн</b>"
            msg += f"\n\n🔗 <a href='{item['url']}'>Ссылка на товар</a>"

            # item_id уже записан в seen_items ДО Telegram. Даже если Render внезапно
            # перезапустится после отправки, этот товар не пойдёт по второму кругу.
            if not send_telegram_message(msg):
                logging.error(
                    f"Telegram не подтвердил отправку нового item {item['id']}. "
                    "ID оставлен в seen_items специально, чтобы не создать повторную отправку."
                )
            time.sleep(1)
    else:
        logging.info("Новых нет")
    return 'ok'


def bot_worker():
    global is_paused
    logging.info("🤖 Бот-воркер запущен")
    db_ready_event.wait()
    if is_db_empty():
        if not perform_initial_snapshot():
            send_telegram_message("❌ Ошибка инициализации")
            return
        startup_line = "✅ Бот запущен, начальный снимок сделан"
    else:
        startup_line = "✅ Бот запущен / перезапущен"

    seen_total = get_seen_count_cached()
    seen_line = f"\n📚 В базе: {seen_total} товаров." if seen_total is not None else ""
    send_telegram_message(
        startup_line +
        "\n🇬🇧 eBay UK monitor v6.5 работает." +
        seen_line +
        "\nКоманды: /stop /start /list (/auctions) /delauction НОМЕР_ЛОТА"
        "\nМожно отправить ссылку на eBay-аукцион — сохраню точное время и напомню заранее.",
        reply_markup=auction_list_only_keyboard(),
    )
    while True:
        if is_paused:
            time.sleep(2)
            continue
        try:
            result = check_and_send_new_items()
            if result == 'ok':
                wait = random.uniform(CHECK_INTERVAL, CHECK_INTERVAL + 12)
                logging.info(f"✅ Успешная проверка. Следующая через {wait:.0f} секунд.")
            elif result == 'parse_error':
                # Сеть/eBay были доступны, проблема только в DOM-разметке. Нельзя
                # ошибочно объявлять рабочий proxy плохим и запускать частый discovery.
                wait = random.uniform(CHECK_INTERVAL, CHECK_INTERVAL + 12)
                logging.warning(
                    f"⚠️ eBay доступен, но разметка не распознана. "
                    f"Повтор обычной проверки через {wait:.0f} секунд без смены proxy."
                )
            else:
                # Реальная проблема загрузки/proxy: после активного discovery оставляем
                # короткий jitter, чтобы быстро вернуться к поиску, но не крутить busy-loop.
                wait = random.uniform(FAILED_SEARCH_RETRY_MIN, FAILED_SEARCH_RETRY_MAX)
                logging.info(f"⚠️ Рабочий proxy пока не найден. Новый цикл через {wait:.1f} секунд.")
            time.sleep(wait)
        except Exception as e:
            logging.error(f"Ошибка в основном цикле: {e}", exc_info=True)
            time.sleep(5)

def start_leader_workers():
    """Инициализирует БД и запускает фоновые задачи только в leader-instance."""
    init_db()
    initialize_seen_count_cache()
    db_ready_event.set()
    leader_active_event.set()
    logging.info("👑 Эта Render-копия стала leader; запускаем фоновые worker-ы")

    threading.Thread(target=telegram_listener, daemon=True, name='telegram-listener').start()
    threading.Thread(target=connection_watchdog, daemon=True, name='connection-watchdog').start()
    threading.Thread(target=auction_link_worker, daemon=True, name='auction-link-worker').start()
    threading.Thread(target=auction_reminder_worker, daemon=True, name='auction-reminder-worker').start()
    threading.Thread(target=auction_status_worker, daemon=True, name='auction-status-worker').start()
    threading.Thread(target=bot_worker, daemon=True, name='main-ebay-worker').start()


def leader_supervisor():
    """Не даёт двум Render instances одновременно запускать Telegram/eBay workers."""
    last_standby_log = 0.0
    while True:
        conn = try_acquire_leader_lock()
        if conn is None:
            now = time.monotonic()
            if now - last_standby_log >= 30:
                logging.info("🟡 Standby: другая Render-копия уже держит bot leader-lock")
                last_standby_log = now
            time.sleep(LEADER_RETRY_INTERVAL)
            continue

        try:
            start_leader_workers()
            # Держим session-level advisory lock отдельным соединением.
            # Если оно умерло, PostgreSQL сам освободит lock. Чтобы старая копия
            # не продолжала workers без lock, завершаем процесс — Render его перезапустит.
            while True:
                time.sleep(LEADER_HEALTH_INTERVAL)
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
        except Exception as e:
            logging.critical(f"Потерян PostgreSQL leader-lock/connection: {e}", exc_info=True)
            try:
                conn.close()
            except Exception:
                pass
            os._exit(1)


@app.route('/')
def index():
    role = "leader" if leader_active_event.is_set() else "standby"
    return f"eBay бот работает (Великобритания, adaptive parallel UK v6.5, {role})"


@app.route('/health')
def health():
    # /health намеренно не зависит от eBay/proxy/Aiven: UptimeRobot должен держать
    # Render Web Service активным даже во время временного сбоя внешних сервисов.
    return "OK", 200


if __name__ == "__main__":
    # Flask привязывается к PORT сразу, чтобы новый Render instance прошёл health/port check.
    # Фоновые задачи стартуют только после получения PostgreSQL leader-lock.
    threading.Thread(target=leader_supervisor, daemon=True, name='leader-supervisor').start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
