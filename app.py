import os
import sys
import ssl
import time
import random
import re
import json
import threading
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from datetime import datetime
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
PROBE_READ_TIMEOUT = float(os.getenv("PROBE_READ_TIMEOUT", "15"))

# Для уже найденной рабочей пары таймауты мягче: её не надо выбрасывать
# только потому, что один ответ оказался чуть медленнее.
FIXED_CONNECT_TIMEOUT = float(os.getenv("FIXED_CONNECT_TIMEOUT", "8"))
FIXED_READ_TIMEOUT = float(os.getenv("FIXED_READ_TIMEOUT", "25"))

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

# ============ TELEGRAM ============
def send_telegram_message(message, parse_mode='HTML'):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': parse_mode, 'disable_web_page_preview': False}
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            logging.error(f"Ошибка Telegram: {r.text}")
    except Exception as e:
        logging.error(f"Не удалось отправить в Telegram: {e}")

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
                    message = update.get('message')
                    if message and str(message.get('chat', {}).get('id')) == TELEGRAM_CHAT_ID:
                        text = message.get('text', '').strip()
                        if text == '/stop':
                            is_paused = True
                            send_telegram_message("⏸ Бот приостановлен. Для возобновления отправьте /start")
                            logging.info("Команда /stop - пауза")
                        elif text == '/start':
                            is_paused = False
                            send_telegram_message("▶ Бот продолжает работу")
                            logging.info("Команда /start - продолжение")
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
      proxy_timeout - connect/read timeout
      proxy_ssl     - SSL certificate / MITM proxy
      proxy_error   - CONNECT/SOCKS/reset/прочая transport ошибка
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
        return 'proxy_error', None, None if own_session else session


def _probe_proxy(proxy, profile):
    """Одна discovery-проверка. У каждой задачи своя Session — thread-safe."""
    return _make_request(
        proxy,
        profile,
        session=None,
        timeout=(PROBE_CONNECT_TIMEOUT, PROBE_READ_TIMEOUT),
    )


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
                timeout=(min(FIXED_CONNECT_TIMEOUT, 5.0), FIXED_READ_TIMEOUT),
            )
            if retry_result == 'success':
                fixed_proxy = old_proxy
                fixed_profile = old_profile
                fixed_session = retry_session
                proxy_manager.mark_success(old_proxy)
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

        results = []
        # Ждём весь маленький batch. Даже если один ответил раньше, максимум ещё
        # ~3.5 сек на connect timeout остальных, зато не оставляем фоновые Session.
        with ThreadPoolExecutor(max_workers=len(batch), thread_name_prefix='proxy-probe') as executor:
            future_to_proxy = {
                executor.submit(_probe_proxy, proxy, profile): proxy
                for proxy in batch
            }
            for future in as_completed(future_to_proxy):
                proxy = future_to_proxy[future]
                try:
                    result, html, session = future.result()
                except Exception as e:
                    logging.error(f"Ошибка probe worker для {proxy}: {e}")
                    result, html, session = 'proxy_error', None, None
                results.append((proxy, result, html, session))

        successes = []
        for proxy, result, html, session in results:
            if result == 'success':
                proxy_manager.mark_success(proxy)
                successes.append((proxy, html, session))
            elif result == 'profile_error':
                close_session(session)
            else:
                close_session(session)
                proxy_manager.mark_failure(proxy, result, reason=result)

        if successes:
            # as_completed() сохранил приблизительный порядок скорости ответа —
            # берём первый успешный; остальные успехи запоминаем как warm backup.
            winner_proxy, winner_html, winner_session = successes[0]
            for backup_proxy, _, backup_session in successes[1:]:
                logging.info(f"🟢 Запомнен запасной успешный proxy {backup_proxy}")
                close_session(backup_session)

            fixed_proxy = winner_proxy
            fixed_profile = profile
            fixed_session = winner_session
            logging.info(
                f"✅ Найдена рабочая пара: proxy {winner_proxy}, профиль {profile['name']}; "
                f"проверено {attempts} proxy за {time.monotonic() - started:.1f} сек."
            )
            return winner_html

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
    return "eBay бот работает (Великобритания, adaptive parallel UK mode)"

@app.route('/health')
def health():
    return "OK", 200

if __name__ == "__main__":
    send_telegram_message("🚀 Бот запущен (Великобритания, adaptive parallel UK mode). Команды /stop /start")
    threading.Thread(target=telegram_listener, daemon=True).start()
    worker_thread = threading.Thread(target=bot_worker, daemon=False)
    worker_thread.start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
