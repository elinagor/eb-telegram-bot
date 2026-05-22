import os
import sys
import ssl
import time
import random
import re
import json
import threading
import logging
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from flask import Flask
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

# Отключаем проверку SSL
ssl._create_default_https_context = ssl._create_unverified_context

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
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "60"))
DATABASE_URL = os.getenv("DATABASE_URL")
PROXY_LIST_URL = os.getenv("PROXY_LIST")
PROXY_REFRESH_INTERVAL = 15 * 60

MAX_ITEMS = 20
MAX_SEARCH_ATTEMPTS = 20
RETRY_DELAY = 2
GBP_TO_UAH = 60
EXTRA_DELIVERY_COST = 120

if '?' in EBAY_SEARCH_URL:
    EBAY_SEARCH_URL += '&LH_PrefLoc=3&_ipg=240&_sop=10'
else:
    EBAY_SEARCH_URL += '?LH_PrefLoc=3&_ipg=240&_sop=10'

if not all([EBAY_SEARCH_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DATABASE_URL]):
    logging.error("Не хватает переменных окружения.")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
app = Flask(__name__)

is_paused = False

# ============ ПРОФИЛИ БРАУЗЕРОВ ============
BROWSER_PROFILES = [
    {
        'name': 'Chrome146',
        'ua': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        'sec_ch_ua': '"Google Chrome";v="146", "Chromium";v="146", "Not_A Brand";v="99"',
        'impersonate': "chrome146",
        'disabled': False
    },
    {
        'name': 'Firefox147',
        'ua': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0",
        'sec_ch_ua': '"Firefox";v="147", "Not_A Brand";v="99"',
        'impersonate': "firefox147",
        'disabled': False
    },
    {
        'name': 'Safari26.4',
        'ua': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.4 Safari/605.1.15",
        'sec_ch_ua': '"Safari";v="26", "Not_A Brand";v="99"',
        'impersonate': "safari260",
        'disabled': False
    },
    {
        'name': 'Chrome_Universal',
        'ua': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        'sec_ch_ua': '"Google Chrome";v="146", "Chromium";v="146", "Not_A Brand";v="99"',
        'impersonate': "chrome",
        'disabled': False
    },
    {
        'name': 'Firefox_Universal',
        'ua': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0",
        'sec_ch_ua': '"Firefox";v="147", "Not_A Brand";v="99"',
        'impersonate': "firefox",
        'disabled': False
    },
    {
        'name': 'Safari_Universal',
        'ua': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.4 Safari/605.1.15",
        'sec_ch_ua': '"Safari";v="26", "Not_A Brand";v="99"',
        'impersonate': "safari",
        'disabled': False
    },
    {
        'name': 'Chrome148_Custom',
        'ua': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
        'sec_ch_ua': '"Google Chrome";v="148", "Chromium";v="148", "Not_A Brand";v="99"',
        'impersonate': "chrome",
        'disabled': False
    },
]

def get_random_profile():
    active = [p for p in BROWSER_PROFILES if not p.get('disabled', False)]
    if not active:
        BROWSER_PROFILES[0]['disabled'] = False
        active = [BROWSER_PROFILES[0]]
    return random.choice(active)

def disable_profile(profile_name):
    for p in BROWSER_PROFILES:
        if p['name'] == profile_name:
            p['disabled'] = True
            logging.warning(f"Профиль {profile_name} отключён (не поддерживается curl_cffi)")
            break

# ============ МЕНЕДЖЕР ПРОКСИ ============
class ProxyManager:
    def __init__(self, proxy_list_url=None):
        self.proxy_list_url = proxy_list_url
        self.proxies = []
        self.lock = threading.Lock()
        self.last_refresh = 0
        self.refresh_interval = PROXY_REFRESH_INTERVAL

    def fetch_proxies_from_api(self):
        if not self.proxy_list_url:
            return []
        try:
            logging.info(f"Загрузка прокси из {self.proxy_list_url}")
            resp = requests.get(self.proxy_list_url, timeout=20)
            if resp.status_code != 200:
                logging.error(f"Ошибка загрузки прокси: HTTP {resp.status_code}")
                return []
            text = resp.text.strip()
            proxies = []
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                if '://' not in line:
                    line = 'http://' + line
                proxies.append(line)
            logging.info(f"Загружено {len(proxies)} прокси")
            return proxies
        except Exception as e:
            logging.error(f"Ошибка при получении прокси: {e}")
            return []

    def refresh_proxies(self):
        with self.lock:
            now = time.time()
            if self.proxies and (now - self.last_refresh) < self.refresh_interval:
                return
            new_proxies = self.fetch_proxies_from_api()
            if new_proxies:
                self.proxies = new_proxies
                self.last_refresh = now
                logging.info(f"Пул прокси обновлён: {len(self.proxies)} доступно")
            else:
                if not self.proxies:
                    logging.warning("Не удалось загрузить прокси, работаем без прокси")
                else:
                    logging.warning("Не удалось обновить прокси, продолжаем использовать старые")

    def get_random_proxy(self):
        self.refresh_proxies()
        with self.lock:
            if not self.proxies:
                return None
            return random.choice(self.proxies)

    def mark_bad_proxy(self, bad_proxy):
        with self.lock:
            if bad_proxy in self.proxies:
                self.proxies.remove(bad_proxy)
                logging.info(f"Прокси {bad_proxy} удалён (нерабочий/заблокирован). Осталось {len(self.proxies)} прокси")

proxy_manager = ProxyManager(PROXY_LIST_URL)

# ============ ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ДЛЯ ФИКСИРОВАННОЙ ПАРЫ ============
fixed_proxy = None
fixed_profile = None
fixed_profile_failures = 0
MAX_FIXED_FAILURES = 2

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

# ============ ФУНКЦИЯ ЗАПРОСА С ФИКСАЦИЕЙ ============
def fetch_ebay_html_with_fixed_pair():
    global fixed_proxy, fixed_profile, fixed_profile_failures

    cookies = {'ebay': '%2F', 'm': 'GB', 's': 'UK', 'siteid': '3'}

    if fixed_proxy is not None and fixed_profile is not None:
        logging.info(f"🔁 Используем зафиксированную пару: прокси {fixed_proxy}, профиль {fixed_profile['name']}")
        success, html = _make_request(fixed_proxy, fixed_profile, cookies)
        if success:
            fixed_profile_failures = 0
            return html
        else:
            fixed_profile_failures += 1
            logging.warning(f"Зафиксированная пара не сработала (ошибка {fixed_profile_failures}/{MAX_FIXED_FAILURES})")
            if fixed_profile_failures >= MAX_FIXED_FAILURES:
                logging.info("Сбрасываем зафиксированную пару, ищем новую...")
                fixed_proxy = None
                fixed_profile = None
                fixed_profile_failures = 0
            else:
                return None

    for attempt in range(1, MAX_SEARCH_ATTEMPTS + 1):
        proxy = proxy_manager.get_random_proxy()
        profile = get_random_profile()
        logging.info(f"🔍 Поиск рабочей пары: попытка {attempt}/{MAX_SEARCH_ATTEMPTS}, прокси {proxy}, профиль {profile['name']}")
        success, html = _make_request(proxy, profile, cookies)
        if success:
            logging.info(f"✅ Найдена рабочая пара: прокси {proxy}, профиль {profile['name']}")
            fixed_proxy = proxy
            fixed_profile = profile
            fixed_profile_failures = 0
            return html
        else:
            continue

    logging.error("❌ Не удалось найти рабочую пару после всех попыток")
    return None

def _make_request(proxy, profile, cookies):
    headers = {
        'User-Agent': profile['ua'],
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-GB,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': 'https://www.ebay.co.uk/',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
        'X-EBay-Site-Id': '3',
        'Sec-Ch-Ua': profile['sec_ch_ua'],
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"' if 'Windows' in profile['ua'] else '"macOS"',
    }
    proxies_dict = {'http': proxy, 'https': proxy} if proxy else None

    try:
        response = cffi_requests.get(
            EBAY_SEARCH_URL,
            headers=headers,
            cookies=cookies,
            impersonate=profile['impersonate'],
            proxies=proxies_dict,
            verify=False,
            timeout=30,
            allow_redirects=True
        )

        if response.status_code == 200:
            text_lower = response.text.lower()
            if 'pardon our interruption' in text_lower or 'access denied' in text_lower or 'robot' in text_lower:
                logging.warning(f"🚫 БЛОКИРОВКА (страница защиты) для прокси {proxy}, профиль {profile['name']}")
                if proxy:
                    proxy_manager.mark_bad_proxy(proxy)
                return False, None
            else:
                logging.info(f"✅ УСПЕШНО c прокси {proxy}, профиль {profile['name']}")
                return True, response.text
        elif response.status_code == 403:
            logging.warning(f"🚫 БЛОКИРОВКА (HTTP 403) для прокси {proxy}, профиль {profile['name']}")
            if proxy:
                proxy_manager.mark_bad_proxy(proxy)
            return False, None
        else:
            logging.warning(f"⚠️ НЕУДАЧА: HTTP {response.status_code} для прокси {proxy}, профиль {profile['name']}")
            if proxy:
                proxy_manager.mark_bad_proxy(proxy)
            return False, None

    except Exception as e:
        error_msg = str(e)
        logging.error(f"❌ ОШИБКА для прокси {proxy}, профиль {profile['name']}: {error_msg}")
        if 'not supported' in error_msg:
            disable_profile(profile['name'])
        if proxy:
            proxy_manager.mark_bad_proxy(proxy)
        return False, None

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
    """
    Возвращает итоговую сумму в гривнах (int) или None, если нельзя вычислить.
    """
    if not price_str or price_str == "Цена не указана (не GBP)" or "до" in price_str:
        return None

    price_num = None
    # Для аукциона с Buy It Now используем цену Buy It Now
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
        return
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
            # Добавляем итоговую сумму, только если:
            # - товар НЕ аукцион, ИЛИ (аукцион И есть Buy It Now)
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
            check_and_send_new_items()
            wait = max(60, CHECK_INTERVAL + random.uniform(-30, 60))
            logging.info(f"Следующая проверка через {wait:.0f} секунд.")
            time.sleep(wait)
        except Exception as e:
            logging.error(f"Ошибка в основном цикле: {e}", exc_info=True)
            time.sleep(120)

@app.route('/')
def index():
    return "eBay бот работает (итоговая сумма только для Buy It Now аукционов)"

@app.route('/health')
def health():
    return "OK", 200

if __name__ == "__main__":
    send_telegram_message("🚀 Бот запущен (Великобритания, исправлено условие показа итоговой суммы). Интервал 60 сек, команды /stop /start")
    threading.Thread(target=telegram_listener, daemon=True).start()
    worker_thread = threading.Thread(target=bot_worker, daemon=False)
    worker_thread.start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
