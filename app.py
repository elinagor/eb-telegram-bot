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

# Отключаем проверку SSL для Bright Data
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
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "120"))
BRIGHT_DATA_PROXY_URL = os.getenv("BRIGHT_DATA_PROXY_URL")
DATABASE_URL = os.getenv("DATABASE_URL")
MAX_ITEMS = 20
MAX_RETRIES = 20
RETRY_DELAY = 5

# Принудительная локализация США и доллары
if '?' in EBAY_SEARCH_URL:
    EBAY_SEARCH_URL += '&LH_PrefLoc=1&_ipg=240&_sop=10'
else:
    EBAY_SEARCH_URL += '?LH_PrefLoc=1&_ipg=240&_sop=10'

if not all([EBAY_SEARCH_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, BRIGHT_DATA_PROXY_URL, DATABASE_URL]):
    logging.error("Не хватает переменных окружения.")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
app = Flask(__name__)

# ============ ГЛОБАЛЬНЫЕ ФЛАГИ ============
is_paused = False
restart_flag = False
bot_worker_thread = None
worker_should_exit = False

# ============ РОТАЦИЯ USER-AGENT ============
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:144.0) Gecko/20100101 Firefox/144.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
]

# ============ РАБОТА С БАЗОЙ ДАННЫХ ============
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

# ============ ОТПРАВКА СООБЩЕНИЙ В TELEGRAM ============
def send_telegram_message(message, parse_mode='HTML'):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': parse_mode, 'disable_web_page_preview': False}
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            logging.error(f"Ошибка Telegram: {r.text}")
    except Exception as e:
        logging.error(f"Не удалось отправить в Telegram: {e}")

# ============ ОБРАБОТЧИК КОМАНД (LONG POLLING) ============
def telegram_listener():
    global is_paused, restart_flag, bot_worker_thread, worker_should_exit
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
                        elif text == '/restart':
                            if bot_worker_thread and bot_worker_thread.is_alive():
                                send_telegram_message("🔄 Перезапуск бота...")
                                logging.info("Команда /restart - мягкий перезапуск воркера")
                                worker_should_exit = True
                                bot_worker_thread.join(timeout=10)
                                # Запускаем новый воркер
                                worker_should_exit = False
                                bot_worker_thread = threading.Thread(target=bot_worker, daemon=False)
                                bot_worker_thread.start()
                                send_telegram_message("✅ Бот успешно перезапущен")
                            else:
                                send_telegram_message("⚠ Воркер не активен, запускаю новый")
                                worker_should_exit = False
                                bot_worker_thread = threading.Thread(target=bot_worker, daemon=False)
                                bot_worker_thread.start()
            time.sleep(1)
        except Exception as e:
            logging.error(f"Ошибка в слушателе Telegram: {e}")
            time.sleep(5)

# ============ ЗАПРОС К EBAY (Аналогично предыдущему коду) ============
def fetch_ebay_html_with_retry():
    proxies = {"http": BRIGHT_DATA_PROXY_URL, "https": BRIGHT_DATA_PROXY_URL}
    cookies = {'ebay': '%2F', 'm': 'USA', 's': 'S0'}
    for attempt in range(1, MAX_RETRIES + 1):
        current_ua = random.choice(USER_AGENTS)
        headers = {
            'User-Agent': current_ua,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.ebay.com/',
            'X-EBay-Site-Id': '0',
            'Sec-Ch-Ua': '"Google Chrome";v="142", "Chromium";v="142", "Not_A Brand";v="99"',
            'Upgrade-Insecure-Requests': '1',
        }
        if attempt > 1:
            time.sleep(random.uniform(1.5, 3.5))
        try:
            response = cffi_requests.get(
                EBAY_SEARCH_URL,
                headers=headers,
                cookies=cookies,
                impersonate="chrome142",
                proxies=proxies,
                verify=False,
                timeout=35
            )
            if response.status_code == 200:
                logging.info(f"✅ Загружено (попытка {attempt}, UA={current_ua[:40]}...)")
                return response.text
            elif response.status_code == 403:
                logging.warning(f"⚠️ 403 Forbidden (попытка {attempt})")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY + random.uniform(2, 5))
            else:
                logging.warning(f"Попытка {attempt}/{MAX_RETRIES}: HTTP {response.status_code}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
        except Exception as e:
            logging.error(f"Попытка {attempt}/{MAX_RETRIES}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY + random.uniform(1, 3))
    return None

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
    title = re.sub(r'[^\w\s\$€£¥]', ' ', title)
    title = re.sub(r'\s+', ' ', title).strip()
    return title

def is_usd_price(text):
    if not text: return False
    if re.search(r'^\$|\s\$|USD\s*\$\d+', text):
        return True
    if re.search(r'[€£¥]|ZAR|EUR|GBP|JPY|DKK|NOK|SEK|CHF', text, re.I):
        return False
    return re.search(r'\d', text) is not None

def extract_price_jsonld(card, url=None, soup=None):
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
        if curr == 'USD' or (curr == '' and str(price).startswith('$')):
            return f"${price}"
    for price, curr in candidates:
        if curr:
            return f"{curr} {price}"
        else:
            return str(price)
    return None

def extract_price_css(card):
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
        if is_usd_price(cand):
            parts = cand.split()
            for p in parts:
                if is_usd_price(p):
                    return p
            return cand
    if candidates:
        return candidates[0]
    return None

def extract_shipping(card, item_price=None):
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
                            if item_price and str(shipping) == str(item_price) and currency == 'USD':
                                return None
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
                            if item_price and str(shipping) == str(item_price) and currency == 'USD':
                                return None
                            return amount
        except:
            pass

    shipping_selectors = [
        'span.s-item__shipping', 'div.s-item__shipping',
        'span.s-item__logisticsCost', 'span.s-item__delivery',
        'span.su-styled-text', '.su-styled-text.secondary.large',
        '[class*="shippingCost"]'
    ]
    for sel in shipping_selectors:
        for elem in card.select(sel):
            text = elem.get_text(strip=True)
            text = re.sub(r'\s+', ' ', text)
            if not text:
                continue
            if re.search(r'(?i)(buy it now|best offer|make offer|watch|add to cart)', text):
                continue
            if not re.search(r'(?i)(free|shipping|delivery|postage|shipping cost)', text):
                continue
            if 'free' in text.lower():
                return "Бесплатно"
            match = re.search(r'([$€£¥]\s*[\d,]+\.?\d*)', text)
            if match:
                price_candidate = match.group(1)
                if item_price and price_candidate == item_price:
                    continue
                return price_candidate
            if len(text) < 30 and not re.search(r'\d', text):
                if 'free' in text.lower():
                    return "Бесплатно"
                return text

    html = str(card)
    if re.search(r'(?i)free\s+shipping', html):
        return "Бесплатно"
    match = re.search(r'(?i)\+?\s*([$€£¥]\s*[\d,]+\.?\d*)\s*(delivery|shipping)', html)
    if match:
        pc = match.group(1)
        if not (item_price and pc == item_price):
            return pc
    match = re.search(r'(?i)shipping:\s*([$€£¥]\s*[\d,]+\.?\d*)', html)
    if match:
        pc = match.group(1)
        if not (item_price and pc == item_price):
            return pc
    return None

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
            url = 'https://www.ebay.com' + url
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
        if price and not is_usd_price(price):
            price = None
        shipping = extract_shipping(card, item_price=price)
        if shipping and shipping != "Бесплатно" and not is_usd_price(shipping):
            shipping = None
        items[item_id] = {'url': url, 'title': title, 'price': price, 'shipping': shipping}
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
            url = 'https://www.ebay.com' + url
        item_id = extract_item_id(url)
        if not item_id:
            continue
        title = clean_title(link.get_text(strip=True))
        if not title:
            continue
        price = None
        shipping = None
        parent = link.parent
        for _ in range(5):
            if parent:
                price = extract_price_jsonld(parent, url) or extract_price_css(parent)
                if price and not is_usd_price(price):
                    price = None
                shipping = extract_shipping(parent, item_price=price)
                if shipping and shipping != "Бесплатно" and not is_usd_price(shipping):
                    shipping = None
                if price or shipping:
                    break
                parent = parent.parent
        items[item_id] = {'url': url, 'title': title, 'price': price, 'shipping': shipping}
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
            logging.info(f"НОВЫЙ: {data['title'][:50]}... цена: {data['price']}, доставка: {data.get('shipping')}")
    if new:
        for item in new:
            msg = f"🔹 <b>НОВЫЙ ТОВАР НА EBAY</b> 🔹\n\n<b>{item['title']}</b>\n\n"
            if item['price']:
                msg += f"💰 Цена: {item['price']}\n"
            else:
                msg += f"💰 Цена не указана (не USD)\n"
            if item['shipping']:
                msg += f"🚚 Доставка: {item['shipping']}\n"
            else:
                msg += f"🚚 Доставка: не указана\n"
            msg += f"\n🔗 <a href='{item['url']}'>Ссылка на товар</a>"
            send_telegram_message(msg)
            add_seen_ids_batch([item['id']])
            time.sleep(1)
    else:
        logging.info("Новых нет")

def bot_worker():
    global is_paused, worker_should_exit
    logging.info("🤖 Бот-воркер запущен")
    init_db()
    if is_db_empty():
        if not perform_initial_snapshot():
            send_telegram_message("❌ Ошибка инициализации")
            return
        send_telegram_message("✅ Бот запущен, начальный снимок сделан")
    else:
        send_telegram_message("✅ Бот перезапущен")
    while not worker_should_exit:
        if is_paused:
            time.sleep(2)
            continue
        try:
            check_and_send_new_items()
            wait = max(60, CHECK_INTERVAL + random.uniform(-30, 60))
            logging.info(f"Следующая проверка через {wait:.0f} секунд.")
            # Выход из цикла, если запрошен перезапуск (спим с частыми проверками)
            for _ in range(int(wait)):
                if worker_should_exit:
                    break
                time.sleep(1)
        except Exception as e:
            logging.error(f"Ошибка в основном цикле: {e}", exc_info=True)
            time.sleep(120)
    logging.info("Бот-воркер завершает работу по запросу перезапуска")

@app.route('/')
def index():
    return "eBay бот работает (команды /stop /start /restart)"

@app.route('/health')
def health():
    return "OK", 200

if __name__ == "__main__":
    send_telegram_message("🚀 Бот запущен. Доступны команды: /stop, /start, /restart")
    # Запускаем слушателя команд (демонический)
    threading.Thread(target=telegram_listener, daemon=True).start()
    # Запускаем основной воркер в не-демоническом потоке
    global bot_worker_thread
    bot_worker_thread = threading.Thread(target=bot_worker, daemon=False)
    bot_worker_thread.start()
    # Запускаем Flask
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
