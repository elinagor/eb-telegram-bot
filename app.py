import os
import sys
import ssl
import time
import random
import re
import json
import threading
import logging
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

# Если в URL нет параметров локации, добавим для США
if EBAY_SEARCH_URL and '?' in EBAY_SEARCH_URL:
    EBAY_SEARCH_URL += '&LH_PrefLoc=1'  # только товары из США
else:
    EBAY_SEARCH_URL += '?LH_PrefLoc=1'

if not all([EBAY_SEARCH_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, BRIGHT_DATA_PROXY_URL, DATABASE_URL]):
    logging.error("Не хватает переменных окружения.")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
app = Flask(__name__)

# ============ РАБОТА С БАЗОЙ ДАННЫХ ============
def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def init_db():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS seen_items (
                    item_id TEXT PRIMARY KEY,
                    first_seen TIMESTAMP
                )
            """)
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

# ============ ФУНКЦИИ ДЛЯ EBAY ============
def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': 'HTML', 'disable_web_page_preview': False}
    try:
        import requests
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            logging.error(f"Ошибка Telegram: {r.text}")
    except Exception as e:
        logging.error(f"Не удалось отправить в Telegram: {e}")

def fetch_ebay_html_with_retry():
    proxies = {"http": BRIGHT_DATA_PROXY_URL, "https": BRIGHT_DATA_PROXY_URL}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.ebay.com/',
        'X-EBay-Site-Id': '0',  # США
        'Sec-Ch-Ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="99"',
        'Upgrade-Insecure-Requests': '1',
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = cffi_requests.get(EBAY_SEARCH_URL, headers=headers, impersonate="chrome131", proxies=proxies, verify=False, timeout=30)
            if response.status_code == 200:
                logging.info(f"Страница загружена (попытка {attempt})")
                return response.text
            else:
                logging.warning(f"Попытка {attempt}/{MAX_RETRIES}: HTTP {response.status_code}")
                if attempt < MAX_RETRIES: time.sleep(RETRY_DELAY)
        except Exception as e:
            logging.error(f"Попытка {attempt}/{MAX_RETRIES}: {e}")
            if attempt < MAX_RETRIES: time.sleep(RETRY_DELAY)
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
    """Проверяет, является ли текст ценой в долларах (начинается с $ или содержит USD $)."""
    if not text:
        return False
    # Если строка явно начинается с $ или содержит "$"
    if re.search(r'^\$|\s\$|USD\s*\$\d+', text):
        return True
    # Если в тексте нет знака доллара, но есть другая валюта - не USD
    if re.search(r'[€£¥]|ZAR|EUR|GBP|JPY|RUB|CNY', text, re.I):
        return False
    # Если есть цифры и возможно точка/запятая, но нет явного USD - всё равно пытаемся взять, но с меньшим приоритетом
    return re.search(r'\d', text) is not None

def extract_price_jsonld(card, url=None, soup=None):
    # Сначала ищем внутри карточки
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
    # Поиск по всему soup (запасной) – но нужно сопоставить с url
    if soup and url:
        for script in soup.find_all('script', type='application/ld+json'):
            if not script.string: continue
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
    # Выбираем цену: сначала USD, затем любую
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
    # Ищем все элементы с ценой
    candidates = []
    selectors = ['span.s-item__price', '[data-testid="item-price"]', '.s-item__detail .s-item__price']
    for sel in selectors:
        for elem in card.select(sel):
            text = elem.get_text(strip=True)
            if text:
                candidates.append(text)
    # Дополнительный поиск по классам содержащим price
    for elem in card.select('[class*="price"]'):
        text = elem.get_text(strip=True)
        if text:
            candidates.append(text)
    # Фильтруем: сначала USD, потом другие
    for cand in candidates:
        if is_usd_price(cand):
            # Очищаем: оставляем только первую часть до пробела, если есть
            parts = cand.split()
            for p in parts:
                if is_usd_price(p):
                    return p
            return cand
    if candidates:
        return candidates[0]
    return None

def extract_shipping(card, url=None, soup=None):
    # 1) JSON-LD
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
                            return f"{currency} {shipping}" if currency else str(shipping)
                elif isinstance(offers, list) and len(offers) > 0:
                    first = offers[0]
                    shipping = first.get('shippingCost')
                    if shipping is not None:
                        if shipping == 0 or str(shipping) == '0':
                            return "Бесплатно"
                        if isinstance(shipping, (int, float)):
                            currency = first.get('priceCurrency', '')
                            return f"{currency} {shipping}" if currency else str(shipping)
        except:
            pass
    # 2) Селекторы для доставки, включая su-styled-text
    shipping_selectors = [
        'span.s-item__shipping', '.s-item__shipping', 'div.s-item__shipping',
        'span.s-item__logisticsCost', '.s-item__logisticsCost',
        'span.s-item__delivery', '.s-item__delivery',
        'span.su-styled-text', 'span.su-styled-text.secondary.large',
        '[class*="shipping"]', '[class*="logistics"]', '[class*="delivery"]',
    ]
    for sel in shipping_selectors:
        elem = card.select_one(sel)
        if elem:
            text = elem.get_text(strip=True)
            text = re.sub(r'\s+', ' ', text)
            if text:
                if 'free' in text.lower():
                    return "Бесплатно"
                if re.search(r'\d', text) and re.search(r'[$€£¥]', text):
                    return text
                if len(text) < 30 and not text.lower() in ('shipping', 'delivery', 'logistics'):
                    return text
    # 3) Регулярные выражения
    html = str(card)
    if re.search(r'(?i)free\s+shipping', html):
        return "Бесплатно"
    # Ищем "+$5.98 delivery"
    match = re.search(r'(?i)\+?\s*([$€£¥]\s*[\d,]+\.?\d*)\s*(delivery|shipping)', html)
    if match:
        return match.group(1).strip()
    # Старые шаблоны
    patterns = [r'(?i)shipping:\s*([$€£¥]\s*[\d,]+\.?\d*)', r'(?i)delivery:\s*([$€£¥]\s*[\d,]+\.?\d*)']
    for pat in patterns:
        m = re.search(pat, html)
        if m:
            return m.group(1)
    return None

def parse_ebay_listings(html, max_items=MAX_ITEMS):
    if not html:
        return {}
    soup = BeautifulSoup(html, 'html.parser')
    cards = soup.select('li.s-item')
    if not cards:
        cards = soup.select('.s-item')
    if not cards:
        # Резерв по ссылкам
        return parse_ebay_listings_fallback(soup, max_items)
    items_data = {}
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
        # Название
        title_elem = (card.select_one('div.s-item__title span[role="heading"]') or
                      card.select_one('span[role="heading"]') or
                      card.select_one('div.s-item__title') or link)
        title = clean_title(title_elem.get_text(strip=True) if title_elem else '')
        if not title:
            title = clean_title(link.get_text(strip=True))
            if not title:
                continue
        # Цена
        price = extract_price_jsonld(card, url, soup)
        if not price:
            price = extract_price_css(card)
        # Доставка
        shipping = extract_shipping(card, url, soup)
        items_data[item_id] = {'url': url, 'title': title, 'price': price, 'shipping': shipping}
        processed += 1
    logging.info(f"Обработано {len(items_data)} товаров")
    return items_data

def parse_ebay_listings_fallback(soup, max_items):
    items_data = {}
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
                shipping = extract_shipping(parent, url)
                if price or shipping:
                    break
                parent = parent.parent
        items_data[item_id] = {'url': url, 'title': title, 'price': price, 'shipping': shipping}
    return items_data

def perform_initial_snapshot():
    html = fetch_ebay_html_with_retry()
    if not html:
        return False
    items = parse_ebay_listings(html, max_items=50)
    if not items:
        return False
    add_seen_ids_batch(list(items.keys()))
    logging.info(f"Начальный снимок: {len(items)} товаров")
    return True

def check_and_send_new_items():
    seen = get_seen_ids()
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
                msg += f"💰 Цена не указана\n"
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
    time.sleep(5)
    init_db()
    if is_db_empty():
        if not perform_initial_snapshot():
            send_telegram_message("❌ Ошибка инициализации")
            return
        send_telegram_message("✅ Бот запущен")
    else:
        send_telegram_message("✅ Бот перезапущен")
    while True:
        try:
            check_and_send_new_items()
            wait = max(60, CHECK_INTERVAL + random.uniform(-30, 60))
            time.sleep(wait)
        except Exception as e:
            logging.error(f"Ошибка: {e}", exc_info=True)
            time.sleep(120)

@app.route('/')
def index():
    return "eBay бот (USD + доставка)"

@app.route('/health')
def health():
    return "OK", 200

if __name__ == "__main__":
    send_telegram_message("🚀 Бот запущен, приоритет USD, улучшена доставка")
    threading.Thread(target=bot_worker, daemon=False).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
