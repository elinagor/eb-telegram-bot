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

if not all([EBAY_SEARCH_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, BRIGHT_DATA_PROXY_URL, DATABASE_URL]):
    logging.error("Не хватает переменных окружения. Проверьте .env или настройки Render.")
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
    logging.info("Таблица seen_items готова (PostgreSQL)")

def get_seen_ids():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT item_id FROM seen_items")
            rows = cur.fetchall()
            return {row[0] for row in rows}

def add_seen_ids_batch(item_ids):
    if not item_ids:
        return
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            data = [(item_id, datetime.now()) for item_id in item_ids]
            execute_values(
                cur,
                "INSERT INTO seen_items (item_id, first_seen) VALUES %s ON CONFLICT (item_id) DO NOTHING",
                data
            )
        conn.commit()
    logging.info(f"Добавлено {len(item_ids)} ID в базу")

def is_db_empty():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT EXISTS (SELECT 1 FROM seen_items LIMIT 1)")
            return not cur.fetchone()[0]

# ============ ФУНКЦИИ ДЛЯ EBAY ============
def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message,
        'parse_mode': 'HTML',
        'disable_web_page_preview': False
    }
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
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Referer': 'https://www.ebay.com/',
        'Sec-Ch-Ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="99"',
        'Upgrade-Insecure-Requests': '1',
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = cffi_requests.get(
                EBAY_SEARCH_URL,
                headers=headers,
                impersonate="chrome131",
                proxies=proxies,
                verify=False,
                timeout=30
            )
            if response.status_code == 200:
                logging.info(f"Страница eBay успешно загружена (попытка {attempt})")
                return response.text
            else:
                logging.warning(f"Попытка {attempt}/{MAX_RETRIES}: HTTP {response.status_code}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
        except Exception as e:
            logging.error(f"Попытка {attempt}/{MAX_RETRIES}: ошибка запроса: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    logging.error("Не удалось загрузить страницу после всех попыток")
    return None

def extract_item_id(url):
    if not url or '/itm/' not in url:
        return None
    try:
        return url.split('/itm/')[1].split('?')[0]
    except IndexError:
        return None

def clean_title(title):
    if not title:
        return ""
    title = re.sub(r'(?i)new\s*listing', '', title)
    title = re.sub(r'(?i)\blisting\b', '', title)
    title = re.sub(r'(?i)\bnew\b', '', title)
    title = re.sub(r'[^\w\s\$€£¥]', ' ', title)
    title = re.sub(r'\s+', ' ', title).strip()
    return title

def is_valid_price(text):
    """Проверяет, похоже ли на цену."""
    if not text or len(text) > 20:
        return False
    has_digit = re.search(r'\d', text)
    has_currency = re.search(r'[$€£¥]', text)
    has_decimal = re.search(r'[,.]', text)
    if not has_digit:
        return False
    if not (has_currency or has_decimal):
        return False
    # Если содержит слова shipping, delivery, handling — не цена
    if re.search(r'(?i)(shipping|delivery|handling|postage|versand|lieferung)', text):
        return False
    return True

def extract_price_jsonld(card, url=None, soup=None):
    """Извлекает цену из JSON-LD внутри карточки или общего скрипта."""
    # Сначала ищем script внутри карточки
    script = card.find('script', type='application/ld+json')
    if script and script.string:
        try:
            data = json.loads(script.string)
            if isinstance(data, dict):
                # Ищем offers
                if 'offers' in data:
                    offers = data['offers']
                    if isinstance(offers, dict):
                        price = offers.get('price')
                        currency = offers.get('priceCurrency', '')
                        if price and price != '0':
                            if currency:
                                return f"{currency} {price}"
                            return str(price)
                    elif isinstance(offers, list) and len(offers) > 0:
                        first = offers[0]
                        price = first.get('price')
                        currency = first.get('priceCurrency', '')
                        if price and price != '0':
                            if currency:
                                return f"{currency} {price}"
                            return str(price)
                elif 'price' in data:
                    price = data['price']
                    if price and price != '0':
                        return str(price)
        except:
            pass
    
    # Если внутри карточки нет, ищем общий JSON-LD по всему soup и сопоставляем по URL
    if soup and url:
        for script in soup.find_all('script', type='application/ld+json'):
            if not script.string:
                continue
            try:
                data = json.loads(script.string)
                if isinstance(data, dict) and 'url' in data and data['url'] == url:
                    if 'offers' in data:
                        offers = data['offers']
                        if isinstance(offers, dict):
                            price = offers.get('price')
                            currency = offers.get('priceCurrency', '')
                            if price and price != '0':
                                if currency:
                                    return f"{currency} {price}"
                                return str(price)
            except:
                continue
    return None

def extract_price_css(card):
    """Запасной метод: поиск цены через CSS-селекторы с фильтрацией."""
    # Приоритетные селекторы
    selectors = [
        'span.s-item__price',
        '[data-testid="item-price"]',
        '.s-item__detail .s-item__price',
        'span.POSITIVE',
        'span.vi-price',
    ]
    for sel in selectors:
        elem = card.select_one(sel)
        if elem:
            text = elem.get_text(strip=True)
            # Убираем перечеркнутые (старые цены)
            if elem.find('span', class_='s-item__price--strikethrough'):
                continue
            if is_valid_price(text):
                return text
    
    # Если не нашли, пробуем любой элемент с классом содержащим 'price'
    for elem in card.select('[class*="price"]'):
        text = elem.get_text(strip=True)
        if is_valid_price(text):
            return text
    return None

def parse_ebay_listings(html, max_items=MAX_ITEMS):
    if not html:
        return {}
    soup = BeautifulSoup(html, 'html.parser')
    items_data = {}

    cards = soup.select('li.s-item')
    if not cards:
        cards = soup.select('.s-item')
    
    if cards:
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
                          card.select_one('div.s-item__title') or
                          link)
            raw_title = title_elem.get_text(strip=True) if title_elem else ''
            title = clean_title(raw_title)
            if not title:
                title = clean_title(link.get_text(strip=True))
                if not title:
                    continue
            
            # Цена: сначала JSON-LD, потом CSS
            price = extract_price_jsonld(card, url, soup)
            if not price:
                price = extract_price_css(card)
            
            items_data[item_id] = {
                'url': url,
                'title': title,
                'price': price
            }
            processed += 1
        
        logging.info(f"Обработано {len(items_data)} товаров")
        return items_data
    
    # Резерв
    logging.warning("Карточки не найдены, поиск по ссылкам")
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
        raw_title = link.get_text(strip=True)
        title = clean_title(raw_title)
        if not title:
            continue
        price = None
        parent = link.parent
        for _ in range(4):
            if parent:
                price = extract_price_jsonld(parent, url, soup)
                if price:
                    break
                price = extract_price_css(parent)
                if price:
                    break
                parent = parent.parent
        items_data[item_id] = {'url': url, 'title': title, 'price': price}
    logging.info(f"Резерв: {len(items_data)} товаров")
    return items_data

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
    current_items = parse_ebay_listings(html)
    new_items = []
    for item_id, data in current_items.items():
        if item_id not in seen:
            new_items.append(data)
            logging.info(f"НОВЫЙ: {data['title'][:50]}... цена: {data['price']}")
    if new_items:
        for item in new_items:
            msg = f"🔹 <b>НОВЫЙ ТОВАР НА EBAY</b> 🔹\n\n<b>{item['title']}</b>\n\n"
            if item['price']:
                msg += f"💰 Цена: {item['price']}\n\n"
            else:
                msg += f"💰 Цена не указана\n\n"
            msg += f"🔗 <a href='{item['url']}'>Ссылка на товар</a>"
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
        send_telegram_message("✅ Бот запущен, начальный снимок сделан")
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
    return "eBay бот работает (цена через JSON-LD)"

@app.route('/health')
def health():
    return "OK", 200

if __name__ == "__main__":
    send_telegram_message("🚀 Бот запущен, цена из JSON-LD")
    thread = threading.Thread(target=bot_worker, daemon=False)
    thread.start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
