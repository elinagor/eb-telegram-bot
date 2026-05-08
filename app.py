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
    """
    Удаляет 'new listing' и 'listing' из любого места названия (даже слитно)
    """
    if not title:
        return ""
    # 1) Удаляем 'new listing' с пробелом или без в любом регистре
    title = re.sub(r'(?i)new\s*listing', '', title)
    # 2) Удаляем 'listing' как отдельную подстроку (учитывая границы слов, но также и слитно)
    title = re.sub(r'(?i)\blisting\b', '', title)
    # 3) Удаляем 'new' (отдельное слово)
    title = re.sub(r'(?i)\bnew\b', '', title)
    # 4) Убираем повторяющиеся или конечные символы-разделители
    title = re.sub(r'[^\w\s\$€£¥]', ' ', title)
    title = re.sub(r'\s+', ' ', title).strip()
    # 5) Если после чистки строка пустая, вернуть исходную (но без 'listing'?)
    if not title:
        # Пробуем вырезать только если слово было в начале
        title = re.sub(r'(?i)^listing', '', title)
    return title

def is_valid_price(text):
    """Проверяет, может ли текст быть ценой."""
    if not text or len(text) > 15:
        return False
    # Должен содержать хотя бы одну цифру и символ валюты или десятичную точку/запятую
    has_digit = re.search(r'\d', text)
    has_currency = re.search(r'[$€£¥]', text)
    has_decimal = re.search(r'[,.]', text)
    if not has_digit:
        return False
    if not (has_currency or has_decimal):
        return False
    # Если содержит буквы (кроме валюты), то скорее всего не цена
    # Но оставляем символы валюты, цифры, пробелы, точки, запятые
    valid_chars = r'[$€£¥\d\s\.,]'
    cleaned = re.sub(valid_chars, '', text)
    if cleaned:  # остались буквы (не валюта)
        return False
    return True

def extract_price_from_element(card):
    """
    Ищет цену внутри карточки. Возвращает строку с ценой или None.
    """
    # 1) Специфичные селекторы eBay
    specific_selectors = [
        'span.s-item__price',
        '.s-item__detail .s-item__price',
        'span.s-item__price--buynow',
        '[data-testid="item-price"]',
    ]
    for sel in specific_selectors:
        elem = card.select_one(sel)
        if elem:
            text = elem.get_text(strip=True)
            if is_valid_price(text):
                return clean_price_text(text)
    
    # 2) Любые элементы с классом, содержащим "price"
    for elem in card.select('[class*="price"]'):
        text = elem.get_text(strip=True)
        if is_valid_price(text):
            return clean_price_text(text)
    
    # 3) Поиск в атрибутах aria-label
    for elem in card.select('[aria-label]'):
        label = elem.get('aria-label', '')
        match = re.search(r'(?:price|Price):?\s*([\d\.,]+)', label)
        if match:
            candidate = match.group(1)
            if is_valid_price(candidate):
                return candidate
    
    # 4) JSON-LD
    script = card.find('script', type='application/ld+json')
    if script:
        try:
            data = json.loads(script.string)
            if isinstance(data, dict):
                if 'offers' in data and isinstance(data['offers'], dict):
                    price = data['offers'].get('price')
                    if price and is_valid_price(str(price)):
                        currency = data['offers'].get('priceCurrency', '')
                        if currency:
                            return f"{currency} {price}"
                        return str(price)
                elif 'price' in data:
                    price = data['price']
                    if price and is_valid_price(str(price)):
                        return str(price)
        except:
            pass
    
    # 5) Регулярные выражения на всей HTML карточки
    html = str(card)
    # Приоритет шаблонов: с валютой, с цифрами и точкой/запятой
    patterns = [
        r'([$€£¥]\s*[\d,]+\.?\d*)',               # $12.99
        r'([\d,]+\.?\d*\s*[$€£¥])',               # 12.99 $
        r'([A-Z]{3}\s*[\d,]+\.?\d*)',            # USD 12.99
        r'([\d,]+\.?\d*\s*[A-Z]{3})',            # 12.99 USD
        r'([\d,]+\.\d{2})',                      # 12.99 (без валюты, но с двумя десятичными)
    ]
    for pattern in patterns:
        matches = re.findall(pattern, html)
        for match in matches:
            if is_valid_price(match):
                return clean_price_text(match)
    return None

def clean_price_text(price_text):
    """Оставляет только первую часть похожую на цену."""
    # Убираем лишние слова типа "to", "–"
    price_text = re.sub(r'(?i)\s*(to|–|-|—)\s*', ' ', price_text)
    parts = re.split(r'\s+', price_text)
    for part in parts:
        if is_valid_price(part):
            return part
    return price_text

def parse_ebay_listings(html, max_items=MAX_ITEMS):
    if not html:
        return {}
    soup = BeautifulSoup(html, 'html.parser')
    items_data = {}

    # Основные карточки
    cards = soup.select('li.s-item')
    if not cards:
        cards = soup.select('.s-item')
    
    if cards:
        processed = 0
        for idx, card in enumerate(cards):
            if processed >= max_items:
                break
            # Ссылка
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
            
            # Цена
            price = extract_price_from_element(card)
            
            # Отладочный вывод для первых 3 карточек
            if idx < 3:
                logging.debug(f"DEBUG: Карточка {idx+1} -> Цена: {price}, Название: {title[:40]}")
            
            items_data[item_id] = {
                'url': url,
                'title': title,
                'price': price
            }
            processed += 1
        
        if items_data:
            logging.info(f"Основной парсинг: найдено {len(items_data)} товаров")
            return items_data
    
    # Резервный метод (поиск ссылок)
    logging.warning("Карточки не найдены, ищу по ссылкам /itm/")
    links = soup.find_all('a', href=True)
    itm_links = [link for link in links if '/itm/' in link['href']]
    itm_links = itm_links[:max_items]
    processed = 0
    for link in itm_links:
        if processed >= max_items:
            break
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
        for _ in range(3):
            if parent:
                price = extract_price_from_element(parent)
                if price:
                    break
                parent = parent.parent
        items_data[item_id] = {
            'url': url,
            'title': title,
            'price': price
        }
        processed += 1
    
    logging.info(f"Резервный метод: найдено {len(items_data)} товаров")
    return items_data

def perform_initial_snapshot():
    logging.info("Делаем начальный снимок (сохраняем ID из текущей выдачи)...")
    html = fetch_ebay_html_with_retry()
    if not html:
        logging.error("Не удалось загрузить eBay для начального снимка.")
        return False
    items = parse_ebay_listings(html, max_items=50)
    if not items:
        logging.warning("На странице не найдено товаров. Проверьте ссылку.")
        return False
    item_ids = list(items.keys())
    add_seen_ids_batch(item_ids)
    logging.info(f"Начальный снимок: добавлено {len(item_ids)} товаров.")
    return True

def check_and_send_new_items():
    seen = get_seen_ids()
    logging.info(f"В базе данных {len(seen)} уникальных товаров.")
    html = fetch_ebay_html_with_retry()
    if not html:
        logging.error("Не удалось получить HTML, пропускаем этот цикл.")
        return
    current_items = parse_ebay_listings(html, max_items=MAX_ITEMS)
    new_items = []
    for item_id, data in current_items.items():
        if item_id not in seen:
            new_items.append({
                'id': item_id,
                'url': data['url'],
                'title': data['title'],
                'price': data['price']
            })
            logging.info(f"НОВЫЙ товар: {item_id} -> '{data['title'][:60]}...' (цена: {data['price']})")
    
    if new_items:
        logging.info(f"Найдено новых товаров: {len(new_items)}. Отправляем в Telegram...")
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
        logging.info("Новых товаров не найдено.")

def bot_worker():
    logging.info("🔄 Фоновый поток запущен")
    time.sleep(5)
    
    init_db()
    if is_db_empty():
        logging.info("База данных пуста. Выполняется начальный снимок...")
        if not perform_initial_snapshot():
            send_telegram_message("❌ Ошибка инициализации: не удалось загрузить eBay. Бот остановлен.")
            return
        send_telegram_message("✅ Бот запущен: начальный снимок сделан. Отслеживаю только новые товары.")
    else:
        logging.info("База данных уже содержит товары. Пропускаем начальный снимок.")
        send_telegram_message("✅ Бот перезапущен. Продолжаю отслеживание новых товаров.")
    
    while True:
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
    return "eBay бот работает (интервал 120 сек, 20 попыток, усиленная очистка названий и поиск цен)"

@app.route('/health')
def health():
    return "OK", 200

if __name__ == "__main__":
    logging.info("Запуск бота...")
    send_telegram_message("🚀 Бот запущен: интервал 120 сек, 20 попыток, улучшен поиск цены и удаление 'Listing'")
    thread = threading.Thread(target=bot_worker, daemon=False)
    thread.start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
