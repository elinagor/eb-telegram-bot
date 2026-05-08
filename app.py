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
    """Выполняет запрос с повторными попытками (до MAX_RETRIES) при любых ошибках."""
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
    Агрессивно удаляет фразы 'New Listing', 'Listing', 'New' из названия товара.
    Обрабатывает как раздельные слова, так и слитные варианты ('NewListing', 'ListingDimensions').
    """
    if not title:
        return ""
    # Удаляем 'new listing' с любыми разделителями (пробел, запятая, точка, скобки)
    title = re.sub(r'(?i)\bnew\s*listing\b', '', title)
    # Удаляем 'newlisting' слитно
    title = re.sub(r'(?i)\bnewlisting\b', '', title)
    # Удаляем 'listing' в начале строки (и слитно со следующим словом)
    title = re.sub(r'(?i)^listing', '', title)
    # Удаляем 'listing' как отдельное слово с пробелами или знаками
    title = re.sub(r'(?i)\blisting\b', '', title)
    # Удаляем 'new' в начале строки
    title = re.sub(r'(?i)^new\s+', '', title)
    # Удаляем 'new' как отдельное слово
    title = re.sub(r'(?i)\bnew\b', '', title)
    # Убираем лишние пробелы и знаки препинания
    title = re.sub(r'\s+', ' ', title).strip()
    # Удаляем возможные остаточные символы вроде '|', '-', ':' по краям
    title = re.sub(r'^[\s\|\.\,\:\;\-\_]+|[\s\|\.\,\:\;\-\_]+$', '', title)
    return title

def extract_price_from_element(card):
    """
    Многоуровневый поиск цены внутри карточки товара.
    Пробует: CSS-селекторы, атрибуты, JSON-LD, регулярные выражения.
    """
    # Уровень 1: стандартные селекторы eBay
    price_selectors = [
        'span.s-item__price',
        '.s-item__detail .s-item__price',
        'span.s-item__price--buynow',
        '.s-item__price span',
        '[class*="price"]',
        '[data-testid="item-price"]',
        'span.POSITIVE',
        'span.NEGATIVE',
        '.vi-price',
        '.bold',
    ]
    for selector in price_selectors:
        elem = card.select_one(selector)
        if elem:
            text = elem.get_text(strip=True)
            if text and any(c.isdigit() or c in '$€£¥' for c in text):
                return clean_price_text(text)
    
    # Уровень 2: поиск по атрибутам aria-label
    for elem in card.select('[aria-label]'):
        label = elem.get('aria-label', '')
        price_match = re.search(r'(?:price|Price):?\s*([\d.,]+)', label, re.I)
        if price_match:
            return price_match.group(1)
    
    # Уровень 3: поиск в JSON-LD внутри карточки (если есть)
    script = card.find('script', type='application/ld+json')
    if script:
        try:
            data = json.loads(script.string)
            if isinstance(data, dict):
                if 'offers' in data and isinstance(data['offers'], dict):
                    price = data['offers'].get('price')
                    if price:
                        currency = data['offers'].get('priceCurrency', 'USD')
                        return f"{currency} {price}"
                elif 'price' in data:
                    return data['price']
        except:
            pass
    
    # Уровень 4: поиск по регулярному выражению в HTML карточки (как запасной вариант)
    card_html = str(card)
    # Ищем шаблоны: $12.99, USD 12.99, 12.99 USD, €10.50
    patterns = [
        r'(\$[\d,]+\.?\d*)',                     # $12.99
        r'([A-Z]{3}\s*[\d,]+\.?\d*)',            # USD 12.99
        r'([\d,]+\.?\d*\s*[A-Z]{3})',            # 12.99 USD
        r'([€£¥]\s*[\d,]+\.?\d*)',               # €10.50
        r'([\d,]+\.?\d*\s*[€£¥])',               # 10.50 €
    ]
    for pattern in patterns:
        match = re.search(pattern, card_html)
        if match:
            return match.group(1)
    
    return None

def clean_price_text(price_text):
    """Убирает лишние слова и оставляет только цену с валютой."""
    # Удаляем слова "to", "–", "-" и пр.
    price_text = re.sub(r'(?i)\s*(to|–|-|—)\s*', ' ', price_text)
    # Берём первую часть, если их несколько
    parts = price_text.split()
    for part in parts:
        if any(c.isdigit() or c in '$€£¥' for c in part):
            return part
    return price_text

def parse_ebay_listings(html, max_items=MAX_ITEMS):
    """
    Парсит страницу поиска eBay с несколькими стратегиями.
    Возвращает словарь {item_id: {'url': url, 'title': clean_title, 'price': price}}
    """
    if not html:
        return {}
    soup = BeautifulSoup(html, 'html.parser')
    items_data = {}

    # Стратегия 1: стандартные карточки li.s-item
    cards = soup.select('li.s-item')
    if not cards:
        cards = soup.select('.s-item')  # альтернативный класс
    
    if cards:
        processed = 0
        for card in cards:
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
                # Пробуем взять title из link, если он есть
                title = clean_title(link.get_text(strip=True))
                if not title:
                    continue
            
            # Цена
            price = extract_price_from_element(card)
            
            items_data[item_id] = {
                'url': url,
                'title': title,
                'price': price
            }
            processed += 1
        
        if items_data:
            logging.info(f"Стандартный парсинг: найдено {len(items_data)} товаров")
            return items_data
    
    # Стратегия 2: если карточки не найдены, ищем все ссылки /itm/ и пытаемся вытащить цену из окружающего HTML
    logging.warning("Стандартные карточки не найдены, использую резервный метод (поиск по ссылкам)")
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
        
        # Пытаемся найти цену в родительских элементах ссылки
        price = None
        parent = link.parent
        for _ in range(3):  # поднимаемся на 3 уровня вверх
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
    send_telegram_message("🚀 Бот запущен: интервал 120 сек, 20 попыток, улучшен поиск цены и удаление 'New Listing'")
    thread = threading.Thread(target=bot_worker, daemon=False)
    thread.start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
