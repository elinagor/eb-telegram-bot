import os
import sys
import ssl
import time
import random
import re
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

# ============ ФУНКЦИИ ДЛЯ EBAY (С ПОВТОРНЫМИ ПОПЫТКАМИ) ============
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
                timeout=30  # уменьшено с 45 до 30 секунд
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
    Удаляет фразы 'New Listing', 'New', 'Listing' из начала/середины названия.
    Также удаляет лишние пробелы и знаки препинания вокруг.
    """
    if not title:
        return ""
    # Удаляем "new listing" (с пробелом и без) в любом регистре
    # А также варианты с точкой, запятой, скобками
    patterns = [
        r'(?i)\bnew listing\b',          # New listing
        r'(?i)\bnewlisting\b',           # Newlisting
        r'(?i)\bnew\s+listing\b',        # New   listing
        r'(?i)^listing\s*',              # Listing в начале
        r'(?i)^new\s+',                  # New в начале (с пробелом)
        r'(?i)\bnew\b\s*',               # New как отдельное слово
    ]
    cleaned = title
    for pat in patterns:
        cleaned = re.sub(pat, '', cleaned)
    # Убираем лишние символы пунктуации и пробелы
    cleaned = re.sub(r'[^\w\s\$€£¥]', ' ', cleaned)  # заменяем знаки на пробелы
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned

def extract_price_from_element(item):
    """
    Пытается извлечь цену из элемента карточки товара.
    Возвращает строку с ценой или None.
    """
    # Список селекторов для поиска цены (от наиболее специфичных к общим)
    price_selectors = [
        'span.s-item__price',                           # стандартный класс
        '.s-item__detail .s-item__price',               # альтернативная структура
        '.s-item__price[aria-label]',                   # ценник с aria-label
        'span.POSITIVE',                                # положительная цена (акции)
        'span.NEGATIVE',                                # отрицательная (со скидкой)
        '.s-item__detail .vi-price',                    # старый вариант
        '.s-item__price span',                          # вложенный span
        'div.s-item__price',                            # div вместо span
        '.s-item__price--strikethrough',                # зачёркнутая (можно пропустить)
        '.s-item__discount-price',                      # цена со скидкой
        '[class*="price"]',                             # любой класс содержащий price
        '.bold',                                        # иногда цена выделена жирным
    ]
    
    for selector in price_selectors:
        price_elem = item.select_one(selector)
        if price_elem:
            price_text = price_elem.get_text(strip=True)
            if price_text:
                # Убираем лишние символы, оставляем только цену
                # Пример: "$12.99", "EUR 10.50", "C $15.00"
                # Удаляем ненужные надписи типа "to" или "–"
                price_text = re.sub(r'(?i)\s*(to|To|–|-)\s*', ' ', price_text)
                # Если в тексте несколько цен (диапазон), берём первую
                first_price = re.split(r'\s+', price_text)[0]
                # Проверяем, что первый фрагмент похож на цену (цифры или валюта)
                if re.search(r'[\d.,]', first_price):
                    return first_price
    return None

def parse_ebay_listings(html, max_items=MAX_ITEMS):
    """
    Парсит страницу поиска eBay с использованием нескольких стратегий.
    Возвращает словарь {item_id: {'url': url, 'title': clean_title, 'price': price}}
    """
    if not html:
        return {}
    soup = BeautifulSoup(html, 'html.parser')
    items_data = {}

    # СТРАТЕГИЯ 1: Ищем стандартные карточки li.s-item
    items_cards = soup.select('li.s-item')
    if not items_cards:
        # Альтернативные контейнеры (div вместо li)
        items_cards = soup.select('.s-item')
    if not items_cards:
        # Если вообще нет карточек, используем универсальный метод по ссылкам /itm/
        logging.warning("Не найдены стандартные карточки товаров, используем универсальный поиск по ссылкам (без цены)")
        links = soup.find_all('a', href=True)
        itm_links = [link for link in links if '/itm/' in link['href']]
        itm_links = itm_links[:max_items]
        processed = 0
        for link in itm_links:
            if processed >= max_items:
                break
            url = link['href']
            if url.startswith('/'):
                url = 'https://www.ebay.com' + url
            item_id = extract_item_id(url)
            if not item_id:
                continue
            raw_title = link.get_text(strip=True)
            title = clean_title(raw_title)
            if not title:
                continue
            items_data[item_id] = {
                'url': url,
                'title': title,
                'price': None
            }
            processed += 1
        logging.info(f"Универсальный метод: найдено {len(items_data)} товаров (без цены)")
        return items_data

    # Если карточки найдены, парсим их с ценой
    processed = 0
    for card in items_cards:
        if processed >= max_items:
            break
        
        # Ссылка на товар
        link_elem = card.select_one('a.s-item__link')
        if not link_elem:
            continue
        url = link_elem.get('href')
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
                      link_elem)
        raw_title = title_elem.get_text(strip=True) if title_elem else ''
        title = clean_title(raw_title)
        if not title:
            continue
        
        # Цена
        price = extract_price_from_element(card)
        if not price:
            # Иногда цена может быть внутри ссылки
            price = extract_price_from_element(link_elem)
        
        items_data[item_id] = {
            'url': url,
            'title': title,
            'price': price
        }
        processed += 1
    
    logging.info(f"Основной парсинг: обработано {len(items_data)} товаров (с ценами где возможно)")
    # Для отладки: вывести в лог образец цен
    sample_count = min(3, len(items_data))
    if sample_count:
        sample_items = list(items_data.items())[:sample_count]
        for item_id, data in sample_items:
            logging.info(f"Пример: {data['title'][:40]}... Цена: {data['price']}")
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
            logging.info(f"НОВЫЙ товар: {item_id} -> {data['title'][:60]}... (цена: {data['price']})")
    
    if new_items:
        logging.info(f"Найдено новых товаров: {len(new_items)}. Отправляем в Telegram...")
        for item in new_items:
            # Формируем чистое сообщение без "New listing" и с ценой
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
    return "eBay бот работает (интервал 120 сек, 20 попыток при ошибках, есть цена и чистые названия)"

@app.route('/health')
def health():
    return "OK", 200

if __name__ == "__main__":
    logging.info("Запуск бота...")
    send_telegram_message("🚀 Бот запущен: интервал 120 сек, 20 попыток при ошибках, улучшен парсинг цен.")
    thread = threading.Thread(target=bot_worker, daemon=False)
    thread.start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
