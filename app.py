import os
import sys
import ssl
import time
import random
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
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "600"))
BRIGHT_DATA_PROXY_URL = os.getenv("BRIGHT_DATA_PROXY_URL")
DATABASE_URL = os.getenv("DATABASE_URL")
MAX_ITEMS = 20  # Ограничение: обрабатывать только первые N товаров (чем меньше, тем свежее)

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

def fetch_ebay_html_with_retry(retries=3, delay=10):
    proxies = {"http": BRIGHT_DATA_PROXY_URL, "https": BRIGHT_DATA_PROXY_URL}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Referer': 'https://www.ebay.com/',
        'Sec-Ch-Ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="99"',
        'Upgrade-Insecure-Requests': '1',
    }
    for attempt in range(1, retries+1):
        try:
            response = cffi_requests.get(
                EBAY_SEARCH_URL,
                headers=headers,
                impersonate="chrome131",
                proxies=proxies,
                verify=False,
                timeout=45
            )
            if response.status_code == 200:
                logging.info("Страница eBay успешно загружена")
                return response.text
            else:
                logging.warning(f"Попытка {attempt}: HTTP {response.status_code}")
                if attempt < retries:
                    time.sleep(delay)
                continue
        except Exception as e:
            logging.error(f"Попытка {attempt} ошибка: {e}")
            if attempt < retries:
                time.sleep(delay)
            continue
    logging.error("Не удалось загрузить страницу после всех попыток")
    return None

def extract_item_id(url):
    if not url or '/itm/' not in url:
        return None
    try:
        return url.split('/itm/')[1].split('?')[0]
    except IndexError:
        return None

def parse_ebay_listings(html, max_items=MAX_ITEMS):
    if not html:
        return {}
    soup = BeautifulSoup(html, 'html.parser')
    items = {}
    # Находим все ссылки с '/itm/'
    links = soup.find_all('a', href=True)
    itm_links = [link for link in links if '/itm/' in link['href']]
    logging.info(f"Найдено всего ссылок с '/itm/': {len(itm_links)}")
    # Ограничиваем количество первыми max_items
    itm_links = itm_links[:max_items]
    for link in itm_links:
        url = link['href']
        if url.startswith('/'):
            url = 'https://www.ebay.com' + url
        item_id = extract_item_id(url)
        if not item_id:
            continue
        title = link.get_text(strip=True)
        if not title or title.lower() in ('', 'new listing', 'новое объявление'):
            continue
        items[item_id] = {'url': url, 'title': title}
    logging.info(f"Обработано товаров (первые {max_items}): {len(items)}")
    return items

def perform_initial_snapshot():
    logging.info("Делаем начальный снимок (сохраняем ID из текущей выдачи)...")
    html = fetch_ebay_html_with_retry()
    if not html:
        logging.error("Не удалось загрузить eBay для начального снимка.")
        return False
    items = parse_ebay_listings(html, max_items=50)  # для снимка можно больше
    if not items:
        logging.warning("На странице не найдено товаров. Проверьте ссылку.")
        return False
    item_ids = list(items.keys())
    add_seen_ids_batch(item_ids)
    logging.info(f"Начальный снимок: добавлено {len(item_ids)} товаров. Они НЕ будут отправлены.")
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
            new_items.append({'id': item_id, 'url': data['url'], 'title': data['title']})
            logging.info(f"НОВЫЙ товар: {item_id} -> {data['title'][:60]}...")
    if new_items:
        logging.info(f"Найдено новых товаров: {len(new_items)}. Отправляем в Telegram...")
        for item in new_items:
            msg = (f"🔹 <b>НОВЫЙ ТОВАР НА EBAY</b> 🔹\n\n"
                   f"📦 <b>{item['title']}</b>\n\n"
                   f"🔗 <a href='{item['url']}'>Ссылка на товар</a>")
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
        send_telegram_message("✅ Бот перезапущен. Продолжаю отслеживание новых товаров (старые не будут дублироваться).")

    while True:
        try:
            check_and_send_new_items()
            wait = max(300, CHECK_INTERVAL + random.uniform(-60, 120))
            logging.info(f"Следующая проверка через {wait:.0f} секунд.")
            time.sleep(wait)
        except Exception as e:
            logging.error(f"Ошибка в основном цикле: {e}", exc_info=True)
            time.sleep(120)

@app.route('/')
def index():
    return "eBay бот работает с PostgreSQL (только свежие товары, устойчив к 502)"

@app.route('/health')
def health():
    return "OK", 200

if __name__ == "__main__":
    logging.info("Запуск бота...")
    send_telegram_message("🚀 Бот запускается с PostgreSQL и Bright Data (улучшенная устойчивость, ограничение на количество товаров).")
    thread = threading.Thread(target=bot_worker, daemon=False)
    thread.start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
