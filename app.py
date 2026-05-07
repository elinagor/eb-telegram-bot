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

# Отключение проверки SSL для Bright Data
ssl._create_default_https_context = ssl._create_unverified_context

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    logging.error("curl_cffi не установлен")
    sys.exit(1)

load_dotenv()

# === НАСТРОЙКИ ===
EBAY_SEARCH_URL = os.getenv("EBAY_SEARCH_URL")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "600"))
BRIGHT_DATA_PROXY_URL = os.getenv("BRIGHT_DATA_PROXY_URL")
DATABASE_URL = os.getenv("DATABASE_URL")

if not all([EBAY_SEARCH_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, BRIGHT_DATA_PROXY_URL, DATABASE_URL]):
    logging.error("Не хватает переменных окружения")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
app = Flask(__name__)

# === РАБОТА С БАЗОЙ ДАННЫХ ===
def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def init_db():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS seen_items (
                    item_id TEXT PRIMARY KEY,
                    first_seen TEXT
                )
            """)
        conn.commit()
    logging.info("База данных PostgreSQL инициализирована")

def get_seen_ids():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT item_id FROM seen_items")
            rows = cur.fetchall()
            return {row[0] for row in rows}

def add_seen_id(item_id):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO seen_items (item_id, first_seen) VALUES (%s, %s) ON CONFLICT (item_id) DO NOTHING",
                (item_id, datetime.now().isoformat())
            )
        conn.commit()

def add_seen_ids_batch(item_ids):
    if not item_ids:
        return
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            data = [(item_id, datetime.now().isoformat()) for item_id in item_ids]
            execute_values(cur, "INSERT INTO seen_items (item_id, first_seen) VALUES %s ON CONFLICT (item_id) DO NOTHING", data)
        conn.commit()

def init_db_and_snapshot():
    init_db()
    logging.info("Делаем начальный снимок (запоминаем текущие товары, чтобы не отправлять их)...")
    html = fetch_ebay_html()
    if not html:
        logging.error("Не удалось загрузить eBay для снимка")
        return
    items = parse_ebay_listings(html)
    item_ids = list(items.keys())
    if item_ids:
        add_seen_ids_batch(item_ids)
        logging.info(f"Снимок выполнен: добавлено {len(item_ids)} товаров в БД (они не будут отправлены)")
    else:
        logging.warning("Товары не найдены на странице")

# === ОСТАЛЬНЫЕ ФУНКЦИИ (парсинг, запросы, Telegram) ===
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
        logging.error(f"Ошибка отправки в Telegram: {e}")

def fetch_ebay_html():
    proxies = {"http": BRIGHT_DATA_PROXY_URL, "https": BRIGHT_DATA_PROXY_URL}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Referer': 'https://www.ebay.com/',
        'Sec-Ch-Ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="99"',
        'Upgrade-Insecure-Requests': '1',
    }
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
            logging.error(f"Ошибка HTTP {response.status_code}")
            return None
    except Exception as e:
        logging.error(f"Ошибка при запросе: {e}")
        return None

def extract_item_id(url):
    if not url or '/itm/' not in url:
        return None
    try:
        return url.split('/itm/')[1].split('?')[0]
    except IndexError:
        return None

def parse_ebay_listings(html):
    if not html:
        return {}
    soup = BeautifulSoup(html, 'html.parser')
    items = {}
    links = soup.find_all('a', href=True)
    itm_links = [link for link in links if '/itm/' in link['href']]
    logging.info(f"Найдено ссылок с '/itm/': {len(itm_links)}")
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
    logging.info(f"Извлечено уникальных товаров: {len(items)}")
    return items

def check_new_items():
    seen = get_seen_ids()
    html = fetch_ebay_html()
    if not html:
        return []
    current = parse_ebay_listings(html)
    new_items = []
    for item_id, data in current.items():
        if item_id not in seen:
            new_items.append({'id': item_id, 'url': data['url'], 'title': data['title']})
            logging.info(f"НОВЫЙ ТОВАР: {item_id} -> {data['title']}")
    return new_items

def bot_worker():
    logging.info("Фоновый поток запущен")
    time.sleep(10)
    try:
        init_db_and_snapshot()
        send_telegram_message("🚀 Бот запущен на Render с PostgreSQL и Bright Data (только новые товары)")
        while True:
            try:
                new_items = check_new_items()
                if new_items:
                    logging.info(f"Найдено новых товаров: {len(new_items)}")
                    for item in new_items:
                        msg = (f"🔹 <b>НОВЫЙ ТОВАР НА EBAY</b> 🔹\n\n"
                               f"📦 <b>{item['title']}</b>\n\n"
                               f"🔗 <a href='{item['url']}'>Ссылка на товар</a>")
                        send_telegram_message(msg)
                        add_seen_id(item['id'])
                        time.sleep(1)
                else:
                    logging.info("Новых товаров нет")
                wait = max(300, CHECK_INTERVAL + random.uniform(-60, 120))
                logging.info(f"Следующая проверка через {wait:.0f} сек")
                time.sleep(wait)
            except Exception as e:
                logging.error(f"Ошибка в основном цикле: {e}", exc_info=True)
                time.sleep(120)
    except Exception as e:
        logging.error(f"Критическая ошибка: {e}", exc_info=True)
        send_telegram_message("❌ Критическая ошибка, бот остановлен")
        sys.exit(1)

@app.route('/')
def index():
    return "eBay бот работает с PostgreSQL"

@app.route('/health')
def health():
    return "OK", 200

if __name__ == "__main__":
    logging.info("Запуск бота...")
    send_telegram_message("✅ Бот стартует с PostgreSQL и Bright Data.")
    thread = threading.Thread(target=bot_worker, daemon=False)
    thread.start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
