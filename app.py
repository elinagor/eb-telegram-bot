import os
import threading
import time
import random
import sqlite3
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests
from flask import Flask
from dotenv import load_dotenv

# Загружаем переменные из .env
load_dotenv()

# ========== НАСТРОЙКИ ==========
EBAY_SEARCH_URL = os.getenv("EBAY_SEARCH_URL")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "600"))
# ===============================

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Создаём веб-приложение Flask для health check
app = Flask(__name__)

def send_telegram_message(message):
    """Отправляет сообщение в Telegram"""
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

def fetch_ebay_html():
    """Загружает страницу eBay с маскировкой под браузер"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': 'https://www.ebay.com/',
        'Sec-Ch-Ua': '"Google Chrome";v="124", "Chromium";v="124", "Not-A.Brand";v="99"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Upgrade-Insecure-Requests': '1',
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
    }
    
    time.sleep(random.uniform(2.0, 5.0))
    try:
        response = cffi_requests.get(
            EBAY_SEARCH_URL,
            headers=headers,
            impersonate="chrome124",
            timeout=45
        )
        response.raise_for_status()
        response.encoding = 'utf-8'
        logging.info("Страница eBay загружена")
        return response.text
    except Exception as e:
        logging.error(f"Ошибка загрузки eBay: {e}")
        return None

def extract_item_id(url):
    if not url or '/itm/' not in url:
        return None
    parts = url.split('/itm/')
    if len(parts) < 2:
        return None
    return parts[1].split('?')[0]

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
        title = None
        parent = link.find_parent('div', class_='s-item__info') or link.find_parent('div', class_='s-card')
        if parent:
            title_elem = parent.find('div', class_='s-item__title') or parent.find('h3', class_='s-item__title')
            if title_elem:
                title = title_elem.get_text(strip=True)
        if not title:
            title = link.get_text(strip=True)
        if not title or title.lower() in ('', 'new listing', 'новое объявление'):
            continue
        items[item_id] = {'url': url, 'title': title}
    logging.info(f"Извлечено товаров: {len(items)}")
    return items

def init_db_and_snapshot():
    """Создаёт БД и делает начальный снимок"""
    conn = sqlite3.connect('ebay_tracker.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS seen_items (
            item_id TEXT PRIMARY KEY,
            first_seen TIMESTAMP
        )
    ''')
    conn.commit()
    logging.info("Делаем начальный снимок...")
    html = fetch_ebay_html()
    if not html:
        logging.error("Не удалось загрузить eBay для снимка")
        conn.close()
        return
    items = parse_ebay_listings(html)
    count = 0
    for item_id in items:
        try:
            cursor.execute('INSERT INTO seen_items (item_id, first_seen) VALUES (?, ?)', 
                           (item_id, datetime.now()))
            count += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    conn.close()
    logging.info(f"Начальный снимок: добавлено {count} товаров")

def get_seen_ids():
    conn = sqlite3.connect('ebay_tracker.db')
    cursor = conn.cursor()
    cursor.execute('SELECT item_id FROM seen_items')
    seen = set(row[0] for row in cursor.fetchall())
    conn.close()
    return seen

def add_seen_id(item_id):
    conn = sqlite3.connect('ebay_tracker.db')
    cursor = conn.cursor()
    cursor.execute('INSERT OR IGNORE INTO seen_items (item_id, first_seen) VALUES (?, ?)', 
                   (item_id, datetime.now()))
    conn.commit()
    conn.close()

def check_new_items():
    """Проверяет новые товары"""
    seen = get_seen_ids()
    html = fetch_ebay_html()
    if not html:
        return []
    current = parse_ebay_listings(html)
    new_items = []
    for item_id, data in current.items():
        if item_id not in seen:
            new_items.append({'id': item_id, 'url': data['url'], 'title': data['title']})
            logging.info(f"НОВЫЙ ТОВАР: {item_id}")
    return new_items

def bot_worker():
    """Фоновый поток для мониторинга eBay"""
    # Ждём 30 секунд перед стартом, чтобы веб-сервер успел запуститься
    time.sleep(30)
    init_db_and_snapshot()
    send_telegram_message("🚀 Бот мониторинга eBay запущен на Render.com!")
    logging.info("Бот начал работу")
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
            sleep_time = CHECK_INTERVAL + random.uniform(-30, 60)
            sleep_time = max(300, sleep_time)
            logging.info(f"Следующая проверка через {sleep_time:.0f} сек")
            time.sleep(sleep_time)
        except Exception as e:
            logging.error(f"Ошибка в цикле: {e}")
            time.sleep(120)

@app.route('/')
def index():
    return "Бот мониторинга eBay работает!"

@app.route('/health')
def health_check():
    """Endpoint для health check (чтобы Render не усыплял бота)"""
    return "OK", 200

# Запуск Flask в основном потоке, бота — в фоновом
if __name__ == "__main__":
    # Запускаем бота в отдельном потоке
    thread = threading.Thread(target=bot_worker)
    thread.start()
    # Запускаем веб-сервер
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)