import os
import sys
import time
import random
import sqlite3
import threading
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from flask import Flask
from dotenv import load_dotenv

# Импорт curl_cffi
try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    logging.error("curl_cffi не установлен. Добавьте его в requirements.txt")
    sys.exit(1)

load_dotenv()

# ========== НАСТРОЙКИ ==========
EBAY_SEARCH_URL = os.getenv("EBAY_SEARCH_URL")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "600"))

# --- Новая переменная для прокси Bright Data ---
BRIGHT_DATA_PROXY_URL = os.getenv("BRIGHT_DATA_PROXY_URL")
# =============================================

if not EBAY_SEARCH_URL or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    logging.error("Не хватает основных переменных окружения (EBAY_SEARCH_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID).")
    sys.exit(1)

if not BRIGHT_DATA_PROXY_URL:
    logging.error("Не указана переменная BRIGHT_DATA_PROXY_URL. Добавьте её в Environment Variables на Render.")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

# ==================== ФУНКЦИЯ ДЛЯ ЗАПРОСОВ ЧЕРЕЗ BRIGHT DATA ====================
def fetch_ebay_html():
    """Выполняет запрос к eBay через прокси Bright Data."""
    # Настраиваем прокси для curl_cffi
    proxies = {
        "http": BRIGHT_DATA_PROXY_URL,
        "https": BRIGHT_DATA_PROXY_URL,
    }

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Referer': 'https://www.ebay.com/',
        'Sec-Ch-Ua': '"Google Chrome";v="124", "Chromium";v="124", "Not-A.Brand";v="99"',
        'Upgrade-Insecure-Requests': '1',
    }

    try:
        # Делаем запрос, используя proxies и impersonate для маскировки
        response = cffi_requests.get(
            EBAY_SEARCH_URL,
            headers=headers,
            impersonate="chrome124",
            proxies=proxies,
            timeout=45
        )
        if response.status_code == 200:
            logging.info("Страница eBay успешно загружена через Bright Data.")
            return response.text
        else:
            logging.error(f"Ошибка HTTP {response.status_code} при запросе через Bright Data.")
            return None
    except Exception as e:
        logging.error(f"Ошибка при запросе через Bright Data: {e}")
        return None

# --- ВСЕ ОСТАЛЬНЫЕ ФУНКЦИИ (парсинг, работа с БД, отправка сообщений) ОСТАЮТСЯ БЕЗ ИЗМЕНЕНИЙ ---
# Они такие же, как в предыдущей стабильной версии вашего бота.

def send_telegram_message(message):
    # ... (без изменений) ...
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

def extract_item_id(url):
    # ... (без изменений) ...
    if not url or '/itm/' not in url:
        return None
    try:
        return url.split('/itm/')[1].split('?')[0]
    except IndexError:
        return None

def parse_ebay_listings(html):
    # ... (без изменений) ...
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
    # ... (без изменений) ...
    conn = sqlite3.connect('ebay_tracker.db')
    cursor = conn.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS seen_items (item_id TEXT PRIMARY KEY, first_seen TIMESTAMP)')
    conn.commit()
    logging.info("Делаем начальный снимок...")
    html = fetch_ebay_html()
    if not html:
        logging.error("Не удалось загрузить eBay для снимка. Проверьте работу прокси.")
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
    logging.info(f"Начальный снимок: добавлено {count} товаров (не будут отправлены)")

def get_seen_ids():
    # ... (без изменений) ...
    conn = sqlite3.connect('ebay_tracker.db')
    cursor = conn.cursor()
    cursor.execute('SELECT item_id FROM seen_items')
    seen = set(row[0] for row in cursor.fetchall())
    conn.close()
    return seen

def add_seen_id(item_id):
    # ... (без изменений) ...
    conn = sqlite3.connect('ebay_tracker.db')
    cursor = conn.cursor()
    cursor.execute('INSERT OR IGNORE INTO seen_items (item_id, first_seen) VALUES (?, ?)',
                   (item_id, datetime.now()))
    conn.commit()
    conn.close()

def check_new_items():
    # ... (без изменений) ...
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
    # ... (без изменений) ...
    logging.info("🔄 Фоновый поток запущен")
    time.sleep(10)
    try:
        init_db_and_snapshot()
        send_telegram_message(f"🚀 Бот запущен на Render.com, используя Bright Data Residential Proxy!")
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
                logging.error(f"Ошибка в цикле: {e}", exc_info=True)
                time.sleep(120)
    except Exception as e:
        logging.error(f"Критическая ошибка: {e}", exc_info=True)
        send_telegram_message("❌ Критическая ошибка, бот остановлен. Проверьте логи на Render.")
        sys.exit(1)

@app.route('/')
def index():
    return "eBay бот работает через Bright Data Proxy."

@app.route('/health')
def health():
    return "OK", 200

if __name__ == "__main__":
    logging.info("Запуск процесса...")
    send_telegram_message("✅ Бот запускается и настраивает соединение через Bright Data.")
    thread = threading.Thread(target=bot_worker, daemon=False)
    thread.start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
