import os
import sys
import time
import random
import re
import threading
import logging
import requests
from datetime import datetime
from flask import Flask
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()

# ============ НАСТРОЙКИ ============
EBAY_SEARCH_URL = os.getenv("EBAY_SEARCH_URL")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DATABASE_URL = os.getenv("DATABASE_URL")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "60"))
MAX_ITEMS = 20
MAX_RETRIES = 5       # меньше попыток для отладки
RETRY_DELAY = 5

if not all([EBAY_SEARCH_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DATABASE_URL]):
    logging.basicConfig(level=logging.INFO)
    logging.error("Не хватает переменных окружения. Проверьте .env")
    sys.exit(1)

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ============ РАБОТА С БАЗОЙ ДАННЫХ ============
def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def init_db():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS seen_items (item_id TEXT PRIMARY KEY, first_seen TIMESTAMP)")
        conn.commit()
    logging.info("Таблица seen_items готова")

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

def reset_database():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM seen_items")
        conn.commit()
    logging.info("База данных очищена")

# ============ TELEGRAM ============
def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': 'HTML', 'disable_web_page_preview': False}
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            logging.error(f"Ошибка Telegram: {r.text}")
    except Exception as e:
        logging.error(f"Не удалось отправить в Telegram: {e}")

# ============ EBAY ЗАПРОС (БЕЗ CURL_CFFI) ============
def fetch_ebay_html():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
        'Accept-Language': 'en-GB,en;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Referer': 'https://www.ebay.co.uk/',
    }
    for attempt in range(1, MAX_RETRIES+1):
        try:
            logging.info(f"Попытка {attempt} загрузить {EBAY_SEARCH_URL}")
            r = requests.get(EBAY_SEARCH_URL, headers=headers, timeout=30)
            if r.status_code == 200:
                logging.info("Страница загружена")
                return r.text
            else:
                logging.warning(f"HTTP {r.status_code}, попытка {attempt}")
                time.sleep(RETRY_DELAY)
        except Exception as e:
            logging.error(f"Ошибка: {e}, попытка {attempt}")
            time.sleep(RETRY_DELAY)
    return None

# ============ ПАРСИНГ (ПРОСТОЙ) ============
def parse_simple(html):
    """Ищет все /itm/ ссылки и извлекает данные регулярками и ближайшими элементами"""
    items = {}
    # Ищем все ссылки с /itm/
    link_pattern = r'<a\s+[^>]*href="([^"]*)"[^>]*>'
    all_links = re.findall(link_pattern, html)
    itm_links = [url for url in all_links if '/itm/' in url]
    logging.info(f"Найдено ссылок /itm/: {len(itm_links)}")
    for url in itm_links[:MAX_ITEMS]:
        if url.startswith('/'):
            url = 'https://www.ebay.co.uk' + url
        # item_id из URL
        match = re.search(r'/itm/(\d+)', url)
        if not match:
            continue
        item_id = match.group(1)
        # Ищем контейнер: нужно найти блок, содержащий и ссылку, и цену
        # Упростим: найдём фрагмент HTML вокруг ссылки
        # Но проще взять название из атрибута title ссылки или из текста
        # Поищем span.s-card__title или span.su-styled-text
        # Так как у нас нет структуры, попробуем найти ближайший span с классом, содержащим price/title
        # Используем комбинацию: ищем span.su-styled-text.primary.default
        # Вместо сложного парсинга – возьмём данные из JSON-LD, если есть
        # Но для начала – просто текст ссылки
        title_match = re.search(r'<span[^>]*class="[^"]*s-card__title[^"]*"[^>]*>([^<]+)</span>', html)
        if title_match:
            title = title_match.group(1).strip()
        else:
            # fallback: ищем текст после ссылки
            title = "Товар"
        # Цена
        price_match = re.search(r'<span[^>]*class="[^"]*s-card__price[^"]*"[^>]*>([^<]+)</span>', html)
        price = price_match.group(1).strip() if price_match else None
        # Доставка
        ship_match = re.search(r'<span[^>]*class="[^"]*su-styled-text secondary large[^"]*"[^>]*>\+?(£[\d,\.]+)\s*delivery</span>', html, re.I)
        shipping = ship_match.group(1) if ship_match else None
        # Best Offer
        best_offer = bool(re.search(r'or Best Offer', html, re.I))
        # Auction
        auction = bool(re.search(r'\d+\s+bids?\b', html, re.I))
        
        items[item_id] = {
            'url': url,
            'title': title,
            'price': price,
            'shipping': shipping,
            'best_offer': best_offer,
            'auction': auction
        }
    return items

# ============ ОСНОВНЫЕ ФУНКЦИИ ============
def perform_initial_snapshot():
    logging.info("Начальный снимок...")
    html = fetch_ebay_html()
    if not html:
        return False
    items = parse_simple(html)
    if not items:
        logging.warning("Товары не найдены. Проверьте URL или ответ eBay.")
        return False
    add_seen_ids_batch(list(items.keys()))
    logging.info(f"Снимок: сохранено {len(items)} товаров")
    return True

def check_and_send():
    seen = get_seen_ids()
    logging.info(f"В БД {len(seen)} товаров")
    html = fetch_ebay_html()
    if not html:
        return
    current = parse_simple(html)
    new = []
    for item_id, data in current.items():
        if item_id not in seen:
            new.append({'id': item_id, **data})
            logging.info(f"Новый: {data['title']}")
    if new:
        for item in new:
            msg = f"🇬🇧 <b>НОВЫЙ ТОВАР Англия</b> 🇬🇧\n\n<b>{item['title']}</b>\n\n"
            if item['price']:
                msg += f"💰 Цена: {item['price']}\n"
            else:
                msg += f"💰 Цена не указана\n"
            if item['shipping']:
                msg += f"🚚 Доставка: {item['shipping']}\n"
            if item['best_offer']:
                msg += f"✅ Сделать предложение (Best Offer)\n"
            if item['auction']:
                msg += f"⏰ Аукцион\n"
            msg += f"\n🔗 <a href='{item['url']}'>Ссылка на товар</a>"
            send_telegram_message(msg)
            add_seen_ids_batch([item['id']])
            time.sleep(1)
    else:
        logging.info("Новых нет")

# ============ ПОТОК ВОРКЕРА ============
def bot_worker():
    logging.info("Бот-воркер запущен")
    init_db()
    if is_db_empty():
        if not perform_initial_snapshot():
            send_telegram_message("❌ Ошибка инициализации: не удалось получить товары с eBay")
            return
        send_telegram_message("✅ Бот запущен, начальный снимок сделан")
    else:
        send_telegram_message("✅ Бот перезапущен")
    while True:
        try:
            check_and_send()
            wait = max(60, CHECK_INTERVAL + random.uniform(-30, 60))
            logging.info(f"Следующая проверка через {int(wait)} сек")
            time.sleep(wait)
        except Exception as e:
            logging.error(f"Ошибка в цикле: {e}", exc_info=True)
            time.sleep(120)

# ============ СЛУШАТЕЛЬ КОМАНД TELEGRAM ============
def telegram_listener():
    global is_paused
    logging.info("Телеграм-слушатель запущен")
    last_update_id = 0
    is_paused = False
    while True:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
            params = {'offset': last_update_id + 1, 'timeout': 30}
            r = requests.get(url, params=params, timeout=35)
            if r.status_code == 200:
                updates = r.json().get('result', [])
                for update in updates:
                    last_update_id = update['update_id']
                    msg = update.get('message')
                    if msg and str(msg.get('chat', {}).get('id')) == TELEGRAM_CHAT_ID:
                        text = msg.get('text', '').strip()
                        if text == '/stop':
                            is_paused = True
                            send_telegram_message("⏸ Бот приостановлен. /start - возобновить")
                        elif text == '/start':
                            is_paused = False
                            send_telegram_message("▶ Бот работает")
                        elif text == '/reset':
                            reset_database()
                            send_telegram_message("🔄 База очищена. При следующей проверке бот сделает новый снимок.")
            time.sleep(1)
        except Exception as e:
            logging.error(f"Ошибка слушателя: {e}")
            time.sleep(5)

# ============ FLASK ============
app = Flask(__name__)

@app.route('/')
def index():
    return "eBay bot running"

@app.route('/health')
def health():
    return "OK", 200

# ============ ЗАПУСК ============
if __name__ == "__main__":
    logging.info("Запуск бота...")
    send_telegram_message("🚀 Бот запущен (упрощённая версия). Команды: /stop /start /reset")
    # Запуск слушателя в демон-потоке
    threading.Thread(target=telegram_listener, daemon=True).start()
    # Запуск воркера в основном потоке (или в отдельном, но не демон)
    worker = threading.Thread(target=bot_worker, daemon=False)
    worker.start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
