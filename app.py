import os
import sys
import ssl
import time
import random
import re
import threading
import logging
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from flask import Flask
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

# Отключаем проверку SSL
ssl._create_default_https_context = ssl._create_unverified_context

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    print("curl_cffi не установлен. Установите: pip install curl_cffi")
    sys.exit(1)

load_dotenv()

# ============ НАСТРОЙКИ ============
EBAY_SEARCH_URL = os.getenv("EBAY_SEARCH_URL")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DATABASE_URL = os.getenv("DATABASE_URL")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "60"))
MAX_ITEMS = 20
MAX_RETRIES = 20
RETRY_DELAY = 5

# Проверяем только необходимые переменные
if not all([EBAY_SEARCH_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DATABASE_URL]):
    logging.error("Не хватает переменных окружения. Нужны: EBAY_SEARCH_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DATABASE_URL")
    sys.exit(1)

# Принудительная локализация Великобритании и сортировка по новизне
if '?' in EBAY_SEARCH_URL:
    EBAY_SEARCH_URL += '&LH_PrefLoc=3&_ipg=240&_sop=10'
else:
    EBAY_SEARCH_URL += '?LH_PrefLoc=3&_ipg=240&_sop=10'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
app = Flask(__name__)

is_paused = False

# ============ USER-AGENTЫ ============
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
]

# ============ БАЗА ДАННЫХ ============
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
def send_telegram_message(message, parse_mode='HTML'):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': parse_mode, 'disable_web_page_preview': False}
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            logging.error(f"Ошибка Telegram: {r.text}")
    except Exception as e:
        logging.error(f"Не удалось отправить в Telegram: {e}")

# ============ ЗАПРОС К EBAY (С ОТЛАДКОЙ) ============
def fetch_ebay_html_with_retry():
    cookies = {'ebay': '%2F', 'm': 'GB', 's': 'UK', 'siteid': '3'}
    for attempt in range(1, MAX_RETRIES + 1):
        current_ua = random.choice(USER_AGENTS)
        headers = {
            'User-Agent': current_ua,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-GB,en;q=0.9',
            'Referer': 'https://www.ebay.co.uk/',
            'X-EBay-Site-Id': '3',
            'Sec-Ch-Ua': '"Google Chrome";v="142", "Chromium";v="142", "Not_A Brand";v="99"',
            'Upgrade-Insecure-Requests': '1',
        }
        if attempt > 1:
            time.sleep(random.uniform(1.5, 3.5))
        try:
            response = cffi_requests.get(
                EBAY_SEARCH_URL,
                headers=headers,
                cookies=cookies,
                impersonate="chrome142",
                verify=False,
                timeout=35
            )
            if response.status_code == 200:
                logging.info(f"✅ Загружено (попытка {attempt})")
                # Отладка: сохраняем первые 500 символов в лог
                html_preview = response.text[:500]
                logging.info(f"HTML preview: {html_preview}")
                # Если нет карточек, сохраняем полный HTML в файл
                if 's-card' not in html_preview and 's-item' not in html_preview and '/itm/' not in html_preview:
                    with open('ebay_debug.html', 'w', encoding='utf-8') as f:
                        f.write(response.text)
                    logging.error("Страница не содержит карточек товаров. Полный HTML сохранён в ebay_debug.html")
                return response.text
            else:
                logging.warning(f"HTTP {response.status_code} (попытка {attempt})")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
        except Exception as e:
            logging.error(f"Ошибка запроса: {e} (попытка {attempt})")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    return None

# ============ ПАРСИНГ (НОВЫЕ СЕЛЕКТОРЫ) ============
def parse_ebay_listings(html, max_items=MAX_ITEMS):
    if not html:
        return {}
    soup = BeautifulSoup(html, 'html.parser')
    # Ищем карточки – приоритет на новые классы
    cards = soup.select('div.s-card, li.s-card')
    if not cards:
        cards = soup.select('li.s-item')
    if not cards:
        logging.warning("Карточки не найдены. На странице нет s-card или s-item")
        return {}
    
    logging.info(f"Найдено карточек: {len(cards)}")
    items = {}
    processed = 0
    for card in cards:
        if processed >= max_items:
            break
        # Ссылка
        link = card.select_one('a[href*="/itm/"]')
        if not link:
            continue
        url = link.get('href')
        if url.startswith('/'):
            url = 'https://www.ebay.co.uk' + url
        item_id_match = re.search(r'/itm/(\d+)', url)
        if not item_id_match:
            continue
        item_id = item_id_match.group(1)
        
        # Название
        title_elem = card.select_one('span.s-card__title')
        if not title_elem:
            title_elem = card.select_one('div[role="heading"]')
        if not title_elem:
            title_elem = link
        title = title_elem.get_text(strip=True)
        title = re.sub(r'(?i)new listing', '', title).strip()
        if not title:
            continue
        
        # Цена
        price_elem = card.select_one('span.s-card__price')
        if not price_elem:
            price_elem = card.select_one('span.su-styled-text.primary.bold.large-1')
        price = price_elem.get_text(strip=True) if price_elem else None
        if price and not re.search(r'£', price):
            price = None  # не GBP
        
        # Доставка
        shipping = None
        for elem in card.select('span.su-styled-text.secondary.large'):
            text = elem.get_text(strip=True)
            if 'delivery' in text.lower() or 'shipping' in text.lower():
                match = re.search(r'([£€$]\s*[\d,]+\.?\d*)', text)
                if match:
                    shipping = match.group(1)
                    break
                elif 'free' in text.lower():
                    shipping = "Бесплатно"
                    break
        
        # Best Offer
        best_offer = False
        for elem in card.select('span.su-styled-text.secondary.large'):
            if 'or Best Offer' in elem.get_text():
                best_offer = True
                break
        
        # Аукцион
        auction = bool(re.search(r'\d+\s+bids?\b', card.get_text(), re.I))
        
        items[item_id] = {
            'url': url,
            'title': title,
            'price': price,
            'shipping': shipping,
            'best_offer': best_offer,
            'auction': auction
        }
        processed += 1
    logging.info(f"Обработано товаров: {len(items)}")
    return items

def perform_initial_snapshot():
    logging.info("Начальный снимок...")
    html = fetch_ebay_html_with_retry()
    if not html:
        return False
    items = parse_ebay_listings(html, max_items=50)
    if not items:
        logging.warning("Не удалось извлечь товары. Структура страницы могла измениться.")
        return False
    add_seen_ids_batch(list(items.keys()))
    logging.info(f"Снимок: добавлено {len(items)} товаров")
    return True

def check_and_send_new_items():
    seen = get_seen_ids()
    logging.info(f"В базе {len(seen)} товаров")
    html = fetch_ebay_html_with_retry()
    if not html:
        return
    current = parse_ebay_listings(html)
    new = []
    for item_id, data in current.items():
        if item_id not in seen:
            new.append({'id': item_id, **data})
            logging.info(f"НОВЫЙ: {data['title'][:50]} | цена: {data['price']} | доставка: {data['shipping']}")
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

def telegram_listener():
    global is_paused
    last_update_id = 0
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
                            send_telegram_message("▶ Бот продолжает работу")
                        elif text == '/reset':
                            reset_database()
                            send_telegram_message("🔄 База очищена. При следующей проверке бот сделает новый снимок.")
            time.sleep(1)
        except Exception as e:
            logging.error(f"Ошибка слушателя: {e}")
            time.sleep(5)

def bot_worker():
    global is_paused
    logging.info("Бот-воркер запущен")
    init_db()
    if is_db_empty():
        if not perform_initial_snapshot():
            send_telegram_message("❌ Ошибка инициализации: не удалось получить товары")
            return
        send_telegram_message("✅ Бот запущен, начальный снимок сделан")
    else:
        send_telegram_message("✅ Бот перезапущен")
    while True:
        if is_paused:
            time.sleep(2)
            continue
        try:
            check_and_send_new_items()
            wait = max(60, CHECK_INTERVAL + random.uniform(-30, 60))
            logging.info(f"Следующая проверка через {int(wait)} секунд")
            time.sleep(wait)
        except Exception as e:
            logging.error(f"Ошибка в цикле: {e}", exc_info=True)
            time.sleep(120)

@app.route('/')
def index():
    return "eBay bot is running (UK, debug)"

@app.route('/health')
def health():
    return "OK", 200

if __name__ == "__main__":
    send_telegram_message("🚀 Бот запущен (UK, отладка). Команды: /stop /start /reset")
    threading.Thread(target=telegram_listener, daemon=True).start()
    worker = threading.Thread(target=bot_worker, daemon=False)
    worker.start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
