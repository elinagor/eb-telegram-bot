import os
import sys
import ssl
import time
import random
import re
import json
import threading
import logging
import requests
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
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "60"))
DATABASE_URL = os.getenv("DATABASE_URL")
MAX_ITEMS = 20
MAX_RETRIES = 20
RETRY_DELAY = 5

# Параметры для UK (товары из Великобритании, сортировка по новизне)
if '?' in EBAY_SEARCH_URL:
    EBAY_SEARCH_URL += '&LH_PrefLoc=3&_ipg=240&_sop=10'
else:
    EBAY_SEARCH_URL += '?LH_PrefLoc=3&_ipg=240&_sop=10'

if not all([EBAY_SEARCH_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DATABASE_URL]):
    logging.error("Не хватает переменных окружения.")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
app = Flask(__name__)

is_paused = False

# ============ РОТАЦИЯ USER-AGENT ============
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:144.0) Gecko/20100101 Firefox/144.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
]

# ============ РАБОТА С БАЗОЙ ДАННЫХ ============
def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def init_db():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS seen_items (item_id TEXT PRIMARY KEY, first_seen TIMESTAMP)")
        conn.commit()

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

# ============ ОТПРАВКА В TELEGRAM ============
def send_telegram_message(message, parse_mode='HTML'):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': parse_mode, 'disable_web_page_preview': False}
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            logging.error(f"Ошибка Telegram: {r.text}")
    except Exception as e:
        logging.error(f"Не удалось отправить в Telegram: {e}")

# ============ СЛУШАТЕЛЬ КОМАНД ============
def telegram_listener():
    global is_paused
    logging.info("🔁 Поток слушателя команд Telegram запущен")
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
                    message = update.get('message')
                    if message and str(message.get('chat', {}).get('id')) == TELEGRAM_CHAT_ID:
                        text = message.get('text', '').strip()
                        if text == '/stop':
                            is_paused = True
                            send_telegram_message("⏸ Бот приостановлен. Для возобновления отправьте /start")
                        elif text == '/start':
                            is_paused = False
                            send_telegram_message("▶ Бот продолжает работу")
                        elif text == '/reset':
                            reset_database()
                            send_telegram_message("🔄 База данных очищена. При следующей проверке будет новый снимок.")
            time.sleep(1)
        except Exception as e:
            logging.error(f"Ошибка в слушателе Telegram: {e}")
            time.sleep(5)

# ============ ЗАПРОС К EBAY С ОТЛАДКОЙ ============
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
                # Сохраняем первые 2000 символов HTML в лог для отладки
                html_preview = response.text[:2000]
                logging.info(f"✅ Загружено (попытка {attempt}, UA={current_ua[:40]}...)")
                # Проверяем, есть ли признаки капчи или редиректа
                if "robot" in html_preview.lower() or "captcha" in html_preview.lower():
                    logging.error("Обнаружена капча или блокировка! Страница содержит robot/captcha.")
                    # Пробуем другой UA и увеличиваем задержку
                    time.sleep(10)
                    continue
                if "ebay.com/sch/" not in html_preview and "ebay.co.uk/sch/" not in html_preview:
                    logging.warning("Похоже, страница не содержит результатов поиска. Первые 500 символов: " + html_preview[:500])
                return response.text
            elif response.status_code == 403:
                logging.warning(f"⚠️ 403 Forbidden (попытка {attempt})")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY + random.uniform(2, 5))
            else:
                logging.warning(f"Попытка {attempt}/{MAX_RETRIES}: HTTP {response.status_code}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
        except Exception as e:
            logging.error(f"Попытка {attempt}/{MAX_RETRIES}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY + random.uniform(1, 3))
    return None

def extract_item_id(url):
    if not url:
        return None
    # Ищем /itm/ в любом месте URL
    match = re.search(r'/itm/(\d+)', url)
    if match:
        return match.group(1)
    return None

def clean_title(title):
    if not title: return ""
    title = re.sub(r'(?i)new\s*listing', '', title)
    title = re.sub(r'(?i)\blisting\b', '', title)
    title = re.sub(r'(?i)\bnew\b', '', title)
    title = re.sub(r'[^\w\s£€$]', ' ', title)
    title = re.sub(r'\s+', ' ', title).strip()
    return title

def is_gbp_price(text):
    if not text: return False
    if re.search(r'£|\bGBP\b', text, re.I):
        return True
    if re.search(r'[$€]|USD|EUR', text, re.I):
        return False
    return re.search(r'\d', text) is not None

# ============ НОВАЯ УНИВЕРСАЛЬНАЯ ФУНКЦИЯ ПАРСИНГА ============
def parse_ebay_listings(html, max_items=MAX_ITEMS):
    if not html:
        return {}
    soup = BeautifulSoup(html, 'html.parser')
    
    # Универсальный поиск всех ссылок, содержащих /itm/
    all_links = soup.find_all('a', href=True)
    item_links = []
    for link in all_links:
        href = link.get('href', '')
        if '/itm/' in href:
            item_links.append(link)
        elif 'ebay.co.uk/itm/' in href:
            item_links.append(link)
        elif 'ebay.com/itm/' in href:
            item_links.append(link)
    
    logging.info(f"Найдено ссылок /itm/: {len(item_links)}")
    if not item_links:
        # Дополнительная отладка: записываем в файл HTML для анализа
        with open('ebay_debug.html', 'w', encoding='utf-8') as f:
            f.write(html)
        logging.error("HTML сохранён в ebay_debug.html. Проверьте содержимое вручную.")
        return {}
    
    items = {}
    processed = 0
    for link in item_links:
        if processed >= max_items:
            break
        url = link.get('href')
        if not url:
            continue
        if url.startswith('/'):
            url = 'https://www.ebay.co.uk' + url
        item_id = extract_item_id(url)
        if not item_id:
            continue
        
        # Пытаемся найти название: ближайший элемент с классом или просто текст ссылки
        title = None
        # Ищем родительский div, который может содержать название
        parent = link
        for _ in range(4):
            title_span = parent.find_previous_sibling('span', class_=re.compile(r's-card__title|s-item__title'))
            if title_span:
                title = clean_title(title_span.get_text(strip=True))
                break
            # Ищем div с ролью heading
            heading_div = parent.find_previous_sibling('div', {'role': 'heading'})
            if heading_div:
                title = clean_title(heading_div.get_text(strip=True))
                break
            parent = parent.parent
        if not title:
            title = clean_title(link.get_text(strip=True))
        if not title or len(title) < 3:
            continue
        
        # Цена: ищем ближайший span с классом содержащим price или s-card__price
        price = None
        # Ищем в том же контейнере (родительский блок вокруг ссылки)
        container = link.parent
        for _ in range(3):
            price_elem = container.select_one('span.s-card__price, span.su-styled-text.primary.bold.large-1, span.s-item__price')
            if price_elem:
                price = price_elem.get_text(strip=True)
                if is_gbp_price(price):
                    break
                else:
                    price = None
            container = container.parent
        if price and not is_gbp_price(price):
            price = None
        
        # Доставка
        shipping = None
        shipping_elem = None
        # Ищем span с классом su-styled-text secondary large и текстом delivery
        for elem in link.parent.find_all('span', class_='su-styled-text'):
            text = elem.get_text(strip=True)
            if 'delivery' in text or 'shipping' in text:
                shipping_elem = elem
                break
        if shipping_elem:
            text = shipping_elem.get_text(strip=True)
            if 'free' in text.lower():
                shipping = "Бесплатно"
            else:
                match = re.search(r'([£€$]\s*[\d,]+\.?\d*)', text)
                if match:
                    shipping = match.group(1)
        if not shipping:
            # fallback
            shipping_text = link.parent.get_text()
            if 'free shipping' in shipping_text.lower():
                shipping = "Бесплатно"
            else:
                match = re.search(r'\+?([£€$]\s*[\d,]+\.?\d*)\s*delivery', shipping_text.lower())
                if match:
                    shipping = match.group(1)
        
        # Best Offer
        best_offer = False
        for elem in link.parent.find_all('span', class_='su-styled-text'):
            if 'or Best Offer' in elem.get_text():
                best_offer = True
                break
        
        # Auction
        auction = False
        if re.search(r'\d+\s+bids?', link.parent.get_text(), re.I):
            auction = True
        
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

# ============ СНИМОК И ОТПРАВКА ============
def perform_initial_snapshot():
    logging.info("Начальный снимок...")
    html = fetch_ebay_html_with_retry()
    if not html:
        return False
    items = parse_ebay_listings(html, max_items=50)
    if not items:
        logging.warning("Снимок не дал товаров. Возможно, изменилась структура страницы.")
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
    current = parse_ebay_listings(html)
    new = []
    for item_id, data in current.items():
        if item_id not in seen:
            new.append({'id': item_id, **data})
            logging.info(f"НОВЫЙ: {data['title'][:50]}... цена: {data['price']}, доставка: {data.get('shipping')}, best_offer: {data.get('best_offer')}, auction: {data.get('auction')}")
    if new:
        for item in new:
            msg = f"🇬🇧 <b>НОВЫЙ ТОВАР Англия</b> 🇬🇧\n\n<b>{item['title']}</b>\n\n"
            if item['price']:
                msg += f"💰 Цена: {item['price']}\n"
            else:
                msg += f"💰 Цена не указана (не GBP)\n"
            if item['shipping']:
                msg += f"🚚 Доставка: {item['shipping']}\n"
            else:
                msg += f"🚚 Доставка: не указана\n"
            if item.get('best_offer', False):
                msg += f"✅ Сделать предложение (Best Offer)\n"
            if item.get('auction', False):
                msg += f"⏰ Аукцион\n"
            msg += f"\n🔗 <a href='{item['url']}'>Ссылка на товар</a>"
            send_telegram_message(msg)
            add_seen_ids_batch([item['id']])
            time.sleep(1)
    else:
        logging.info("Новых нет")

def bot_worker():
    global is_paused
    logging.info("🤖 Бот-воркер запущен")
    init_db()
    if is_db_empty():
        if not perform_initial_snapshot():
            send_telegram_message("❌ Ошибка инициализации: не удалось получить товары. Проверьте URL или структуру eBay.")
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
            logging.info(f"Следующая проверка через {wait:.0f} секунд.")
            time.sleep(wait)
        except Exception as e:
            logging.error(f"Ошибка в основном цикле: {e}", exc_info=True)
            time.sleep(120)

@app.route('/')
def index():
    return "eBay бот работает (UK, универсальный парсинг)"

@app.route('/health')
def health():
    return "OK", 200

if __name__ == "__main__":
    send_telegram_message("🚀 Бот запущен (UK). Команды: /stop /start /reset. В случае ошибки проверяйте логи.")
    threading.Thread(target=telegram_listener, daemon=True).start()
    worker_thread = threading.Thread(target=bot_worker, daemon=False)
    worker_thread.start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
