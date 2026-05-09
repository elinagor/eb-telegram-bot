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
    print("curl_cffi не установлен. Добавьте в requirements.txt", file=sys.stderr)
    sys.exit(1)

load_dotenv()

# ============ НАСТРОЙКИ ============
EBAY_SEARCH_URL = os.getenv("EBAY_SEARCH_URL")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "60"))
BRIGHT_DATA_PROXY_URL = os.getenv("BRIGHT_DATA_PROXY_URL")
DATABASE_URL = os.getenv("DATABASE_URL")
MAX_ITEMS = 20
MAX_RETRIES = 5
RETRY_DELAY = 5

# Проверка обязательных переменных
missing = []
if not TELEGRAM_BOT_TOKEN:
    missing.append("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_CHAT_ID:
    missing.append("TELEGRAM_CHAT_ID")
if not BRIGHT_DATA_PROXY_URL:
    missing.append("BRIGHT_DATA_PROXY_URL")
if not DATABASE_URL:
    missing.append("DATABASE_URL")
if missing:
    print(f"❌ Не хватает переменных: {', '.join(missing)}", file=sys.stderr)
    sys.exit(1)

# Если EBAY_SEARCH_URL не задан, используем поиск по умолчанию
if not EBAY_SEARCH_URL:
    EBAY_SEARCH_URL = "https://www.ebay.co.uk/sch/i.html?_from=R40&_nkw=cross+stitch+kit&_sacat=34017"
    print("⚠️ EBAY_SEARCH_URL не задан, использую значение по умолчанию", file=sys.stderr)

# ============ ФОРМИРОВАНИЕ МОБИЛЬНОГО URL ============
print(f"🔧 Исходный URL: {EBAY_SEARCH_URL}", file=sys.stderr)

# Заменяем домен на мобильный и удаляем /sch/i.html
EBAY_SEARCH_URL = EBAY_SEARCH_URL.replace("www.ebay.co.uk", "m.ebay.co.uk")
EBAY_SEARCH_URL = EBAY_SEARCH_URL.replace("/sch/i.html", "")

# Разбираем параметры
if '?' in EBAY_SEARCH_URL:
    base, query = EBAY_SEARCH_URL.split('?', 1)
    params = {}
    for pair in query.split('&'):
        if '=' in pair:
            key, val = pair.split('=', 1)
            params[key] = val
else:
    base = EBAY_SEARCH_URL
    params = {}

if '_nkw' not in params:
    params['_nkw'] = 'cross+stitch+kit'
if '_dcat' in params:
    params['_sacat'] = params.pop('_dcat')
params['_sop'] = '10'
params['_ipg'] = '20'
params['LH_PrefLoc'] = '3'
if '_fcid' not in params:
    params['_fcid'] = '3'
if '_stpos' not in params:
    params['_stpos'] = 'E107QF'

# Удаляем мусорные параметры
for bad in ['_from', 'rt', 'LH_TitleDesc', 'LH_Specifics', 'df']:
    params.pop(bad, None)

new_query = '&'.join(f"{k}={v}" for k, v in params.items())
EBAY_SEARCH_URL = f"{base}?{new_query}"
print(f"✅ Финальный URL: {EBAY_SEARCH_URL}", file=sys.stderr)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
app = Flask(__name__)

is_paused = False

# ============ МОБИЛЬНЫЕ USER-AGENT ============
USER_AGENTS = [
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 17_5_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; SM-S921B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.6998.71 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.6943.121 Mobile Safari/537.36",
]

# ============ БАЗА ДАННЫХ ============
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
            time.sleep(1)
        except Exception as e:
            logging.error(f"Ошибка в слушателе Telegram: {e}")
            time.sleep(5)

# ============ ЗАПРОС К EBAY ============
def fetch_ebay_html_with_retry():
    proxies = {"http": BRIGHT_DATA_PROXY_URL, "https": BRIGHT_DATA_PROXY_URL}
    cookies = {'ebay': '%2F', 'm': 'GB', 's': 'UK', 'siteid': '3'}
    for attempt in range(1, MAX_RETRIES + 1):
        current_ua = random.choice(USER_AGENTS)
        headers = {
            'User-Agent': current_ua,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-GB,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://m.ebay.co.uk/',
        }
        if attempt > 1:
            time.sleep(random.uniform(1.5, 3.5))
        try:
            response = cffi_requests.get(
                EBAY_SEARCH_URL,
                headers=headers,
                cookies=cookies,
                impersonate="chrome124",
                proxies=proxies,
                verify=False,
                timeout=35
            )
            if response.status_code == 200:
                content_encoding = response.headers.get('Content-Encoding', 'none')
                size_kb = len(response.content) / 1024
                logging.info(f"✅ Загружено {size_kb:.1f} KB, сжатие: {content_encoding} (попытка {attempt})")
                # Для отладки можно сохранить HTML (раскомментировать при необходимости)
                # with open(f"debug_{int(time.time())}.html", "w", encoding="utf-8") as f:
                #     f.write(response.text)
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
    if not url or '/itm/' not in url:
        return None
    try:
        return url.split('/itm/')[1].split('?')[0]
    except IndexError:
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
    return '£' in text or 'GBP' in text.upper()

def parse_ebay_listings(html, max_items=MAX_ITEMS):
    if not html:
        return {}
    soup = BeautifulSoup(html, 'html.parser')
    
    # Пробуем разные селекторы для мобильной версии
    cards = soup.select('li.s-item')  # десктопный
    if not cards:
        cards = soup.select('.s-item')  # общий
    if not cards:
        # Мобильная версия часто использует div с class="item-card"
        cards = soup.select('div.item-card')
    if not cards:
        # Универсальный поиск: любые ссылки с /itm/ внутри родительского блока
        cards = soup.find_all('div', class_=re.compile(r'item|result|listing'))
        if not cards:
            # Запасной вариант: ищем все ссылки на товары и пытаемся извлечь информацию
            return parse_ebay_listings_fallback(soup, max_items)
    
    items = {}
    processed = 0
    for card in cards:
        if processed >= max_items:
            break
        
        # Ищем ссылку на товар
        link = card.select_one('a[href*="/itm/"]')
        if not link:
            link = card.find('a', href=re.compile(r'/itm/'))
        if not link:
            continue
        
        url = link.get('href')
        if not url:
            continue
        if url.startswith('/'):
            url = 'https://m.ebay.co.uk' + url
        
        item_id = extract_item_id(url)
        if not item_id:
            continue
        
        # Заголовок
        title_elem = (card.select_one('.item-title, .title, .listing-title, .s-item__title span[role="heading"], span[role="heading"]') or
                      card.select_one('h3, .heading') or link)
        title = clean_title(title_elem.get_text(strip=True)) if title_elem else ''
        if not title:
            title = clean_title(link.get_text(strip=True))
            if not title:
                continue
        
        # Цена (ищем символ £)
        price = None
        price_candidates = []
        # Ищем элементы с ценой
        price_selectors = ['.price', '.item-price', '.s-item__price', '[class*="price"]', '.value']
        for sel in price_selectors:
            for elem in card.select(sel):
                txt = elem.get_text(strip=True)
                if '£' in txt:
                    price_candidates.append(txt)
        if not price_candidates:
            # Поиск в любом тексте карточки
            all_text = card.get_text()
            match = re.search(r'£[\d,]+\.?\d*', all_text)
            if match:
                price = match.group(0)
        else:
            # Берём первый кандидат с £
            for cand in price_candidates:
                match = re.search(r'£[\d,]+\.?\d*', cand)
                if match:
                    price = match.group(0)
                    break
        
        # Доставка
        shipping = None
        if 'free shipping' in card.get_text().lower():
            shipping = "Бесплатно"
        else:
            match = re.search(r'\+?\s*£[\d,]+\.?\d*\s*(postage|delivery|shipping)', card.get_text().lower())
            if match:
                shipping = re.search(r'£[\d,]+\.?\d*', match.group(0)).group(0)
        
        # Best Offer
        best_offer = 'best offer' in card.get_text().lower()
        
        # Аукцион
        auction = bool(re.search(r'\d+\s+bids?', card.get_text().lower()))
        
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
    if len(items) == 0:
        # Дополнительная диагностика: записать начало HTML в лог
        logging.warning("Не найдено ни одного товара. Первые 500 символов HTML:")
        logging.warning(html[:500])
    return items

def parse_ebay_listings_fallback(soup, max_items):
    """Запасной парсер: ищем все ссылки /itm/ и пытаемся вытащить минимум данных"""
    items = {}
    links = soup.find_all('a', href=re.compile(r'/itm/'))
    for link in links[:max_items]:
        url = link.get('href')
        if not url:
            continue
        if url.startswith('/'):
            url = 'https://m.ebay.co.uk' + url
        item_id = extract_item_id(url)
        if not item_id:
            continue
        title = clean_title(link.get_text(strip=True))
        if not title:
            continue
        
        # Поиск цены в родительских элементах
        price = None
        parent = link.parent
        for _ in range(5):
            if parent:
                text = parent.get_text()
                match = re.search(r'£[\d,]+\.?\d*', text)
                if match:
                    price = match.group(0)
                    break
                parent = parent.parent
        
        shipping = None
        if 'free shipping' in link.get_text().lower():
            shipping = "Бесплатно"
        
        best_offer = 'best offer' in link.get_text().lower()
        auction = bool(re.search(r'\d+\s+bids?', link.get_text().lower()))
        
        items[item_id] = {
            'url': url,
            'title': title,
            'price': price,
            'shipping': shipping,
            'best_offer': best_offer,
            'auction': auction
        }
    logging.info(f"Fallback парсер обработал товаров: {len(items)}")
    return items

def perform_initial_snapshot():
    logging.info("Начальный снимок...")
    html = fetch_ebay_html_with_retry()
    if not html:
        return False
    items = parse_ebay_listings(html, max_items=50)
    if not items:
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
                msg += f"💰 Цена не указана\n"
            if item.get('shipping'):
                msg += f"🚚 Доставка: {item['shipping']}\n"
            if item.get('best_offer'):
                msg += f"✅ Сделать предложение (Best Offer)\n"
            if item.get('auction'):
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
    try:
        init_db()
    except Exception as e:
        logging.error(f"Ошибка БД: {e}")
        send_telegram_message(f"❌ Ошибка БД: {e}")
        return
    if is_db_empty():
        if not perform_initial_snapshot():
            send_telegram_message("❌ Ошибка инициализации: не удалось получить начальный снимок")
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
            time.sleep(CHECK_INTERVAL)
        except Exception as e:
            logging.error(f"Ошибка в основном цикле: {e}", exc_info=True)
            time.sleep(120)

@app.route('/')
def index():
    return "eBay бот работает (мобильная версия)"

@app.route('/health')
def health():
    return "OK", 200

if __name__ == "__main__":
    print("🚀 Запуск бота...", file=sys.stderr)
    send_telegram_message("🚀 Бот запущен (мобильная версия, категория Cross Stitch)")
    threading.Thread(target=telegram_listener, daemon=True).start()
    worker_thread = threading.Thread(target=bot_worker, daemon=False)
    worker_thread.start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
