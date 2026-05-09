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

# ============ ЗАПРОС К EBAY (БЕЗ ПРОКСИ) ============
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
                logging.info(f"✅ Загружено (попытка {attempt}, UA={current_ua[:40]}...)")
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
    if re.search(r'£|\bGBP\b', text, re.I):
        return True
    if re.search(r'[$€]|USD|EUR', text, re.I):
        return False
    return re.search(r'\d', text) is not None

# ============ НОВЫЕ ФУНКЦИИ ПАРСИНГА ДЛЯ НОВЫХ КЛАССОВ ============
def extract_price_from_card(card):
    """Ищет цену в карточке нового формата."""
    # Сначала ищем span.s-card__price
    price_span = card.select_one('span.s-card__price')
    if price_span:
        text = price_span.get_text(strip=True)
        if is_gbp_price(text):
            return text
    # Если не нашли, пробуем su-styled-text primary bold large-1
    fallback = card.select_one('span.su-styled-text.primary.bold.large-1')
    if fallback:
        text = fallback.get_text(strip=True)
        if is_gbp_price(text):
            return text
    # Старый метод (на всякий случай)
    return extract_price_css(card)

def extract_price_css(card):
    """Старый метод – оставлен для совместимости."""
    candidates = []
    selectors = ['span.s-item__price', '[data-testid="item-price"]', '.s-item__detail .s-item__price']
    for sel in selectors:
        for elem in card.select(sel):
            text = elem.get_text(strip=True)
            if text:
                candidates.append(text)
    for elem in card.select('[class*="price"]'):
        text = elem.get_text(strip=True)
        if text:
            candidates.append(text)
    for cand in candidates:
        if is_gbp_price(cand):
            parts = cand.split()
            for p in parts:
                if is_gbp_price(p):
                    return p
            return cand
    if candidates:
        return candidates[0]
    return None

def extract_shipping_from_card(card):
    """Извлекает стоимость доставки из карточки нового формата."""
    # Ищем элемент с классом su-styled-text secondary large и текстом, содержащим "delivery"
    for elem in card.select('span.su-styled-text.secondary.large'):
        text = elem.get_text(strip=True)
        if not text:
            continue
        if re.search(r'delivery|shipping', text, re.I):
            # Извлекаем цену, например "+£1.55"
            match = re.search(r'([£€$]\s*[\d,]+\.?\d*)', text)
            if match:
                return match.group(1)
            if 'free' in text.lower():
                return "Бесплатно"
            return text
    # Старый метод – на случай, если новый не сработал
    return extract_shipping(card)

def extract_shipping(card, item_price=None):
    """Старая универсальная функция – оставлена как fallback."""
    script = card.find('script', type='application/ld+json')
    if script and script.string:
        try:
            data = json.loads(script.string)
            if isinstance(data, dict):
                offers = data.get('offers')
                if isinstance(offers, dict):
                    shipping = offers.get('shippingCost')
                    if shipping is not None:
                        if shipping == 0 or str(shipping) == '0':
                            return "Бесплатно"
                        if isinstance(shipping, (int, float)):
                            currency = offers.get('priceCurrency', '')
                            amount = f"{currency} {shipping}" if currency else str(shipping)
                            return amount
        except:
            pass
    # Поиск по CSS-селекторам
    shipping_selectors = [
        'span.s-item__shipping', 'div.s-item__shipping',
        'span.s-item__logisticsCost', 'span.s-item__delivery',
        'span.su-styled-text', '.su-styled-text.secondary.large',
    ]
    for sel in shipping_selectors:
        for elem in card.select(sel):
            text = elem.get_text(strip=True)
            if not text:
                continue
            if re.search(r'(?i)(buy it now|best offer|make offer|watch)', text):
                continue
            if re.search(r'(?i)free|delivery|shipping', text):
                if 'free' in text.lower():
                    return "Бесплатно"
                match = re.search(r'([£€$]\s*[\d,]+\.?\d*)', text)
                if match:
                    return match.group(1)
    return None

def extract_best_offer_from_card(card):
    """Ищет индикатор 'or Best Offer' в карточке нового формата."""
    for elem in card.select('span.su-styled-text.secondary.large'):
        text = elem.get_text(strip=True)
        if re.search(r'or\s+best\s+offer', text, re.I):
            return True
    # Старый метод на случай редизайна
    return extract_best_offer(card)

def extract_best_offer(card):
    """Старая функция для обратной совместимости."""
    text = card.get_text()
    if re.search(r'or\s+best\s+offer', text, re.I):
        return True
    best_offer_selectors = [
        '.s-item__best-offer', '.s-item__detail--best-offer', '.s-item__bonus',
        '[class*="bestOffer"]', '[class*="best-offer"]'
    ]
    for sel in best_offer_selectors:
        if card.select_one(sel):
            return True
    return False

def extract_auction_from_card(card):
    """Определяет аукцион по наличию bids."""
    text = card.get_text()
    if re.search(r'\d+\s+bids?\b', text, re.I):
        return True
    if re.search(r'\bplace\s+bid\b', text, re.I):
        return True
    # Специфичные классы для аукционов
    if card.select_one('.s-item__bid-count, .s-card__bid-count'):
        return True
    return False

# ============ ОСНОВНАЯ ФУНКЦИЯ ПАРСИНГА (НОВАЯ) ============
def parse_ebay_listings(html, max_items=MAX_ITEMS):
    if not html:
        return {}
    soup = BeautifulSoup(html, 'html.parser')
    # Новый основной селектор – карточка с классом s-card (может быть div или li)
    cards = soup.select('div.s-card, li.s-card')
    if not cards:
        # Fallback: ищем старые .s-item
        cards = soup.select('li.s-item')
    if not cards:
        # Второй fallback: ищем любые элементы, содержащие ссылку /itm/
        return parse_ebay_listings_fallback(soup, max_items)
    
    logging.info(f"Найдено карточек: {len(cards)}")
    items = {}
    processed = 0
    for card in cards:
        if processed >= max_items:
            break
        # Ссылка на товар
        link_elem = card.select_one('a[href*="/itm/"]')
        if not link_elem:
            continue
        url = link_elem.get('href')
        if url.startswith('/'):
            url = 'https://www.ebay.co.uk' + url
        item_id = extract_item_id(url)
        if not item_id:
            continue
        # Название: ищем span.s-card__title или div[role="heading"]
        title_elem = card.select_one('span.s-card__title')
        if not title_elem:
            title_elem = card.select_one('div[role="heading"]')
        if not title_elem:
            title_elem = link_elem
        raw_title = title_elem.get_text(strip=True) if title_elem else ''
        title = clean_title(raw_title)
        if not title:
            continue
        # Цена
        price = extract_price_from_card(card)
        if price and not is_gbp_price(price):
            price = None
        # Доставка
        shipping = extract_shipping_from_card(card)
        if shipping and shipping != "Бесплатно" and not is_gbp_price(shipping):
            shipping = None
        # Best Offer
        best_offer = extract_best_offer_from_card(card)
        # Аукцион
        auction = extract_auction_from_card(card)
        
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

def parse_ebay_listings_fallback(soup, max_items):
    items = {}
    links = soup.find_all('a', href=True)
    itm_links = [link for link in links if '/itm/' in link['href']]
    logging.info(f"Fallback: найдено ссылок /itm/: {len(itm_links)}")
    itm_links = itm_links[:max_items]
    for link in itm_links:
        url = link.get('href')
        if url.startswith('/'):
            url = 'https://www.ebay.co.uk' + url
        item_id = extract_item_id(url)
        if not item_id:
            continue
        title = clean_title(link.get_text(strip=True))
        if not title:
            continue
        price = None
        shipping = None
        best_offer = False
        auction = False
        parent = link.parent
        for _ in range(5):
            if parent:
                price = extract_price_from_card(parent)
                if price and not is_gbp_price(price):
                    price = None
                shipping = extract_shipping_from_card(parent)
                if shipping and shipping != "Бесплатно" and not is_gbp_price(shipping):
                    shipping = None
                best_offer = extract_best_offer_from_card(parent)
                auction = extract_auction_from_card(parent)
                if price or shipping or best_offer or auction:
                    break
                parent = parent.parent
        items[item_id] = {
            'url': url,
            'title': title,
            'price': price,
            'shipping': shipping,
            'best_offer': best_offer,
            'auction': auction
        }
    logging.info(f"Fallback обработано товаров: {len(items)}")
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
    return "eBay бот работает (UK, новые селекторы s-card)"

@app.route('/health')
def health():
    return "OK", 200

if __name__ == "__main__":
    send_telegram_message("🚀 Бот запущен (UK). Команды: /stop /start /reset")
    threading.Thread(target=telegram_listener, daemon=True).start()
    worker_thread = threading.Thread(target=bot_worker, daemon=False)
    worker_thread.start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
