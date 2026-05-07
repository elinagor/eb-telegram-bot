import os
import sys
import time
import random
import sqlite3
import threading
import logging
import re
from datetime import datetime
from bs4 import BeautifulSoup
from flask import Flask
from dotenv import load_dotenv

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    print("curl_cffi не установлен. Добавьте в requirements.txt")
    sys.exit(1)

load_dotenv()

# ========== НАСТРОЙКИ ==========
EBAY_SEARCH_URL = os.getenv("EBAY_SEARCH_URL")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "600"))
# ===============================

if not EBAY_SEARCH_URL or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    logging.error("Не хватает переменных окружения.")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

# ==================== ЗАГРУЗКА ПРОКСИ (ТОЛЬКО HTTP/HTTPS) ====================
def fetch_proxy_list():
    """Загружает список HTTP/HTTPS прокси с двух источников."""
    proxies = []
    
    # Источник 1: free-proxy-list.net (парсим таблицу)
    try:
        import requests
        logging.info("Загрузка прокси с free-proxy-list.net...")
        resp = requests.get("https://free-proxy-list.net/", timeout=30)
        soup = BeautifulSoup(resp.text, 'html.parser')
        table = soup.find('table', id='proxylisttable')
        if table:
            for row in table.find_all('tr')[1:]:
                cols = row.find_all('td')
                if len(cols) >= 7:
                    ip = cols[0].text.strip()
                    port = cols[1].text.strip()
                    https = cols[6].text.strip() == 'yes'
                    if https:
                        proxies.append(f"https://{ip}:{port}")
                    else:
                        proxies.append(f"http://{ip}:{port}")
        logging.info(f"С free-proxy-list.net получено {len(proxies)} прокси")
    except Exception as e:
        logging.error(f"Ошибка загрузки free-proxy-list: {e}")
    
    # Источник 2: proxyscrape.com (только HTTP/HTTPS, elite+anonymous)
    try:
        logging.info("Загрузка прокси с proxyscrape.com...")
        url = "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=elite%2Canonymous"
        resp = requests.get(url, timeout=30)
        raw = resp.text.split()
        for p in raw:
            p = p.strip()
            if p:
                if ':' in p:
                    # формат ip:port, добавляем http://
                    proxies.append(f"http://{p}")
                else:
                    proxies.append(f"http://{p}")
        logging.info(f"С proxyscrape.com получено {len(raw)} прокси")
    except Exception as e:
        logging.error(f"Ошибка загрузки proxyscrape: {e}")
    
    # Убираем дубликаты
    proxies = list(dict.fromkeys(proxies))
    logging.info(f"Всего уникальных прокси: {len(proxies)}")
    return proxies

def quick_check_proxy(proxy_url):
    """Быстрая проверка прокси (таймаут 5 секунд) на доступность."""
    test_url = "https://www.ebay.com"
    proxies = {"http": proxy_url, "https": proxy_url}
    try:
        response = cffi_requests.get(test_url, proxies=proxies, impersonate="chrome124", timeout=5)
        return response.status_code == 200
    except Exception:
        return False

# ==================== УМНЫЙ ПУЛ С ПРЕДВАРИТЕЛЬНОЙ ПРОВЕРКОЙ ====================
class SmartProxyPool:
    def __init__(self):
        self.working_proxies = []   # список проверенных рабочих прокси
        self.lock = threading.Lock()
        self.refresh_proxies()      # первичная загрузка и проверка
        # Фоновое обновление каждый час
        threading.Thread(target=self._refresh_loop, daemon=True).start()

    def _refresh_loop(self):
        while True:
            time.sleep(3600)
            self.refresh_proxies()

    def refresh_proxies(self):
        """Загружает свежие прокси, быстро проверяет и обновляет пул."""
        logging.info("Обновление пула прокси (может занять 1-2 минуты)...")
        all_proxies = fetch_proxy_list()
        if not all_proxies:
            logging.warning("Не удалось загрузить прокси, пул остаётся прежним.")
            return
        
        working = []
        total = len(all_proxies)
        for i, proxy in enumerate(all_proxies):
            if i % 100 == 0:
                logging.info(f"Проверка прокси: {i}/{total}")
            if quick_check_proxy(proxy):
                working.append(proxy)
        
        with self.lock:
            self.working_proxies = working
        logging.info(f"Пул обновлён: {len(working)} рабочих прокси из {total}")

    def get_proxy(self):
        """Возвращает случайный рабочий прокси или None, если пул пуст."""
        with self.lock:
            if not self.working_proxies:
                return None
            proxy = random.choice(self.working_proxies)
            return {"http": proxy, "https": proxy}, proxy

    def report_failure(self, proxy_url):
        """Удаляет прокси из пула при ошибке."""
        with self.lock:
            if proxy_url in self.working_proxies:
                self.working_proxies.remove(proxy_url)
                logging.warning(f"Прокси {mask_proxy(proxy_url)} удалён из пула. Осталось: {len(self.working_proxies)}")

def mask_proxy(proxy_url):
    if not proxy_url:
        return "None"
    # маскируем пароль, если есть
    return re.sub(r':([^:]+)@', ':****@', proxy_url)

# Глобальный пул
proxy_pool = SmartProxyPool()

# ==================== ОСТАЛЬНЫЕ ФУНКЦИИ БОТА ====================
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

def fetch_ebay_html():
    """Загружает eBay через случайный рабочий прокси (с повторными попытками)."""
    for attempt in range(3):
        proxy_dict, proxy_url = proxy_pool.get_proxy()
        if proxy_dict is None:
            logging.error("Нет рабочих прокси, бот не может продолжить.")
            return None
        
        logging.info(f"Попытка {attempt+1}/3 через {mask_proxy(proxy_url)}")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Referer': 'https://www.ebay.com/',
            'Sec-Ch-Ua': '"Google Chrome";v="124", "Chromium";v="124", "Not-A.Brand";v="99"',
            'Upgrade-Insecure-Requests': '1',
        }
        time.sleep(random.uniform(1.0, 2.0))
        try:
            response = cffi_requests.get(
                EBAY_SEARCH_URL,
                headers=headers,
                impersonate="chrome124",
                proxies=proxy_dict,
                timeout=30
            )
            if response.status_code == 200:
                logging.info("Страница успешно загружена")
                return response.text
            else:
                logging.warning(f"Ошибка HTTP {response.status_code}")
                proxy_pool.report_failure(proxy_url)
        except Exception as e:
            logging.error(f"Ошибка запроса: {e}")
            proxy_pool.report_failure(proxy_url)
    logging.error("Все попытки не удались")
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
    conn = sqlite3.connect('ebay_tracker.db')
    cursor = conn.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS seen_items (item_id TEXT PRIMARY KEY, first_seen TIMESTAMP)')
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
    logging.info("🔄 Фоновый поток запущен")
    time.sleep(5)
    try:
        init_db_and_snapshot()
        send_telegram_message(f"🚀 Бот запущен на Render.com. Рабочих прокси: {len(proxy_pool.working_proxies)}")
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
        send_telegram_message("❌ Критическая ошибка, бот остановлен")
        sys.exit(1)

@app.route('/')
def index():
    return f"eBay бот работает. Рабочих прокси: {len(proxy_pool.working_proxies)}"

@app.route('/health')
def health():
    return "OK", 200

if __name__ == "__main__":
    logging.info("Запуск процесса...")
    # Ждём, пока пул прокси инициализируется (не дольше 2 минут)
    timeout = 120
    start = time.time()
    while not proxy_pool.working_proxies and (time.time() - start) < timeout:
        logging.info("Ожидание загрузки рабочих прокси...")
        time.sleep(5)
    if proxy_pool.working_proxies:
        send_telegram_message(f"✅ Бот загрузил {len(proxy_pool.working_proxies)} рабочих прокси. Начинаем мониторинг.")
    else:
        send_telegram_message("⚠️ Не найдено ни одного рабочего прокси. Бот не сможет проверять eBay.")
    thread = threading.Thread(target=bot_worker, daemon=False)
    thread.start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
