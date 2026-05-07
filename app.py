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
PROXY_API_URL = os.getenv("PROXY_API_URL", "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text&anonymity=elite%2Canonymous")
# ===============================

if not EBAY_SEARCH_URL or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    logging.error("Не хватает переменных окружения.")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

# ==================== ЗАГРУЗКА СПИСКА ПРОКСИ ====================
def fetch_proxy_list():
    """Загружает список прокси с API (без проверки)."""
    try:
        import requests
        logging.info("Загрузка списка прокси с API...")
        response = requests.get(PROXY_API_URL, timeout=30)
        response.raise_for_status()
        raw_proxies = response.text.split()
        # Нормализуем формат (добавляем http:// если нет схемы)
        proxies = []
        for p in raw_proxies:
            p = p.strip()
            if not p:
                continue
            if not re.match(r'^(http|https|socks4|socks5)://', p):
                p = f"http://{p}"
            proxies.append(p)
        logging.info(f"Загружено {len(proxies)} прокси (без проверки).")
        return proxies
    except Exception as e:
        logging.error(f"Ошибка загрузки прокси: {e}")
        return []

# ==================== УМНЫЙ ПУЛ С ЛЕНИВОЙ ПРОВЕРКОЙ ====================
class LazyProxyPool:
    def __init__(self, api_url):
        self.api_url = api_url
        self.all_proxies = []          # весь список (непроверенные)
        self.working_proxies = []      # проверенные рабочие
        self.current_index = 0
        self.lock = threading.Lock()
        self.load_proxies()
        # Раз в час обновляем список (добавляем свежие)
        self.start_refresh_thread()

    def load_proxies(self):
        """Загружает свежие прокси и добавляет их в конец очереди (не удаляя старые рабочие)."""
        new_list = fetch_proxy_list()
        with self.lock:
            if new_list:
                # Добавляем только те, которых ещё нет в all_proxies
                existing = set(self.all_proxies)
                added = 0
                for p in new_list:
                    if p not in existing:
                        self.all_proxies.append(p)
                        added += 1
                logging.info(f"Добавлено {added} новых прокси из API. Всего в очереди: {len(self.all_proxies)}")
            else:
                logging.warning("Не удалось обновить список прокси.")

    def start_refresh_thread(self):
        def refresh_loop():
            while True:
                time.sleep(3600)  # каждый час
                self.load_proxies()
        thread = threading.Thread(target=refresh_loop, daemon=True)
        thread.start()

    def get_proxy(self):
        """
        Возвращает прокси для запроса.
        Сначала пытается взять случайный из рабочих (проверенных).
        Если рабочих нет — берёт следующий из общей очереди и помечает его как "тестируемый".
        """
        with self.lock:
            # Если есть проверенные рабочие — используем их (случайный)
            if self.working_proxies:
                selected = random.choice(self.working_proxies)
                return {"http": selected, "https": selected}, selected, True
            # Иначе берём следующий из очереди
            if not self.all_proxies:
                return None, None, False
            proxy = self.all_proxies[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.all_proxies)
            return {"http": proxy, "https": proxy}, proxy, False

    def mark_success(self, proxy_url):
        """Прокси успешно отработал — перемещаем в рабочий пул, если его там ещё нет."""
        with self.lock:
            if proxy_url not in self.working_proxies:
                self.working_proxies.append(proxy_url)
                logging.info(f"✅ Прокси {mask_proxy(proxy_url)} добавлен в рабочий пул (всего рабочих: {len(self.working_proxies)})")
            # Также удаляем его из all_proxies, чтобы не повторяться
            if proxy_url in self.all_proxies:
                self.all_proxies.remove(proxy_url)

    def mark_failure(self, proxy_url):
        """Прокси не работает — удаляем из обоих списков."""
        with self.lock:
            if proxy_url in self.working_proxies:
                self.working_proxies.remove(proxy_url)
                logging.warning(f"❌ Прокси {mask_proxy(proxy_url)} удалён из рабочих (осталось {len(self.working_proxies)})")
            if proxy_url in self.all_proxies:
                self.all_proxies.remove(proxy_url)

def mask_proxy(proxy_url):
    return re.sub(r':([^:]+)@', ':****@', proxy_url) if proxy_url else "None"

# Глобальный пул
proxy_pool = LazyProxyPool(PROXY_API_URL)

# ==================== ФУНКЦИИ БОТА ====================
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
    """Загружает eBay через прокси (с автоматическим переключением при ошибке)."""
    for attempt in range(5):
        proxy_dict, proxy_url, is_working = proxy_pool.get_proxy()
        if proxy_dict is None:
            logging.warning("Нет доступных прокси, пробуем без прокси")
            proxy_dict = None
            proxy_url = "direct"
        else:
            logging.info(f"Попытка {attempt+1}/5 через {mask_proxy(proxy_url)} (рабочий: {is_working})")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Referer': 'https://www.ebay.com/',
            'Sec-Ch-Ua': '"Google Chrome";v="124", "Chromium";v="124", "Not-A.Brand";v="99"',
            'Upgrade-Insecure-Requests': '1',
        }
        time.sleep(random.uniform(1.0, 3.0))
        try:
            response = cffi_requests.get(
                EBAY_SEARCH_URL,
                headers=headers,
                impersonate="chrome124",
                proxies=proxy_dict,
                timeout=45
            )
            if response.status_code == 200:
                logging.info("Страница успешно загружена")
                if proxy_url != "direct":
                    proxy_pool.mark_success(proxy_url)
                return response.text
            else:
                logging.warning(f"Ошибка HTTP {response.status_code}")
                if proxy_url != "direct":
                    proxy_pool.mark_failure(proxy_url)
        except Exception as e:
            logging.error(f"Ошибка запроса: {e}")
            if proxy_url != "direct":
                proxy_pool.mark_failure(proxy_url)
    logging.error("Все попытки не удались")
    return None

# --- Остальные функции (парсинг, БД) без изменений ---
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
    time.sleep(10)
    try:
        init_db_and_snapshot()
        send_telegram_message("🚀 Бот запущен на Render.com с ленивой ротацией прокси (без массовой проверки)!")
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
    return "eBay мониторинг бот работает (ленивая ротация)"

@app.route('/health')
def health():
    return "OK", 200

if __name__ == "__main__":
    logging.info("Запуск процесса...")
    total = len(proxy_pool.all_proxies)
    send_telegram_message(f"✅ Бот загрузил {total} прокси. Проверка будет происходить по мере использования.")
    thread = threading.Thread(target=bot_worker, daemon=False)
    thread.start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
