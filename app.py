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
PROXY_API_URL = os.getenv("PROXY_API_URL", "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text&anonymity=elite%2Canonymous")
# ===============================

if not EBAY_SEARCH_URL or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    logging.error("Не хватает переменных окружения. Проверьте .env или настройки Render.")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

# ==================== ФУНКЦИЯ ЗАГРУЗКИ ПРОКСИ ИЗ API ====================
def fetch_proxy_list():
    """
    Загружает список прокси с API и возвращает список строк в формате protocol://ip:port.
    """
    try:
        import requests
        logging.info("Загрузка свежего списка прокси с API...")
        response = requests.get(PROXY_API_URL, timeout=30)
        response.raise_for_status()
        # API возвращает одну строку с прокси, разделёнными пробелами
        proxy_text = response.text
        raw_proxies = [p.strip() for p in proxy_text.split() if p.strip()]
        # Проверяем, что каждый прокси имеет схему. Если нет — добавляем http:// по умолчанию
        validated = []
        for p in raw_proxies:
            if p.startswith(('http://', 'https://', 'socks4://', 'socks5://')):
                validated.append(p)
            else:
                # Если схема отсутствует, предполагаем, что это http
                validated.append(f"http://{p}")
        logging.info(f"Загружено {len(validated)} прокси с API.")
        return validated
    except Exception as e:
        logging.error(f"Ошибка загрузки списка прокси: {e}")
        return []

# ==================== ФУНКЦИЯ КОНВЕРТАЦИИ ПРОКСИ ====================
def normalize_proxy_url(proxy: str) -> str:
    """Приводит прокси к формату, ожидаемому curl_cffi."""
    if not proxy:
        return ""
    # Если уже есть схема и формат правильный
    if re.match(r'^(http|https|socks4|socks5|socks5h)://', proxy, re.I):
        # Убедимся, что нет двоеточий в порту, и при необходимости добавим логин:пароль, если они есть
        # Для простоты считаем, что пришедший прокси уже в формате protocol://ip:port
        return proxy
    # Если начинается с http://, но без логина
    if proxy.startswith("http://"):
        return proxy
    # Убираем возможный http:// в начале
    if proxy.startswith("http://"):
        proxy = proxy[7:]
    parts = proxy.split(':')
    # Формат ip:port:login:password (4 части)
    if len(parts) == 4:
        ip, port, login, password = parts
        return f"http://{login}:{password}@{ip}:{port}"
    # Формат ip:port (без авторизации)
    elif len(parts) == 2:
        return f"http://{ip}:{port}"
    else:
        logging.warning(f"Неизвестный формат прокси: {proxy}")
        return None

# ==================== УПРАВЛЕНИЕ ПРОКСИ-ПУЛОМ ====================
class ProxyManager:
    def __init__(self, api_url):
        self.api_url = api_url
        self.working_proxies = []  # {'url': str, 'fail_count': int}
        self.lock = threading.Lock()
        self._update_pool()

    def _update_pool(self):
        """Обновляет пул прокси: загружает новые и проверяет их."""
        logging.info("Обновление пула прокси...")
        raw_list = fetch_proxy_list()
        if not raw_list:
            logging.warning("Пул прокси пуст. Бот продолжит работу с существующим пулом.")
            return
        # Конвертируем и проверяем новые прокси
        new_working = []
        for p in raw_list:
            norm = normalize_proxy_url(p)
            if norm and self._check_proxy(norm):
                new_working.append({'url': norm, 'fail_count': 0})
                logging.info(f"✅ Новый рабочий прокси: {self._mask(norm)}")
        with self.lock:
            self.working_proxies = new_working
        logging.info(f"Пул обновлён. Всего рабочих прокси: {len(self.working_proxies)}")

    def _check_proxy(self, proxy_url: str) -> bool:
        """Проверяет прокси, делая тестовый запрос к eBay."""
        test_url = "https://www.ebay.com"
        proxies = {"http": proxy_url, "https": proxy_url}
        try:
            response = cffi_requests.get(
                test_url,
                proxies=proxies,
                impersonate="chrome124",
                timeout=15
            )
            return response.status_code == 200
        except Exception:
            return False

    def _mask(self, url: str) -> str:
        """Маскирует пароль в логах."""
        return re.sub(r':([^:]+)@', ':****@', url)

    def get_proxy(self):
        """Возвращает случайный рабочий прокси из пула."""
        with self.lock:
            if not self.working_proxies:
                return None, None
            selected = random.choice(self.working_proxies)
            proxy_url = selected['url']
            return {"http": proxy_url, "https": proxy_url}, selected

    def report_failure(self, proxy_entry):
        """Увеличивает счётчик ошибок, при 3 неудачах удаляет прокси."""
        with self.lock:
            if proxy_entry not in self.working_proxies:
                return
            proxy_entry['fail_count'] += 1
            if proxy_entry['fail_count'] >= 3:
                logging.warning(f"Прокси {self._mask(proxy_entry['url'])} исключён (3 ошибки).")
                self.working_proxies.remove(proxy_entry)
            else:
                logging.warning(f"Прокси {self._mask(proxy_entry['url'])} ошибка, счётчик: {proxy_entry['fail_count']}/3")

# Глобальный менеджер прокси
proxy_manager = ProxyManager(PROXY_API_URL)

# ==================== ОСТАЛЬНЫЕ ФУНКЦИИ БОТА ====================
# ... (функции send_telegram_message, fetch_ebay_html, parse_ebay_listings, 
#      init_db_and_snapshot, get_seen_ids, add_seen_id, check_new_items, bot_worker)
# Обратите внимание, что эти функции должны использовать proxy_manager. 
# Для экономии места я их здесь привожу полностью, но в финальном коде они будут.

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
    """Загружает страницу через случайный прокси из пула (или без прокси)."""
    for attempt in range(3):
        proxy_dict, proxy_entry = proxy_manager.get_proxy()
        if proxy_dict is None:
            logging.warning("Прокси нет, пробуем прямой запрос")
            # В реальности прямой запрос с IP Render может не работать, но оставим как запасной вариант
        else:
            logging.info(f"Попытка {attempt+1}/3 через {proxy_manager._mask(proxy_entry['url'])}")
        # Заголовки (можно упростить)
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
        }
        time.sleep(random.uniform(2.0, 5.0))
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
                return response.text
            else:
                logging.warning(f"Статус {response.status_code}")
                if proxy_entry:
                    proxy_manager.report_failure(proxy_entry)
        except Exception as e:
            logging.error(f"Ошибка запроса: {e}")
            if proxy_entry:
                proxy_manager.report_failure(proxy_entry)
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
    logging.info(f"Начальный снимок: добавлено {count} товаров (не будут отправлены)")

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
        send_telegram_message("🚀 Бот запущен на Render.com с динамическим пулом бесплатных прокси!")
        logging.info("Переход в основной цикл")
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
        logging.error(f"Критическая ошибка в bot_worker: {e}", exc_info=True)
        send_telegram_message("❌ Критическая ошибка, бот остановлен")
        sys.exit(1)

@app.route('/')
def index():
    return "eBay мониторинг бот работает"

@app.route('/health')
def health():
    return "OK", 200

if __name__ == "__main__":
    logging.info("Запуск процесса...")
    # Отправим приветственное сообщение с количеством прокси в пуле
    if proxy_manager.working_proxies:
        send_telegram_message(f"✅ Бот использует {len(proxy_manager.working_proxies)} рабочих прокси из бесплатного API.")
    else:
        logging.warning("Нет рабочих прокси, бот будет работать без них (риск 403)")
        send_telegram_message("⚠️ Внимание: нет рабочих прокси. Возможны блокировки eBay.")
    thread = threading.Thread(target=bot_worker, daemon=False)
    thread.start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
