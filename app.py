import os
import sys
import ssl
import time
import random
import re
import json
import threading
import logging
import html as html_lib
import hashlib
import socket
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import requests
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode, quote
from bs4 import BeautifulSoup, Tag
from flask import Flask
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

# SSL-проверку глобально не отключаем. Ненадёжные MITM-прокси лучше исключать.

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
# Успешные проверки: нижняя граница берётся из Render.
# При CHECK_INTERVAL=15 фактическая пауза будет 15-27 сек.
# Ниже 15 сек. код не позволяет опускаться: это новый безопасный нижний предел,
# а случайный jitter +12 сек. сохраняет непостоянный ритм запросов к eBay.
CHECK_INTERVAL = max(15, int(os.getenv("CHECK_INTERVAL", "40")))
DATABASE_URL = os.getenv("DATABASE_URL")
PROXY_LIST_URL = os.getenv("PROXY_LIST")
# ProxyScrape обновляет бесплатный список примерно раз в минуту.
PROXY_REFRESH_INTERVAL = 60

# PostgreSQL/Aiven: короткие сетевые/SQL timeout защищают worker-ы от долгого зависания
# при временном сетевом сбое. Никаких ручных изменений в Aiven не требуется.
DB_CONNECT_TIMEOUT = max(3, int(os.getenv("DB_CONNECT_TIMEOUT", "5")))
DB_STATEMENT_TIMEOUT_MS = max(5000, int(os.getenv("DB_STATEMENT_TIMEOUT_MS", "10000")))
DB_LOCK_TIMEOUT_MS = max(1000, int(os.getenv("DB_LOCK_TIMEOUT_MS", "3000")))

# Render делает zero-downtime deploy: новая и старая копия некоторое время живут одновременно.
# Один session-level advisory lock в PostgreSQL гарантирует, что фоновые worker-ы активны
# только в ОДНОЙ копии приложения. Ключ стабильный и не требует таблицы/настроек в Aiven.
LEADER_LOCK_KEY = int.from_bytes(
    hashlib.sha256(b"ebay-uk-telegram-bot-single-leader-v6").digest()[:8],
    byteorder="big", signed=True,
)
LEADER_RETRY_INTERVAL = max(2, int(os.getenv("LEADER_RETRY_INTERVAL", "5")))
LEADER_HEALTH_INTERVAL = max(5, int(os.getenv("LEADER_HEALTH_INTERVAL", "10")))

# Discovery запускается только когда уже нет рабочей fixed-session.
# Нормальный аварийный поиск остаётся консервативным: 4 уникальных IP сразу -> 5 после 10 сек.
# Только если outage затянулся и уже включён расширенный emergency pool, допускаем максимум 6.
# При живом fixed proxy по-прежнему выполняется ОДИН запрос — обычная нагрузка не меняется.
PROBE_CONCURRENCY = max(4, min(int(os.getenv("PROBE_CONCURRENCY", "4")), 5))
PROBE_ESCALATED_CONCURRENCY = max(
    PROBE_CONCURRENCY,
    min(int(os.getenv("PROBE_ESCALATED_CONCURRENCY", "5")), 5),
)
PROBE_ESCALATE_AFTER = max(5.0, float(os.getenv("PROBE_ESCALATE_AFTER", "10")))
PROBE_DEEP_CONCURRENCY = max(
    PROBE_ESCALATED_CONCURRENCY,
    min(int(os.getenv("PROBE_DEEP_CONCURRENCY", "6")), 6),
)
PROBE_DEEP_ESCALATE_AFTER = max(
    PROBE_ESCALATE_AFTER,
    float(os.getenv("PROBE_DEEP_ESCALATE_AFTER", "35")),
)
PROBE_CONNECT_TIMEOUT = float(os.getenv("PROBE_CONNECT_TIMEOUT", "3.5"))
# В старой версии после 45 сек. timeout искусственно увеличивался до 4.5/12 и один
# полуживой proxy мог держать целый batch 11+ секунд. При сотнях кандидатов выгоднее
# продолжать быстро перебирать пул тем же строгим timeout.
PROBE_READ_TIMEOUT = float(os.getenv("PROBE_READ_TIMEOUT", "8"))
# Replacement jitter зависит от причины отказа: transport/SSL/reject уже сами дали
# достаточную задержку, а после 403/429 оставляем более осторожный ритм.
PROBE_FAST_PAUSE_MIN = max(0.02, float(os.getenv("PROBE_FAST_PAUSE_MIN", "0.05")))
PROBE_FAST_PAUSE_MAX = max(PROBE_FAST_PAUSE_MIN, float(os.getenv("PROBE_FAST_PAUSE_MAX", "0.15")))
PROBE_BLOCK_PAUSE_MIN = max(0.15, float(os.getenv("PROBE_BLOCK_PAUSE_MIN", "0.30")))
PROBE_BLOCK_PAUSE_MAX = max(PROBE_BLOCK_PAUSE_MIN, float(os.getenv("PROBE_BLOCK_PAUSE_MAX", "0.60")))
PROBE_OTHER_PAUSE_MIN = max(0.05, float(os.getenv("PROBE_OTHER_PAUSE_MIN", "0.12")))
PROBE_OTHER_PAUSE_MAX = max(PROBE_OTHER_PAUSE_MIN, float(os.getenv("PROBE_OTHER_PAUSE_MAX", "0.28")))

# Основной ProxyScrape URL остаётся ровно тем, что задан в Render (обычно timeout=1500).
# Первый emergency tier по-прежнему 3000 мс. По новым логам он уже почти удваивает pool,
# поэтому НЕ заменяем его на 4000 постоянно. Только если 3000-пул тоже не дал результата,
# один раз подключаем deep-emergency 4000 мс как последний дополнительный источник.
# Никаких новых обязательных Environment Variables пользователю не нужно.
PROXY_EMERGENCY_TIMEOUT_MS = max(1500, int(os.getenv("PROXY_EMERGENCY_TIMEOUT_MS", "3000")))
PROXY_EMERGENCY_TRIGGER_AFTER = max(15.0, float(os.getenv("PROXY_EMERGENCY_TRIGGER_AFTER", "30")))
PROXY_EMERGENCY_TRIGGER_ATTEMPTS = max(20, int(os.getenv("PROXY_EMERGENCY_TRIGGER_ATTEMPTS", "50")))
PROXY_EMERGENCY_MIN_AVAILABLE = max(20, int(os.getenv("PROXY_EMERGENCY_MIN_AVAILABLE", "60")))
PROXY_EMERGENCY_MIN_RATIO = min(0.50, max(0.05, float(os.getenv("PROXY_EMERGENCY_MIN_RATIO", "0.20"))))
PROXY_EMERGENCY_TRANSIENT_REPROBES = max(0, min(int(os.getenv("PROXY_EMERGENCY_TRANSIENT_REPROBES", "0")), 3))
PROXY_EMERGENCY_TRANSIENT_MIN_AGE = max(30.0, float(os.getenv("PROXY_EMERGENCY_TRANSIENT_MIN_AGE", "45")))
FINAL_KNOWN_GOOD_REPROBES = max(0, min(int(os.getenv("FINAL_KNOWN_GOOD_REPROBES", "1")), 1))
PROXY_DEEP_EMERGENCY_TIMEOUT_MS = max(
    PROXY_EMERGENCY_TIMEOUT_MS,
    int(os.getenv("PROXY_DEEP_EMERGENCY_TIMEOUT_MS", "4000")),
)
PROXY_DEEP_EMERGENCY_TRIGGER_AFTER = max(
    PROXY_EMERGENCY_TRIGGER_AFTER,
    float(os.getenv("PROXY_DEEP_EMERGENCY_TRIGGER_AFTER", "50")),
)
PROXY_DEEP_EMERGENCY_TRIGGER_ATTEMPTS = max(
    PROXY_EMERGENCY_TRIGGER_ATTEMPTS,
    int(os.getenv("PROXY_DEEP_EMERGENCY_TRIGGER_ATTEMPTS", "110")),
)
# Во время длинного discovery один раз мягко обновляем/МЕРДЖИМ быстрый 1500-ms список,
# не выбрасывая emergency-кандидатов. Это позволяет поймать новый рабочий endpoint,
# появившийся в ProxyScrape уже после начала 75-секундного цикла.
PROXY_STANDARD_MID_REFRESH_AFTER = max(20.0, float(os.getenv("PROXY_STANDARD_MID_REFRESH_AFTER", "35")))
PROXY_STANDARD_MID_REFRESH_ATTEMPTS = max(30, int(os.getenv("PROXY_STANDARD_MID_REFRESH_ATTEMPTS", "60")))

# Если eBay UK не дал ни одного успешного ответа более 10 минут, один раз
# уведомляем в Telegram. После следующего успеха аварийный флаг сбрасывается.
CONNECTION_ALERT_AFTER = max(60, int(os.getenv("CONNECTION_ALERT_AFTER", "600")))
CONNECTION_WATCHDOG_INTERVAL = max(10, int(os.getenv("CONNECTION_WATCHDOG_INTERVAL", "15")))

# ============ НАПОМИНАНИЯ ОБ АУКЦИОНАХ ============
# Храним расписание в PostgreSQL (Aiven), поэтому рестарт/deploy Render не стирает его.
# Планировщик не опрашивает БД каждые несколько секунд: он спит до ближайшего
# события (не дольше минуты) и мгновенно просыпается при добавлении новой ссылки.
AUCTION_REMINDER_MINUTES = (60, 30, 10, 5)
AUCTION_SCHEDULER_MAX_SLEEP = max(15, int(os.getenv("AUCTION_SCHEDULER_MAX_SLEEP", "60")))
AUCTION_FETCH_MAX_PROXIES = max(3, min(int(os.getenv("AUCTION_FETCH_MAX_PROXIES", "8")), 8))
AUCTION_FETCH_PARALLEL = max(2, min(int(os.getenv("AUCTION_FETCH_PARALLEL", "3")), 4))
AUCTION_FETCH_CONNECT_TIMEOUT = float(os.getenv("AUCTION_FETCH_CONNECT_TIMEOUT", "5"))
AUCTION_FETCH_READ_TIMEOUT = float(os.getenv("AUCTION_FETCH_READ_TIMEOUT", "16"))
# V6.17: пользовательская auction-ссылка сначала ждёт короткое безопасное окно и
# использует ИМЕННО уже открытую fixed Session основного монитора. Это важнее, чем
# просто повторно открыть тот же proxy новым CONNECT: по реальным логам новый CONNECT
# мог падать/получать 403, пока существующая fixed Session продолжала давать HTTP 200.
# Lock по-прежнему сериализует доступ, поэтому одна Session никогда не используется
# двумя потоками одновременно. Новых обязательных ENV нет.
AUCTION_MAIN_PROXY_WAIT = max(0.0, min(float(os.getenv("AUCTION_MAIN_PROXY_WAIT", "12")), 20.0))
# Отдельная короткая память только для auction-fetch. Она НЕ штрафует основной monitor.
# Нужна, чтобы queue не выбирала на каждом retry одни и те же reserve IP, которые уже
# доказанно дали 403/timeout/challenge именно на auction/search URL.
AUCTION_PROXY_GOOD_MEMORY = max(60, int(os.getenv("AUCTION_PROXY_GOOD_MEMORY", "900")))
# Для подтверждения пользовательской ссылки пробуем несколько независимых HTML-вариантов:
# eBay иногда отдаёт одному proxy урезанный/иной шаблон без itemEndDate.
AUCTION_VERIFY_MAX_PAGES = max(2, min(int(os.getenv("AUCTION_VERIFY_MAX_PAGES", "4")), 4))
# Лёгкая проверка сохранённых аукционов: максимум два лота за проход. Это позволяет
# заметить досрочное завершение/изменение end time, не создавая burst на eBay/Render.
AUCTION_STATUS_BATCH = max(1, min(int(os.getenv("AUCTION_STATUS_BATCH", "1")), 2))
AUCTION_STATUS_TICK = max(30, int(os.getenv("AUCTION_STATUS_TICK", "60")))
AUCTION_STATUS_CONNECT_TIMEOUT = float(os.getenv("AUCTION_STATUS_CONNECT_TIMEOUT", "4"))
AUCTION_STATUS_READ_TIMEOUT = float(os.getenv("AUCTION_STATUS_READ_TIMEOUT", "8"))
# Scheduler обычно просыпается не реже раза в минуту. Если Render был перезапущен и
# точный порог уже прошёл, старые 60/30/10-минутные сообщения не отправляем пачкой:
# посылаем только одно наиболее актуальное напоминание.
AUCTION_REMINDER_LATE_GRACE = max(30, int(os.getenv("AUCTION_REMINDER_LATE_GRACE", "90")))
# Не удаляем запись мгновенно в сохранённую секунду окончания: eBay официально
# тестирует extended bidding в некоторых категориях. Grace даёт время увидеть
# продление, даже если Render/proxy временно недоступны возле самого конца.
AUCTION_END_GRACE = max(300, int(os.getenv("AUCTION_END_GRACE", "1800")))
# Если eBay пока показывает только грубый countdown (например 4d 14h), не угадываем
# минуту. Сохраняем лот как pending и возвращаемся к нему, когда до даже самого
# позднего возможного конца останется меньше ~23.5 ч. Если минутная точность всё ещё
# скрыта, повторяем редко (по умолчанию раз в 10 минут), без Bid History/Sign-In.
AUCTION_PENDING_REFINE_TARGET = max(20 * 3600, int(os.getenv("AUCTION_PENDING_REFINE_TARGET", str(23 * 3600 + 30 * 60))))
AUCTION_PENDING_RETRY = max(300, int(os.getenv("AUCTION_PENDING_RETRY", "600")))
# Когда до конца <=2 часов и minute-countdown уже виден, не ждём 10 минут после
# временной сетевой ошибки: повторяем lightweight search примерно через 2 минуты.
AUCTION_PENDING_CLOSE_RETRY = max(60, min(int(os.getenv("AUCTION_PENDING_CLOSE_RETRY", "120")), 300))
AUCTION_PENDING_MIN_DELAY = max(60, int(os.getenv("AUCTION_PENDING_MIN_DELAY", "300")))
AUCTION_PENDING_WINDOW_MARGIN = max(30, int(os.getenv("AUCTION_PENDING_WINDOW_MARGIN", "120")))
AUCTION_PENDING_MINUTE_MARGIN = max(5, int(os.getenv("AUCTION_PENDING_MINUTE_MARGIN", "15")))

# Пользовательские auction-ссылки храним в PostgreSQL до успешной проверки.
# Если прямо сейчас нет рабочего proxy, ссылка НЕ отклоняется и НЕ теряется при deploy/restart:
# worker откладывает её с мягким backoff и параллельно может обработать следующие ссылки.
AUCTION_LINK_RETRY_BASE = max(10, int(os.getenv("AUCTION_LINK_RETRY_BASE", "15")))
AUCTION_LINK_RETRY_MAX = max(AUCTION_LINK_RETRY_BASE, int(os.getenv("AUCTION_LINK_RETRY_MAX", "60")))
# Очередь хранится в PostgreSQL, а enqueue всегда будит worker через Event.
# Поэтому нет смысла открывать новый TLS-сеанс к Aiven каждые 5 секунд, когда очередь пуста.
# 60 сек. — только страховочный max-poll; новая ссылка всё равно будит worker мгновенно,
# а запланированный retry ждёт ровно до next_attempt (с cap этим значением).
AUCTION_LINK_WORKER_IDLE = max(30, int(os.getenv("AUCTION_LINK_WORKER_IDLE", "60")))
AUCTION_LINK_DB_ERROR_WAIT = max(10, int(os.getenv("AUCTION_LINK_DB_ERROR_WAIT", "15")))

KYIV_TZ = ZoneInfo("Europe/Kyiv")
LONDON_TZ = ZoneInfo("Europe/London")

# Для уже найденной рабочей пары таймауты мягче: её не надо выбрасывать
# только потому, что один ответ оказался чуть медленнее.
FIXED_CONNECT_TIMEOUT = float(os.getenv("FIXED_CONNECT_TIMEOUT", "6"))
FIXED_READ_TIMEOUT = float(os.getenv("FIXED_READ_TIMEOUT", "18"))
# Если уже хороший proxy сорвался, даём ему ОДИН короткий шанс с новой Session.
# Максимум около 12 сек., затем сразу начинаем discovery по другим proxy.
FIXED_RECOVERY_CONNECT_TIMEOUT = float(os.getenv("FIXED_RECOVERY_CONNECT_TIMEOUT", "4"))
FIXED_RECOVERY_READ_TIMEOUT = float(os.getenv("FIXED_RECOVERY_READ_TIMEOUT", "8"))

# Один аварийный discovery-цикл не должен держать монитор без свежего pool слишком долго.
# За 75 сек. с adaptive 4->5 workers успеваем проверить значительно больше адресов,
# затем делаем лишь короткую паузу, обновляем ProxyScrape и продолжаем поиск.
SEARCH_TIME_BUDGET = max(45, int(os.getenv("SEARCH_TIME_BUDGET", "75")))
MAX_SEARCH_ATTEMPTS = max(60, int(os.getenv("MAX_SEARCH_ATTEMPTS", "180")))
FAILED_SEARCH_RETRY_MIN = max(2.0, float(os.getenv("FAILED_SEARCH_RETRY_MIN", "3")))
FAILED_SEARCH_RETRY_MAX = max(FAILED_SEARCH_RETRY_MIN, float(os.getenv("FAILED_SEARCH_RETRY_MAX", "5")))

# V6.19 SMART RESERVE.
# Уровень 1: очень дешёвый TCP-check самого proxy endpoint.
# Уровень 2: в фоне, ТОЛЬКО пока fixed proxy работает, проверяем реальную способность proxy
# провести HTTPS-туннель + валидный TLS к нейтральному example.com. HTTP-запрос к eBay
# при этом НЕ отправляется. Так отсеиваются CONNECT 400/500/503, SOCKS-ошибки и SSL/MITM,
# которые обычный TCP-open ошибочно считает "живыми".
PROXY_PREFLIGHT_ENABLED = (os.getenv("PROXY_PREFLIGHT_ENABLED", "true").strip().lower()
                           not in ("0", "false", "no", "off"))
PROXY_PREFLIGHT_CONNECT_TIMEOUT = max(
    0.5, min(float(os.getenv("PROXY_PREFLIGHT_CONNECT_TIMEOUT", "1.2")), 2.0)
)
# TCP-сигнал держим недолго: бесплатный proxy может умереть через минуту.
PROXY_PREFLIGHT_OK_TTL = max(45, int(os.getenv("PROXY_PREFLIGHT_OK_TTL", "90")))
PROXY_PREFLIGHT_BAD_TTL = max(15, int(os.getenv("PROXY_PREFLIGHT_BAD_TTL", "30")))

# Более строгая фоновая проверка HTTPS capability. example.com выбран как маленький,
# стандартный и нейтральный TLS endpoint; после handshake соединение сразу закрывается.
PROXY_QUALITY_PREFLIGHT_ENABLED = (
    os.getenv("PROXY_QUALITY_PREFLIGHT_ENABLED", "true").strip().lower()
    not in ("0", "false", "no", "off")
)
PROXY_QUALITY_HOST = (os.getenv("PROXY_QUALITY_HOST", "example.com").strip() or "example.com")
PROXY_QUALITY_PORT = max(1, min(int(os.getenv("PROXY_QUALITY_PORT", "443")), 65535))
PROXY_QUALITY_TIMEOUT = max(1.2, min(float(os.getenv("PROXY_QUALITY_TIMEOUT", "3.0")), 4.0))
PROXY_QUALITY_OK_TTL = max(60, int(os.getenv("PROXY_QUALITY_OK_TTL", "120")))
PROXY_QUALITY_BAD_TTL = max(20, int(os.getenv("PROXY_QUALITY_BAD_TTL", "45")))

# Не пытаемся "прогреть весь интернет". Цель — постоянно иметь 12-16 СВЕЖИХ
# HTTPS-capable резервов. 16 кандидатов за проход = 2x прежних 8, но после достижения
# target worker переключается на маленький maintenance batch.
PROXY_PREFLIGHT_WARM_BATCH = max(4, min(int(os.getenv("PROXY_PREFLIGHT_WARM_BATCH", "16")), 24))
PROXY_PREFLIGHT_WARM_CONCURRENCY = max(
    2, min(int(os.getenv("PROXY_PREFLIGHT_WARM_CONCURRENCY", "6")), 8)
)
PROXY_PREFLIGHT_WARM_INTERVAL = max(10.0, float(os.getenv("PROXY_PREFLIGHT_WARM_INTERVAL", "15")))
PROXY_PREFLIGHT_RESERVE_TARGET = max(
    4, min(int(os.getenv("PROXY_PREFLIGHT_RESERVE_TARGET", "12")), 24)
)
PROXY_PREFLIGHT_MAINTENANCE_BATCH = max(
    2, min(int(os.getenv("PROXY_PREFLIGHT_MAINTENANCE_BATCH", "4")), 8)
)
PROXY_PREFLIGHT_RETAIN_MAX = max(
    PROXY_PREFLIGHT_RESERVE_TARGET,
    min(int(os.getenv("PROXY_PREFLIGHT_RETAIN_MAX", "48")), 96),
)

# Если первый fixed request уже висел очень долго, второй recovery только откладывает failover.
# Быстрый reset/короткий connect-timeout по-прежнему получает один шанс с новой Session.
FIXED_RECOVERY_SKIP_AFTER = max(
    8.0, float(os.getenv("FIXED_RECOVERY_SKIP_AFTER", "18"))
)


# ============ V6.20 MULTI-PROVIDER ============
# Secrets are read ONLY from Render Environment Variables. Never hard-code API keys.
WEBSHARE_API_KEY = (os.getenv("WEBSHARE_API_KEY") or "").strip()
PROXYSCRAPE_PREMIUM_API_KEY = (os.getenv("PROXYSCRAPE_PREMIUM_API_KEY") or "").strip()
PROXYSCRAPE_PREMIUM_SUBACCOUNT_ID = (os.getenv("PROXYSCRAPE_PREMIUM_SUBACCOUNT_ID") or "").strip()
PROVIDER_API_TIMEOUT = max(3.0, min(float(os.getenv("PROVIDER_API_TIMEOUT", "10")), 20.0))
PROXYSCRAPE_PREMIUM_REFRESH = max(30, int(os.getenv("PROXYSCRAPE_PREMIUM_REFRESH", "60")))
WEBSHARE_REFRESH = max(60, int(os.getenv("WEBSHARE_REFRESH", "300")))
# Webshare is metered. Premium/free get the first chance; Webshare is unlocked as rescue.
WEBSHARE_UNLOCK_AFTER = max(3.0, float(os.getenv("WEBSHARE_UNLOCK_AFTER", "12")))
WEBSHARE_UNLOCK_ATTEMPTS = max(4, int(os.getenv("WEBSHARE_UNLOCK_ATTEMPTS", "20")))
WEBSHARE_HARD_RESCUE_AFTER = max(WEBSHARE_UNLOCK_AFTER, float(os.getenv("WEBSHARE_HARD_RESCUE_AFTER", "45")))
# Approximate process-local warning threshold only. It never breaks an already working fixed session.
WEBSHARE_ESTIMATED_MB_WARN = max(10.0, float(os.getenv("WEBSHARE_ESTIMATED_MB_WARN", "250")))
PROVIDER_STATS_INTERVAL = max(60, int(os.getenv("PROVIDER_STATS_INTERVAL", "300")))

# Память outage живёт между соседними 75-секундными discovery. Ранее проверенные неизвестные
# IP не исчезают из пула, но новые IP идут раньше. Known-good всё ещё может получить controlled retry.
OUTAGE_HOST_MEMORY = max(90, int(os.getenv("OUTAGE_HOST_MEMORY", "240")))
# 403/challenge на auction URL не должен жёстко банить основной monitor, но на короткое время
# понижает приоритет того же exit IP для другой eBay-поверхности.
EBAY_CROSS_SOFT_PENALTY = max(30, int(os.getenv("EBAY_CROSS_SOFT_PENALTY", "120")))

# Успешные proxy запоминаем и относим к ним мягче после единичного сбоя.
GOOD_PROXY_MEMORY = 60 * 60
SSL_PROXY_COOLDOWN = 60 * 60

# Страница eBay запрашивается с _ipg=60; обрабатываем все 60 результатов первой страницы.
# Это снижает риск пропустить товар при всплеске новых объявлений между проверками.
MAX_ITEMS = 60
RETRY_DELAY = 2
GBP_TO_UAH = 60
EXTRA_DELIVERY_COST = 120


def build_proxy_list_url_with_timeout(raw_url, timeout_ms, override_env=None):
    """Строит ProxyScrape URL, меняя только query-параметр timeout.

    Основной PROXY_LIST из Render никогда не изменяется. Override необязателен и нужен
    только на будущее; при его отсутствии URL 3000/4000 мс создаются автоматически.
    """
    if override_env:
        override = (os.getenv(override_env) or '').strip()
        if override:
            return override
    if not raw_url:
        return raw_url
    try:
        parts = urlsplit(raw_url)
        pairs = parse_qsl(parts.query, keep_blank_values=True)
        replaced = False
        new_pairs = []
        for k, v in pairs:
            if k.lower() == 'timeout':
                new_pairs.append((k, str(int(timeout_ms))))
                replaced = True
            else:
                new_pairs.append((k, v))
        if not replaced:
            new_pairs.append(('timeout', str(int(timeout_ms))))
        return urlunsplit((
            parts.scheme,
            parts.netloc,
            parts.path,
            urlencode(new_pairs, doseq=True),
            parts.fragment,
        ))
    except Exception as e:
        logging.warning(f"Не удалось построить ProxyScrape URL timeout={timeout_ms}: {e}")
        return raw_url


PROXY_LIST_EMERGENCY_URL = build_proxy_list_url_with_timeout(
    PROXY_LIST_URL, PROXY_EMERGENCY_TIMEOUT_MS, 'PROXY_LIST_EMERGENCY'
)
PROXY_LIST_DEEP_EMERGENCY_URL = build_proxy_list_url_with_timeout(
    PROXY_LIST_URL, PROXY_DEEP_EMERGENCY_TIMEOUT_MS, 'PROXY_LIST_DEEP_EMERGENCY'
)


def normalize_ebay_search_url(raw_url):
    """
    Убираем конфликтующие/дублированные параметры из URL Render.
    Для ebay.co.uk: LH_PrefLoc=1 = UK Only.
    _ipg=60 достаточно: код анализирует всю первую страницу (до MAX_ITEMS=60),
    зато ответ заметно меньше и устойчивее через proxy.
    """
    if not raw_url:
        return raw_url

    parts = urlsplit(raw_url)
    pairs = parse_qsl(parts.query, keep_blank_values=True)

    # Сохраняем исходные параметры, но эти три задаём ровно по одному разу.
    replace_keys = {'LH_PrefLoc', '_ipg', '_sop'}
    cleaned = [(k, v) for k, v in pairs if k not in replace_keys]
    cleaned.extend([
        ('LH_PrefLoc', '1'),   # UK Only на ebay.co.uk
        ('_ipg', '60'),        # вместо 240: меньше HTML и меньше обрывов proxy
        ('_sop', '10'),        # Newly listed
    ])

    return urlunsplit((
        parts.scheme,
        parts.netloc,
        parts.path,
        urlencode(cleaned, doseq=True),
        parts.fragment,
    ))


EBAY_SEARCH_URL = normalize_ebay_search_url(EBAY_SEARCH_URL)

if not all([EBAY_SEARCH_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DATABASE_URL]):
    logging.error("Не хватает переменных окружения.")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
app = Flask(__name__)

is_paused = False

# Telegram listener сохраняет auction-ссылку в PostgreSQL и сразу возвращается к getUpdates.
# Тяжёлое получение страницы eBay выполняется отдельным ОДНИМ worker'ом. Если proxy
# временно нет, job остаётся в БД и переживает deploy/restart, а следующие ссылки не блокируются.
auction_link_wakeup_event = threading.Event()
auction_reminder_wakeup_event = threading.Event()
auction_status_wakeup_event = threading.Event()
# Новый fixed proxy будит Smart Reserve немедленно, а не ждёт до 15 сек. polling interval.
proxy_preflight_wakeup_event = threading.Event()
db_ready_event = threading.Event()
leader_active_event = threading.Event()

# Счётчик seen_items загружается из PostgreSQL один раз при старте leader-копии,
# а затем увеличивается только на реально вставленное число новых item_id.
# Так в логах снова видно общее количество товаров без SELECT COUNT(*) каждые 15-27 сек.
seen_count_lock = threading.Lock()
seen_count_cache = None

def wake_auction_workers():
    # Раздельные Event исключают редкую гонку, когда один worker очищает общий сигнал,
    # предназначенный другому.
    auction_reminder_wakeup_event.set()
    auction_status_wakeup_event.set()

# ============ КОНТРОЛЬ ДОСТУПНОСТИ EBAY ============
# Используем monotonic(): системные часы Render могут корректироваться, а интервал
# "10 минут без успешного подключения" должен оставаться точным.
connection_state_lock = threading.Lock()
connection_watch_started_at = time.monotonic()
last_ebay_success_at = None
connection_alert_sent = False


def record_ebay_success():
    """Фиксирует успешный HTTP 200 с валидной выдачей и сбрасывает outage alert."""
    global last_ebay_success_at, connection_alert_sent
    now = time.monotonic()
    with connection_state_lock:
        first_success = last_ebay_success_at is None
        was_alerted = connection_alert_sent
        last_ebay_success_at = now
        connection_alert_sent = False
    # После deploy/долгого outage не ждём до 60 сек. status-tick: если в PostgreSQL
    # уже есть due pending-аукцион, worker сразу получит шанс его уточнить. На обычных
    # успешных циклах event не ставится, поэтому лишнего DB polling нет.
    if first_success or was_alerted:
        auction_status_wakeup_event.set()
    if was_alerted:
        logging.info("✅ Связь с eBay восстановлена; 10-минутный alert снова разрешён для будущего сбоя")


def reset_connection_watch_after_manual_resume():
    """После /start даём новые 10 минут, чтобы ручная пауза не считалась аварией."""
    global connection_watch_started_at, last_ebay_success_at, connection_alert_sent
    with connection_state_lock:
        connection_watch_started_at = time.monotonic()
        last_ebay_success_at = None
        connection_alert_sent = False


def _connection_outage_snapshot():
    now = time.monotonic()
    with connection_state_lock:
        reference = last_ebay_success_at if last_ebay_success_at is not None else connection_watch_started_at
        elapsed = max(0.0, now - reference)
        return elapsed, connection_alert_sent, (last_ebay_success_at is not None)


# ============ ПРОФИЛИ БРАУЗЕРОВ ============
# По свежему UK-логу именно Chrome150 дал HTTP 200 на IP, где Firefox147
# перед этим получил 403. Поэтому Chrome150 используем как основной профиль.
# Chrome146 — только технический fallback, если текущая сборка curl_cffi
# вдруг не поддерживает Chrome150. На 403 профиль НЕ ротируем.
BROWSER_PROFILES = [
    {
        'name': 'Chrome150_Native',
        'impersonate': 'chrome150',
        'disabled': False,
    },
    {
        'name': 'Chrome146_Native',
        'impersonate': 'chrome146',
        'disabled': False,
    },
]


def get_preferred_profile():
    for preferred in ('Chrome150_Native', 'Chrome146_Native'):
        for p in BROWSER_PROFILES:
            if p['name'] == preferred and not p.get('disabled', False):
                return p
    return None


def disable_profile(profile_name):
    for p in BROWSER_PROFILES:
        if p['name'] == profile_name:
            p['disabled'] = True
            logging.warning(f"Профиль {profile_name} отключён (не поддерживается curl_cffi)")
            break


# ============ МЕНЕДЖЕР ПРОКСИ ============
def _proxy_host(proxy):
    """Возвращает IP/hostname без порта, чтобы не проверять один exit-IP много раз."""
    if not proxy:
        return ""
    try:
        return (urlsplit(proxy).hostname or proxy).lower()
    except Exception:
        return proxy.lower()


def _proxy_scheme(proxy):
    try:
        return (urlsplit(proxy).scheme or 'http').lower()
    except Exception:
        return 'http'



def _proxy_log_name(proxy):
    """Safe proxy label for logs: NEVER prints username/password/API secrets."""
    if not proxy:
        return '<none>'
    try:
        parts = urlsplit(proxy)
        host = parts.hostname or '?'
        port = f":{parts.port}" if parts.port else ''
        scheme = parts.scheme or 'http'
        source = provider_manager.source_fast(proxy) if 'provider_manager' in globals() else None
        suffix = f" [{source}]" if source else ''
        return f"{scheme}://{host}{port}{suffix}"
    except Exception:
        return '<proxy>'


class ProviderManager:
    """Small fail-open source layer for managed proxies.

    Provider API failures never affect the legacy free ProxyScrape path.
    Webshare is intentionally metered/rescue-only; ProxyScrape Premium is priority-1.
    """
    def __init__(self):
        self.lock = threading.Lock()
        self.provider_by_proxy = {}
        self.proxy_sets = {'proxyscrape_premium': set(), 'webshare': set()}
        self.last_refresh = {'proxyscrape_premium': 0.0, 'webshare': 0.0}
        self.ps_subaccount_id = PROXYSCRAPE_PREMIUM_SUBACCOUNT_ID
        self.stats = {
            name: {'probe': 0, 'success': 0, 'blocked': 0, 'rate_limited': 0,
                   'proxy_timeout': 0, 'proxy_error': 0, 'proxy_rejected': 0,
                   'proxy_ssl': 0, 'http_error': 0, 'bytes': 0}
            for name in ('proxyscrape_premium', 'webshare', 'free')
        }
        self.last_stats_log = 0.0
        self._webshare_warned = False

    def source(self, proxy):
        with self.lock:
            return self.provider_by_proxy.get(proxy, 'free')

    def source_fast(self, proxy):
        # Safe lock-free read under CPython for logging/scoring; stale value is harmless.
        return self.provider_by_proxy.get(proxy, 'free')

    def _set_provider_snapshot(self, name, proxies):
        proxies = set(proxies or ())
        with self.lock:
            old = self.proxy_sets.get(name, set())
            for p in old - proxies:
                if self.provider_by_proxy.get(p) == name:
                    self.provider_by_proxy.pop(p, None)
            self.proxy_sets[name] = proxies
            for p in proxies:
                self.provider_by_proxy[p] = name
            self.last_refresh[name] = time.time()
        return len(proxies)

    def _fetch_webshare(self):
        if not WEBSHARE_API_KEY:
            return []
        url = 'https://proxy.webshare.io/api/v2/proxy/list/'
        headers = {'Authorization': f'Token {WEBSHARE_API_KEY}'}
        params = {'mode': 'direct', 'valid': 'true', 'page': 1, 'page_size': 100}
        out = []
        try:
            while url and len(out) < 500:
                resp = requests.get(url, headers=headers, params=params if '?' not in url else None,
                                    timeout=PROVIDER_API_TIMEOUT)
                if resp.status_code != 200:
                    logging.warning(f"Webshare API: HTTP {resp.status_code}; используем старый snapshot")
                    return None
                data = resp.json()
                for row in data.get('results') or []:
                    if row.get('valid') is not True:
                        continue
                    host = str(row.get('proxy_address') or '').strip()
                    port = row.get('port')
                    user = str(row.get('username') or '')
                    password = str(row.get('password') or '')
                    if host and port and user and password:
                        out.append(f"http://{quote(user, safe='')}:{quote(password, safe='')}@{host}:{int(port)}")
                url = data.get('next')
                params = None
            return list(dict.fromkeys(out))
        except Exception as e:
            logging.warning(f"Webshare API недоступен: {e}; используем старый snapshot")
            return None

    def _discover_ps_subaccount(self):
        if self.ps_subaccount_id or not PROXYSCRAPE_PREMIUM_API_KEY:
            return self.ps_subaccount_id
        try:
            resp = requests.get(
                'https://api.proxyscrape.com/v4/account/subaccounts',
                headers={'api-token': PROXYSCRAPE_PREMIUM_API_KEY},
                timeout=PROVIDER_API_TIMEOUT,
            )
            if resp.status_code != 200:
                logging.warning(
                    f"ProxyScrape Premium: subaccount auto-discovery HTTP {resp.status_code}. "
                    "Если API key не имеет subaccount:read, задайте PROXYSCRAPE_PREMIUM_SUBACCOUNT_ID."
                )
                return ''
            data = resp.json().get('data', {}).get('subaccounts', [])
            rows = [r for r in data if str(r.get('AccountType', '')).lower() == 'datacenter_shared']
            if not rows:
                logging.warning('ProxyScrape Premium: datacenter_shared subaccount не найден')
                return ''
            # Prefer a visible/non-hidden account and label containing premium/trial when present.
            rows.sort(key=lambda r: (
                'premium' in str(r.get('label', '')).lower() or 'trial' in str(r.get('label', '')).lower(),
                not bool(r.get('is_hidden')),
            ), reverse=True)
            self.ps_subaccount_id = str(rows[0].get('AccountID') or '').strip()
            return self.ps_subaccount_id
        except Exception as e:
            logging.warning(f"ProxyScrape Premium: subaccount auto-discovery error: {e}")
            return ''

    def _fetch_proxyscrape_premium(self):
        if not PROXYSCRAPE_PREMIUM_API_KEY:
            return []
        sid = self._discover_ps_subaccount()
        if not sid:
            return None
        url = f'https://api.proxyscrape.com/v4/account/{sid}/datacenter_shared/proxy-list'
        params = {
            'type': 'displayproxies',
            'protocol': 'http',
            'format': 'credentials',
            'credential_format': 3,
            'status': 'online',
            'limit': 500,
        }
        try:
            resp = requests.get(url, headers={'api-token': PROXYSCRAPE_PREMIUM_API_KEY},
                                params=params, timeout=PROVIDER_API_TIMEOUT)
            if resp.status_code != 200:
                msg = (resp.text or '')[:180].replace('\n', ' ')
                logging.warning(f"ProxyScrape Premium API: HTTP {resp.status_code}: {msg}; используем старый snapshot")
                return None
            out = []
            for raw in resp.text.splitlines():
                line = raw.strip()
                if not line:
                    continue
                if '://' not in line:
                    line = 'http://' + line
                if _proxy_scheme(line) in ('http', 'https'):
                    out.append(line)
            return list(dict.fromkeys(out))
        except Exception as e:
            logging.warning(f"ProxyScrape Premium API недоступен: {e}; используем старый snapshot")
            return None

    def refresh_all(self, force=False):
        now = time.time()
        tasks = []
        with self.lock:
            ps_due = force or (now - self.last_refresh['proxyscrape_premium']) >= PROXYSCRAPE_PREMIUM_REFRESH
            ws_due = force or (now - self.last_refresh['webshare']) >= WEBSHARE_REFRESH
        if ps_due and PROXYSCRAPE_PREMIUM_API_KEY:
            rows = self._fetch_proxyscrape_premium()
            if rows is not None:
                n = self._set_provider_snapshot('proxyscrape_premium', rows)
                logging.info(f"🔷 ProxyScrape Premium: {n} online HTTP proxy загружено")
        if ws_due and WEBSHARE_API_KEY:
            rows = self._fetch_webshare()
            if rows is not None:
                n = self._set_provider_snapshot('webshare', rows)
                logging.info(f"🟩 Webshare: {n} valid direct proxy загружено (rescue tier)")

    def candidates(self, include_webshare=False):
        with self.lock:
            premium = list(self.proxy_sets['proxyscrape_premium'])
            webshare = list(self.proxy_sets['webshare']) if include_webshare else []
        return premium + webshare

    def record_result(self, proxy, result, body_bytes=0):
        src = self.source_fast(proxy)
        if src not in self.stats:
            src = 'free'
        with self.lock:
            st = self.stats[src]
            st['probe'] += 1
            if result in st:
                st[result] += 1
            st['bytes'] += max(0, int(body_bytes or 0))
            now = time.time()
            do_log = (now - self.last_stats_log) >= PROVIDER_STATS_INTERVAL
            if do_log:
                self.last_stats_log = now
                snapshot = {k: dict(v) for k, v in self.stats.items()}
        if do_log:
            bits = []
            for name in ('proxyscrape_premium', 'webshare', 'free'):
                st = snapshot[name]
                p = st['probe']; s = st['success']
                rate = (100.0 * s / p) if p else 0.0
                bits.append(
                    f"{name}: probes={p}, success={s} ({rate:.1f}%), "
                    f"403={st['blocked']}, timeout={st['proxy_timeout']}, "
                    f"reject={st['proxy_rejected']}, ssl={st['proxy_ssl']}, "
                    f"data≈{st['bytes']/1024/1024:.1f}MB"
                )
            logging.info('📊 Provider stats | ' + ' | '.join(bits))

    def webshare_estimated_mb(self):
        with self.lock:
            return self.stats['webshare']['bytes'] / 1024 / 1024

    def maybe_warn_webshare_usage(self):
        mb = self.webshare_estimated_mb()
        if mb >= WEBSHARE_ESTIMATED_MB_WARN and not self._webshare_warned:
            self._webshare_warned = True
            logging.warning(
                f"⚠️ Webshare estimated traffic in this process ≈{mb:.1f}MB. "
                "Это оценка по HTML body, не billing-данные Webshare."
            )


provider_manager = ProviderManager()

class ProxyManager:
    def __init__(self, proxy_list_url=None):
        self.proxy_list_url = proxy_list_url
        self.proxies = []
        self.all_proxies = []
        self.lock = threading.Lock()
        self.last_refresh = 0
        self.last_emergency_refresh = 0
        self.last_deep_emergency_refresh = 0
        self.refresh_interval = PROXY_REFRESH_INTERVAL

        # cooldown конкретного protocol://ip:port
        self.bad_until = {}
        # cooldown IP целиком. Нужен прежде всего для 403/429 и плохого TLS:
        # eBay видит exit IP, поэтому перебор 5 портов одного IP бессмысленен.
        self.host_bad_until = {}

        self.last_used = {}
        self.success_score = {}
        self.last_success_at = {}
        self.fail_streak = {}
        self.last_failure_result = {}
        self.last_failure_at = {}
        # Источник кандидата: текущий быстрый standard snapshot (timeout=1500).
        # Emergency 3000 нужен как расширение, но свежие standard endpoints должны
        # иметь мягкий приоритет, потому что по логам реальные победители часто
        # появляются именно в новом 1500-ms снимке.
        self.standard_current = set()
        self.standard_first_seen_at = {}

        # V6.19: быстрый TCP cache + отдельный более сильный HTTPS/TLS quality cache.
        self.preflight_ok_until = {}
        self.preflight_bad_until = {}
        self.preflight_last_result = {}
        self.preflight_latency_ms = {}
        self.quality_ok_until = {}
        self.quality_bad_until = {}
        self.quality_last_result = {}
        self.quality_latency_ms = {}

        # V6.19: память текущего outage между отдельными discovery-вызовами.
        # Она не является blacklist: после TTL адрес снова становится обычным кандидатом.
        self.outage_host_last_probe = {}
        self.outage_proxy_last_probe = {}

        # Мягкий cross-surface penalty (auction <-> main) после eBay 403/429.
        self.soft_host_penalty_until = {}

    def _cleanup_bad_locked(self):
        now = time.time()
        for p in [p for p, until in self.bad_until.items() if until <= now]:
            self.bad_until.pop(p, None)
        for host in [h for h, until in self.host_bad_until.items() if until <= now]:
            self.host_bad_until.pop(host, None)
        for p in [p for p, until in self.preflight_ok_until.items() if until <= now]:
            self.preflight_ok_until.pop(p, None)
            self.preflight_latency_ms.pop(p, None)
        for p in [p for p, until in self.preflight_bad_until.items() if until <= now]:
            self.preflight_bad_until.pop(p, None)
            self.preflight_last_result.pop(p, None)
        for p in [p for p, until in self.quality_ok_until.items() if until <= now]:
            self.quality_ok_until.pop(p, None)
            self.quality_latency_ms.pop(p, None)
        for p in [p for p, until in self.quality_bad_until.items() if until <= now]:
            self.quality_bad_until.pop(p, None)
            self.quality_last_result.pop(p, None)
        for host in [h for h, until in self.soft_host_penalty_until.items() if until <= now]:
            self.soft_host_penalty_until.pop(host, None)
        outage_cutoff = now - OUTAGE_HOST_MEMORY
        for host in [h for h, ts in self.outage_host_last_probe.items() if ts < outage_cutoff]:
            self.outage_host_last_probe.pop(host, None)
        for p in [p for p, ts in self.outage_proxy_last_probe.items() if ts < outage_cutoff]:
            self.outage_proxy_last_probe.pop(p, None)

        # Возвращаем proxy после cooldown, если он есть в свежем списке.
        current = set(self.proxies)
        for p in self.all_proxies:
            host = _proxy_host(p)
            if (
                self.bad_until.get(p, 0) <= now
                and self.host_bad_until.get(host, 0) <= now
                and p not in current
            ):
                self.proxies.append(p)
                current.add(p)

    def fetch_proxies_from_api(self, url=None, label='standard'):
        target_url = url or self.proxy_list_url
        if not target_url:
            return []

        try:
            if label == 'emergency':
                prefix = "🆘 Emergency ProxyScrape"
            elif label == 'deep-emergency':
                prefix = "🆘🆘 Deep-emergency ProxyScrape"
            else:
                prefix = "Загрузка прокси"
            logging.info(f"{prefix} из {target_url}")
            resp = requests.get(target_url, timeout=15)
            if resp.status_code != 200:
                logging.error(f"Ошибка загрузки прокси ({label}): HTTP {resp.status_code}")
                return []

            proxies = []
            seen = set()
            scheme_counts = {'http': 0, 'https': 0, 'socks5': 0, 'skipped': 0}

            for raw_line in resp.text.splitlines():
                line = raw_line.strip()
                if not line:
                    continue
                if '://' not in line:
                    line = 'http://' + line

                scheme = _proxy_scheme(line)
                if scheme not in ('http', 'https', 'socks5'):
                    scheme_counts['skipped'] += 1
                    continue
                if line in seen:
                    continue

                seen.add(line)
                proxies.append(line)
                scheme_counts[scheme] = scheme_counts.get(scheme, 0) + 1

            random.shuffle(proxies)
            logging.info(
                f"Загружено {len(proxies)} пригодных proxy ({label}; "
                f"http={scheme_counts.get('http', 0)}, "
                f"https={scheme_counts.get('https', 0)}, "
                f"socks5={scheme_counts.get('socks5', 0)}, "
                f"пропущено={scheme_counts.get('skipped', 0)})"
            )
            return proxies
        except Exception as e:
            logging.error(f"Ошибка при получении прокси ({label}): {e}")
            return []

    def refresh_proxies(self, force=False, emergency=False):
        """Обновляет основной пул или аккуратно ДОБАВЛЯЕТ emergency-кандидатов.

        Emergency timeout=3000 не заменяет основной список 1500 и не снимает cooldown.
        Он лишь расширяет all_proxies новыми endpoint-ами, которых не было в быстром пуле.
        """
        with self.lock:
            self._cleanup_bad_locked()
            now = time.time()
            if not emergency:
                if not force and self.all_proxies and (now - self.last_refresh) < self.refresh_interval:
                    return False
            else:
                if not force and (now - self.last_emergency_refresh) < self.refresh_interval:
                    return False

        target_url = PROXY_LIST_EMERGENCY_URL if emergency else self.proxy_list_url
        new_proxies = self.fetch_proxies_from_api(
            url=target_url,
            label='emergency' if emergency else 'standard',
        )

        with self.lock:
            now = time.time()
            self._cleanup_bad_locked()
            if new_proxies:
                if emergency:
                    old_all = list(self.all_proxies)
                    merged = list(dict.fromkeys(old_all + new_proxies))
                    added = len(merged) - len(old_all)
                    self.all_proxies = merged
                    current = set(self.proxies)
                    for p in new_proxies:
                        if self.bad_until.get(p, 0) <= now and p not in current:
                            host = _proxy_host(p)
                            if self.host_bad_until.get(host, 0) <= now:
                                self.proxies.append(p)
                                current.add(p)
                    self.last_emergency_refresh = now
                    logging.info(
                        f"🆘 Emergency pool объединён: +{added} новых endpoint; "
                        f"всего известно {len(self.all_proxies)}, доступно {len(self.proxies)} "
                        f"(proxy cooldown: {len(self.bad_until)}, host cooldown: {len(self.host_bad_until)})"
                    )
                else:
                    self.standard_current = set(new_proxies)
                    for p in new_proxies:
                        self.standard_first_seen_at.setdefault(p, now)

                    # V6.19: standard refresh больше НЕ выбрасывает только что прогретый
                    # резерв. В v6.18 preflight мог найти хорошие TCP endpoint-ы, но через
                    # минуту новый ProxyScrape snapshot заменял self.proxies целиком прямо
                    # перед failover. Сохраняем ограниченное число свежих warm/known-good
                    # endpoint-ов до конца их TTL.
                    warm_candidates = set()
                    warm_candidates.update(
                        p for p, until in self.quality_ok_until.items() if until > now
                    )
                    warm_candidates.update(
                        p for p, until in self.preflight_ok_until.items() if until > now
                    )
                    warm_candidates.update(
                        p for p, ts in self.last_success_at.items()
                        if (now - ts) <= GOOD_PROXY_MEMORY
                    )
                    warm_candidates.difference_update(self.standard_current)

                    def _retain_key(p):
                        quality = 1 if self.quality_ok_until.get(p, 0) > now else 0
                        tcp = 1 if self.preflight_ok_until.get(p, 0) > now else 0
                        ebay = 1 if (now - self.last_success_at.get(p, 0)) <= GOOD_PROXY_MEMORY else 0
                        latency = self.quality_latency_ms.get(
                            p, self.preflight_latency_ms.get(p, 999999.0)
                        )
                        return (ebay, quality, tcp, -float(latency))

                    retained = sorted(warm_candidates, key=_retain_key, reverse=True)
                    retained = retained[:PROXY_PREFLIGHT_RETAIN_MAX]
                    merged_standard = list(dict.fromkeys(new_proxies + retained))
                    self.all_proxies = merged_standard
                    self.proxies = [
                        p for p in merged_standard
                        if self.bad_until.get(p, 0) <= now
                        and self.host_bad_until.get(_proxy_host(p), 0) <= now
                    ]
                    self.last_refresh = now

                    # Не удаляем историю успешных proxy при каждом refresh: хороший IP
                    # может исчезнуть из одного снимка ProxyScrape и снова появиться.
                    cutoff = now - 6 * GOOD_PROXY_MEMORY
                    stale = [p for p, ts in self.last_success_at.items() if ts < cutoff]
                    for p in stale:
                        self.last_success_at.pop(p, None)
                        self.success_score.pop(p, None)
                        self.fail_streak.pop(p, None)
                        self.last_used.pop(p, None)
                        self.last_failure_result.pop(p, None)
                        self.last_failure_at.pop(p, None)

                    logging.info(
                        f"Пул proxy обновлён: {len(self.proxies)} доступно "
                        f"(standard={len(new_proxies)}, warm retained={len(retained)}, "
                        f"proxy cooldown: {len(self.bad_until)}, host cooldown: {len(self.host_bad_until)})"
                    )
                return True
            elif not self.proxies:
                logging.warning(
                    "Не удалось получить пригодные emergency proxy"
                    if emergency else "Не удалось получить пригодные proxy"
                )
            else:
                logging.warning(
                    "Emergency ProxyScrape не дал новых proxy, продолжаем старый пул"
                    if emergency else "Не удалось обновить proxy, продолжаем использовать старые"
                )
            return False

    def refresh_deep_emergency(self, force=False):
        """Последний резервный tier ProxyScrape timeout=4000 мс.

        Не заменяет standard/emergency pool и никогда не снимает cooldown. Используется
        только в реально затянувшемся discovery после обычного 3000-ms расширения.
        """
        with self.lock:
            self._cleanup_bad_locked()
            now = time.time()
            if not force and (now - self.last_deep_emergency_refresh) < self.refresh_interval:
                return False

        new_proxies = self.fetch_proxies_from_api(
            url=PROXY_LIST_DEEP_EMERGENCY_URL,
            label='deep-emergency',
        )
        if not new_proxies:
            return False

        with self.lock:
            now = time.time()
            self._cleanup_bad_locked()
            old_all = list(self.all_proxies)
            merged = list(dict.fromkeys(old_all + new_proxies))
            added = len(merged) - len(old_all)
            self.all_proxies = merged
            current = set(self.proxies)
            added_usable = 0
            for proxy in new_proxies:
                host = _proxy_host(proxy)
                if (
                    self.bad_until.get(proxy, 0) <= now
                    and self.host_bad_until.get(host, 0) <= now
                    and proxy not in current
                ):
                    self.proxies.append(proxy)
                    current.add(proxy)
                    added_usable += 1
            self.last_deep_emergency_refresh = now
            logging.info(
                f"🆘🆘 Deep-emergency pool {PROXY_DEEP_EMERGENCY_TIMEOUT_MS} ms: "
                f"+{added} новых endpoint, +{added_usable} usable; "
                f"всего известно {len(self.all_proxies)}, доступно {len(self.proxies)}"
            )
            return True

    def refresh_standard_merge(self, force=True):
        """Мягко подмешивает свежий standard(1500 ms) snapshot в текущий pool.

        В отличие от обычного standard refresh НЕ удаляет уже добавленные emergency
        endpoints. Используется максимум один раз внутри длинного discovery-цикла.
        Cooldown/host cooldown полностью сохраняются.
        """
        with self.lock:
            now = time.time()
            if not force and (now - self.last_refresh) < self.refresh_interval:
                return False

        new_proxies = self.fetch_proxies_from_api(url=self.proxy_list_url, label='standard')
        if not new_proxies:
            return False

        with self.lock:
            now = time.time()
            self._cleanup_bad_locked()
            old_all = list(self.all_proxies)
            old_set = set(old_all)
            self.standard_current = set(new_proxies)
            newly_seen = 0
            for p in new_proxies:
                if p not in self.standard_first_seen_at:
                    self.standard_first_seen_at[p] = now
                    newly_seen += 1
            self.all_proxies = list(dict.fromkeys(old_all + new_proxies))
            current = set(self.proxies)
            added_usable = 0
            for p in new_proxies:
                host = _proxy_host(p)
                if (
                    self.bad_until.get(p, 0) <= now
                    and self.host_bad_until.get(host, 0) <= now
                    and p not in current
                ):
                    self.proxies.append(p)
                    current.add(p)
                    added_usable += 1
            self.last_refresh = now

            # Чистим только очень старую source-метаинформацию.
            source_cutoff = now - 6 * 60 * 60
            for p in [p for p, ts in self.standard_first_seen_at.items() if ts < source_cutoff and p not in self.standard_current]:
                self.standard_first_seen_at.pop(p, None)

            logging.info(
                f"🔄 Mid-discovery standard merge: snapshot={len(new_proxies)}, "
                f"новых endpoint={len(set(new_proxies) - old_set)}, "
                f"впервые увидели={newly_seen}, добавлено usable={added_usable}, "
                f"всего известно={len(self.all_proxies)}"
            )
            return True

    def pool_stats(self):
        """Возвращает реальное число usable endpoint, учитывая и host cooldown."""
        with self.lock:
            self._cleanup_bad_locked()
            now = time.time()
            available = sum(
                1 for p in self.proxies
                if self.bad_until.get(p, 0) <= now
                and self.host_bad_until.get(_proxy_host(p), 0) <= now
            )
            return (
                available,
                len(self.all_proxies),
                len(self.bad_until),
                len(self.host_bad_until),
            )

    def emergency_needed(self):
        """Emergency допустим только ПОСЛЕ появления обычного 1500-ms pool.

        В V6.6 свежий процесс имел 0/0 proxy до первого standard refresh, и 0/0
        ошибочно считался "истощённым пулом". Из-за этого бот сразу стартовал с
        timeout=3000. Нулевой ещё-не-загруженный pool теперь НЕ является emergency.
        Если standard ProxyScrape действительно не загрузится, discovery сначала
        сделает обычную попытку 1500 ms, и лишь после этого может расшириться.
        """
        available, total, _, _ = self.pool_stats()
        if total <= 0:
            return False
        return (
            available <= PROXY_EMERGENCY_MIN_AVAILABLE
            or (available / max(1, total)) <= PROXY_EMERGENCY_MIN_RATIO
        )

    def standard_pool_loaded(self):
        with self.lock:
            return bool(self.all_proxies) and self.last_refresh > 0

    def _is_recent_good_locked(self, proxy, now=None):
        if now is None:
            now = time.time()
        return (now - self.last_success_at.get(proxy, 0)) <= GOOD_PROXY_MEMORY

    def is_recent_good(self, proxy):
        with self.lock:
            return self._is_recent_good_locked(proxy)

    def _preflight_state_locked(self, proxy, now=None):
        if not PROXY_PREFLIGHT_ENABLED:
            return 'disabled'
        if now is None:
            now = time.time()
        if self.preflight_ok_until.get(proxy, 0) > now:
            return 'ok'
        if self.preflight_bad_until.get(proxy, 0) > now:
            return 'bad'
        return 'unknown'

    def preflight_state(self, proxy):
        # Hot path: здесь не сканируем весь all_proxies через _cleanup_bad_locked().
        # TTL проверяется непосредственно в _preflight_state_locked.
        with self.lock:
            return self._preflight_state_locked(proxy)

    def preflight_failure_result(self, proxy):
        with self.lock:
            return self.preflight_last_result.get(proxy, 'proxy_error')

    def preflight_connect_timeout(self, proxy):
        """Standard 1500-ms pool проверяем быстрее; emergency-only даём чуть больше времени."""
        with self.lock:
            if proxy in self.standard_current:
                return PROXY_PREFLIGHT_CONNECT_TIMEOUT
        return min(2.0, max(PROXY_PREFLIGHT_CONNECT_TIMEOUT, PROXY_PREFLIGHT_CONNECT_TIMEOUT * 1.6))

    def mark_preflight_result(self, proxy, ok, result=None, latency_ms=None):
        if not proxy:
            return
        now = time.time()
        with self.lock:
            if ok:
                self.preflight_ok_until[proxy] = now + PROXY_PREFLIGHT_OK_TTL
                self.preflight_bad_until.pop(proxy, None)
                self.preflight_last_result.pop(proxy, None)
                if latency_ms is not None:
                    self.preflight_latency_ms[proxy] = float(latency_ms)
            else:
                self.preflight_bad_until[proxy] = now + PROXY_PREFLIGHT_BAD_TTL
                self.preflight_ok_until.pop(proxy, None)
                self.preflight_latency_ms.pop(proxy, None)
                self.preflight_last_result[proxy] = result or 'proxy_error'

    def _quality_state_locked(self, proxy, now=None):
        if not PROXY_QUALITY_PREFLIGHT_ENABLED:
            return 'disabled'
        if now is None:
            now = time.time()
        if self.quality_ok_until.get(proxy, 0) > now:
            return 'ok'
        if self.quality_bad_until.get(proxy, 0) > now:
            return 'bad'
        return 'unknown'

    def quality_state(self, proxy):
        with self.lock:
            return self._quality_state_locked(proxy)

    def quality_failure_result(self, proxy):
        with self.lock:
            return self.quality_last_result.get(proxy, 'proxy_error')

    def mark_quality_result(self, proxy, ok, result=None, latency_ms=None):
        """Soft cache: quality failure НЕ создаёт main cooldown и не банит endpoint."""
        if not proxy:
            return
        now = time.time()
        with self.lock:
            if ok:
                self.quality_ok_until[proxy] = now + PROXY_QUALITY_OK_TTL
                self.quality_bad_until.pop(proxy, None)
                self.quality_last_result.pop(proxy, None)
                if latency_ms is not None:
                    self.quality_latency_ms[proxy] = float(latency_ms)
            else:
                self.quality_bad_until[proxy] = now + PROXY_QUALITY_BAD_TTL
                self.quality_ok_until.pop(proxy, None)
                self.quality_latency_ms.pop(proxy, None)
                self.quality_last_result[proxy] = result or 'proxy_error'

    def warm_reserve_stats(self):
        now = time.time()
        with self.lock:
            self._cleanup_bad_locked()
            quality = 0
            tcp_only = 0
            used_hosts = set()
            candidates = list(dict.fromkeys(
                list(self.proxies)
                + list(self.quality_ok_until.keys())
                + list(self.preflight_ok_until.keys())
            ))
            for p in candidates:
                host = _proxy_host(p)
                if not host or host in used_hosts:
                    continue
                if self.bad_until.get(p, 0) > now or self.host_bad_until.get(host, 0) > now:
                    continue
                if self._quality_state_locked(p, now) == 'ok':
                    quality += 1
                    used_hosts.add(host)
                elif self._preflight_state_locked(p, now) == 'ok':
                    tcp_only += 1
                    used_hosts.add(host)
            return quality, tcp_only

    def get_quality_preflight_candidates(self, limit, excluded_hosts=None):
        """Кандидаты, которым ещё не делали свежую HTTPS/TLS quality-проверку."""
        if not PROXY_PREFLIGHT_ENABLED or limit <= 0:
            return []
        excluded_hosts = set(excluded_hosts or ())
        now = time.time()
        with self.lock:
            self._cleanup_bad_locked()
            rows = []
            used_hosts = set(excluded_hosts)
            for p in self.proxies:
                host = _proxy_host(p)
                if not host or host in used_hosts:
                    continue
                if self.bad_until.get(p, 0) > now or self.host_bad_until.get(host, 0) > now:
                    continue
                if self._quality_state_locked(p, now) != 'unknown':
                    continue
                if self._preflight_state_locked(p, now) == 'bad':
                    continue
                rows.append(p)

            # TCP-open идёт первым, затем ещё неизвестные; внутри — обычный score.
            rows.sort(
                key=lambda p: (
                    1 if self._preflight_state_locked(p, now) == 'ok' else 0,
                    self._candidate_score_locked(p, now),
                ),
                reverse=True,
            )
            result = []
            for p in rows:
                host = _proxy_host(p)
                if host in used_hosts:
                    continue
                result.append(p)
                used_hosts.add(host)
                if len(result) >= limit:
                    break
            return result

    def get_warm_standby_candidates(self, limit, excluded_hosts=None):
        """
        Возвращает резерв БЕЗ refresh ProxyScrape: сначала HTTPS/TLS-verified,
        затем максимум несколько свежих TCP-only. Используется сразу после падения fixed proxy.
        """
        if limit <= 0:
            return []
        excluded_hosts = set(excluded_hosts or ())
        now = time.time()
        with self.lock:
            self._cleanup_bad_locked()
            universe = list(dict.fromkeys(
                list(self.proxies)
                + list(self.quality_ok_until.keys())
                + list(self.preflight_ok_until.keys())
                + list(self.last_success_at.keys())
            ))
            quality = []
            tcp = []
            known = []
            for p in universe:
                host = _proxy_host(p)
                if not host or host in excluded_hosts:
                    continue
                if self.bad_until.get(p, 0) > now or self.host_bad_until.get(host, 0) > now:
                    continue
                if self.soft_host_penalty_until.get(host, 0) > now:
                    continue
                last_reason = self.last_failure_result.get(p)
                if (
                    self._is_recent_good_locked(p, now)
                    and self.fail_streak.get(p, 0) <= 2
                    and last_reason in (None, 'proxy_timeout', 'proxy_error')
                ):
                    known.append(p)
                elif self._quality_state_locked(p, now) == 'ok':
                    quality.append(p)
                elif self._preflight_state_locked(p, now) == 'ok':
                    tcp.append(p)

            def _standby_key(p):
                qlat = self.quality_latency_ms.get(p, 999999.0)
                tlat = self.preflight_latency_ms.get(p, 999999.0)
                return (
                    self._candidate_score_locked(p, now),
                    -min(qlat, tlat),
                )

            known.sort(key=_standby_key, reverse=True)
            quality.sort(key=_standby_key, reverse=True)
            tcp.sort(key=_standby_key, reverse=True)

            result = []
            used_hosts = set(excluded_hosts)

            def add_group(group, max_from_group=None):
                added = 0
                for p in group:
                    if len(result) >= limit:
                        break
                    if max_from_group is not None and added >= max_from_group:
                        break
                    host = _proxy_host(p)
                    if not host or host in used_hosts:
                        continue
                    result.append(p)
                    used_hosts.add(host)
                    self.last_used[p] = now
                    added += 1

            add_group(known)
            add_group(quality)
            # TCP-only — слабый сигнал. Не заполняем им весь первый batch.
            add_group(tcp, max_from_group=1)
            return result

    def remember_outage_attempt(self, proxy):
        if not proxy:
            return
        now = time.time()
        host = _proxy_host(proxy)
        with self.lock:
            if host:
                self.outage_host_last_probe[host] = now
            self.outage_proxy_last_probe[proxy] = now

    def clear_outage_memory(self):
        with self.lock:
            self.outage_host_last_probe.clear()
            self.outage_proxy_last_probe.clear()

    def _host_recent_outage_locked(self, host, now=None):
        if not host:
            return False
        if now is None:
            now = time.time()
        ts = self.outage_host_last_probe.get(host, 0)
        return bool(ts and (now - ts) < OUTAGE_HOST_MEMORY)

    def mark_soft_host_penalty(self, host, seconds=EBAY_CROSS_SOFT_PENALTY, reason='cross-surface eBay block'):
        if not host:
            return
        now = time.time()
        with self.lock:
            until = now + max(1, int(seconds))
            self.soft_host_penalty_until[host] = max(self.soft_host_penalty_until.get(host, 0), until)
        logging.info(f"🟠 Мягкий cross-surface penalty для host {host}: {int(seconds)} сек. ({reason})")

    def get_preflight_candidates(self, limit, excluded_hosts=None):
        """Кандидаты для тихого TCP-прогрева. Никаких запросов к eBay здесь нет."""
        if not PROXY_PREFLIGHT_ENABLED or limit <= 0:
            return []
        excluded_hosts = set(excluded_hosts or ())
        now = time.time()
        with self.lock:
            self._cleanup_bad_locked()
            rows = []
            used_hosts = set(excluded_hosts)
            for p in self.proxies:
                host = _proxy_host(p)
                if not host or host in used_hosts:
                    continue
                if self.bad_until.get(p, 0) > now or self.host_bad_until.get(host, 0) > now:
                    continue
                if self._preflight_state_locked(p, now) != 'unknown':
                    continue
                rows.append(p)
            rows.sort(key=lambda p: self._candidate_score_locked(p, now), reverse=True)
            result = []
            for p in rows:
                host = _proxy_host(p)
                if host in used_hosts:
                    continue
                result.append(p)
                used_hosts.add(host)
                if len(result) >= limit:
                    break
            return result

    def _candidate_score_locked(self, proxy, now):
        # V6.12: recently-good с 0-2 сбоями по-прежнему ценен, но после 3+ подряд
        # ошибок историческая репутация почти обнуляется. В V6.7 proxy с fail_streak=4
        # всё ещё мог обгонять абсолютно свежий endpoint из нового standard snapshot.
        last_ok = self.last_success_at.get(proxy, 0)
        age_ok = now - last_ok if last_ok else 10**9
        recent_bonus = 0.0
        if age_ok <= 5 * 60:
            recent_bonus = 100.0
        elif age_ok <= 30 * 60:
            recent_bonus = 60.0
        elif age_ok <= GOOD_PROXY_MEMORY:
            recent_bonus = 30.0

        streak = self.fail_streak.get(proxy, 0)
        success_bonus = self.success_score.get(proxy, 0) * 8.0
        last_reason = self.last_failure_result.get(proxy)

        if streak >= 3:
            # Не удаляем адрес навсегда: после cooldown он ещё сможет вернуться,
            # но свежие кандидаты должны идти раньше многократно падавшего known-good.
            recent_bonus *= 0.05
            success_bonus *= 0.15

        fail_penalty = streak * 4.0
        unstable_penalty = max(0, streak - 2) * 30.0
        reason_penalty = {
            'blocked': 30.0,
            'rate_limited': 35.0,
            'proxy_rejected': 24.0,
            'proxy_ssl': 45.0,
            'proxy_timeout': 4.0,
            'proxy_error': 6.0,
            'http_error': 10.0,
        }.get(last_reason, 0.0)

        # Текущий быстрый timeout=1500 snapshot получает мягкий приоритет над
        # emergency-only endpoint. Совсем новый standard endpoint — ещё небольшой бонус.
        standard_bonus = 10.0 if proxy in self.standard_current else 0.0
        first_seen = self.standard_first_seen_at.get(proxy, 0)
        fresh_standard_bonus = 0.0
        if proxy in self.standard_current and first_seen and (now - first_seen) <= 120:
            fresh_standard_bonus = 8.0

        scheme_bonus = 1.2 if _proxy_scheme(proxy) in ('http', 'https') else 0.0
        idle = now - self.last_used.get(proxy, 0)
        idle_bonus = min(4.0, idle / 60.0) if idle < 10**8 else 4.0

        # V6.19: TCP-open — слабый сигнал, HTTPS/TLS quality — сильный.
        # Quality не гарантирует eBay 200 (IP ещё может получить 403), но уже доказывает,
        # что endpoint реально умеет HTTPS tunnel без MITM/reject.
        tcp_state = self._preflight_state_locked(proxy, now)
        quality_state = self._quality_state_locked(proxy, now)
        preflight_bonus = 12.0 if tcp_state == 'ok' else 0.0
        quality_bonus = 85.0 if quality_state == 'ok' else 0.0
        quality_bad_penalty = 28.0 if quality_state == 'bad' else 0.0

        provider = provider_manager.source_fast(proxy)
        # Premium gets immediate priority. Webshare is metered and only enters candidates
        # after the rescue unlock; once unlocked it must be tried promptly instead of
        # waiting behind dozens of bad Premium IPs.
        provider_bonus = 150.0 if provider == 'proxyscrape_premium' else (210.0 if provider == 'webshare' else 0.0)

        host = _proxy_host(proxy)
        soft_penalty = 70.0 if self.soft_host_penalty_until.get(host, 0) > now else 0.0
        outage_penalty = 0.0
        if self._host_recent_outage_locked(host, now) and not self._is_recent_good_locked(proxy, now):
            outage_penalty = 45.0

        return (
            recent_bonus + success_bonus + standard_bonus + fresh_standard_bonus
            + scheme_bonus + idle_bonus + preflight_bonus + quality_bonus + provider_bonus
            - fail_penalty - unstable_penalty - reason_penalty
            - quality_bad_penalty - soft_penalty - outage_penalty
            + random.uniform(0, 2.0)
        )

    def get_candidate_batch(self, batch_size, tried_hosts=None, preferred_scheme=None, allow_webshare=False):
        """Выдаёт несколько proxy с уникальными IP и разумным mix HTTP/SOCKS5.

        Раньше небольшой bonus HTTP приводил к тому, что при большом пуле первые десятки
        probe могли почти целиком состоять из HTTP, хотя в ProxyScrape SOCKS5 было больше.
        Теперь недавно успешные proxy всё равно имеют абсолютный приоритет, а среди новых
        кандидатов в batch>=3 резервируем один слот под SOCKS5, если он доступен.
        Так мы реально используем весь большой пул, не повышая число одновременных запросов.
        """
        tried_hosts = tried_hosts or set()
        # Если emergency 3000-ms pool был только что добавлен, не даём автоматическому
        # 60-секундному standard refresh тут же заменить его обратно на один 1500-ms
        # список посреди того же discovery. После ~90 сек. следующий поиск снова начнёт
        # с обычного свежего 1500-ms пула.
        now = time.time()
        with self.lock:
            emergency_hold = max(self.refresh_interval, SEARCH_TIME_BUDGET + 15)
            emergency_recent = bool(
                self.last_emergency_refresh > self.last_refresh
                and (now - self.last_emergency_refresh) < emergency_hold
            )
        if not emergency_recent:
            self.refresh_proxies()
        now = time.time()

        with self.lock:
            self._cleanup_bad_locked()
            candidates = list(provider_manager.candidates(include_webshare=allow_webshare)) + list(self.proxies)

            # Недавно успешный ИЛИ прогретый reserve можно использовать даже если endpoint
            # исчез из очередного ProxyScrape snapshot. Это устраняет главный недостаток
            # v6.18: warm cache был, но refresh мог выкинуть сам endpoint из active pool.
            warm_extra = set()
            warm_extra.update(
                p for p, ts in self.last_success_at.items()
                if now - ts <= GOOD_PROXY_MEMORY
            )
            warm_extra.update(
                p for p, until in self.quality_ok_until.items() if until > now
            )
            warm_extra.update(
                p for p, until in self.preflight_ok_until.items() if until > now
            )
            for p in warm_extra:
                if p not in candidates and self.bad_until.get(p, 0) <= now:
                    candidates.append(p)

            quality_usable = []
            tcp_usable = []
            fresh_usable = []
            recycled_usable = []
            for p in candidates:
                if self.bad_until.get(p, 0) > now:
                    continue
                host = _proxy_host(p)
                if not host or host in tried_hosts:
                    continue
                if self.host_bad_until.get(host, 0) > now:
                    continue
                # Если быстрый TCP-preflight недавно уже доказал, что endpoint мёртв,
                # не тратим на него eBay worker и обычный attempt-counter до истечения короткого TTL.
                if self._preflight_state_locked(p, now) == 'bad':
                    continue
                soft_penalized = self.soft_host_penalty_until.get(host, 0) > now
                recent_outage_unknown = (
                    self._host_recent_outage_locked(host, now)
                    and not self._is_recent_good_locked(p, now)
                )
                quality_state = self._quality_state_locked(p, now)
                tcp_state = self._preflight_state_locked(p, now)

                if quality_state == 'ok' and not soft_penalized:
                    quality_usable.append(p)
                elif tcp_state == 'ok' and quality_state != 'bad' and not soft_penalized:
                    tcp_usable.append(p)
                elif soft_penalized or recent_outage_unknown or quality_state == 'bad':
                    recycled_usable.append(p)
                else:
                    fresh_usable.append(p)

            quality_usable.sort(key=lambda p: self._candidate_score_locked(p, now), reverse=True)
            tcp_usable.sort(key=lambda p: self._candidate_score_locked(p, now), reverse=True)
            fresh_usable.sort(key=lambda p: self._candidate_score_locked(p, now), reverse=True)
            recycled_usable.sort(key=lambda p: self._candidate_score_locked(p, now), reverse=True)
            # HTTPS/TLS-verified reserve идёт раньше неизвестных. TCP-only полезен, но
            # не должен забить все worker slots — в реальном логе TCP-open было ~71%,
            # а CONNECT/SSL failures всё равно оставались частыми.
            usable = quality_usable + tcp_usable + fresh_usable + recycled_usable

            batch = []
            batch_hosts = set()

            def add_candidate(p):
                if len(batch) >= batch_size:
                    return False
                host = _proxy_host(p)
                if not host or host in batch_hosts:
                    return False
                batch.append(p)
                batch_hosts.add(host)
                self.last_used[p] = now
                return True

            # 1) Стабильный недавно успешный proxy важнее protocol mix.
            # Но после 3+ подряд ошибок он теряет абсолютный приоритет и дальше
            # конкурирует с остальными по score — это не даёт старой плохой истории
            # занимать все worker-слоты в тяжёлом discovery.
            for p in usable:
                last_reason = self.last_failure_result.get(p)
                host = _proxy_host(p)
                transport_retry_ok = last_reason in (None, 'proxy_timeout', 'proxy_error')
                if (
                    self._is_recent_good_locked(p, now)
                    and self.fail_streak.get(p, 0) <= 2
                    and transport_retry_ok
                    and self.soft_host_penalty_until.get(host, 0) <= now
                ):
                    add_candidate(p)
                    if len(batch) >= batch_size:
                        return batch

            # 2) Managed providers + verified free reserve. Не даём одному источнику
            # монополизировать весь batch: это одновременно ускоряет поиск и даёт честную
            # сравнительную статистику. До rescue-unlock Webshare вообще отсутствует.
            if allow_webshare:
                for p in usable:
                    if provider_manager.source_fast(p) == 'webshare':
                        if add_candidate(p):
                            break  # metered: максимум один Webshare slot за batch

            premium_limit = max(1, batch_size - 1)
            premium_added = 0
            for p in usable:
                if provider_manager.source_fast(p) != 'proxyscrape_premium':
                    continue
                if add_candidate(p):
                    premium_added += 1
                    if premium_added >= premium_limit or len(batch) >= batch_size:
                        break

            # Один слот по возможности оставляем уже доказанному HTTPS/TLS free-reserve.
            for p in quality_usable:
                if provider_manager.source_fast(p) == 'free' and add_candidate(p):
                    break

            # Если verified-free не было, свободные места снова отдаём Premium.
            if len(batch) < batch_size:
                for p in usable:
                    if provider_manager.source_fast(p) == 'proxyscrape_premium':
                        add_candidate(p)
                        if len(batch) >= batch_size:
                            return batch

            # Остальные quality-ready (например future managed source) тоже полезны.
            if len(batch) < batch_size:
                for p in quality_usable:
                    add_candidate(p)
                    if len(batch) >= batch_size:
                        return batch

            # TCP-only — только один дополнительный слот: лог v6.18 показал, что сам
            # открытый порт слишком слабый признак и не должен забивать весь batch.
            tcp_added = 0
            for p in tcp_usable:
                if add_candidate(p):
                    tcp_added += 1
                    if tcp_added >= 1 or len(batch) >= batch_size:
                        break

            # 3) Rolling-discovery иногда просит всего один новый слот. Если текущий
            # in-flight набор остался без SOCKS5, preferred_scheme='socks5' не даёт
            # HTTP-бонусу снова вытеснить весь SOCKS5-пул.
            if preferred_scheme and len(batch) < batch_size:
                # ВАЖНО: preferred_scheme означает "зарезервировать ОДИН слот",
                # а не заполнить им весь batch. В v6.3 этот цикл мог набрать сразу
                # 3 SOCKS5, что видно по логам и ухудшало поиск из-за множества
                # SSL/MITM-ошибок бесплатных SOCKS5.
                for p in usable:
                    if _proxy_scheme(p) == preferred_scheme and add_candidate(p):
                        break

            # 4) Для новых адресов используем примерно 3:1 HTTP:SOCKS5 (2:1 при batch=3).
            # Это сохраняет приоритет HTTP по реальным UK-логам, но не оставляет 150-200
            # SOCKS5 вообще непроверенными до истечения discovery budget.
            remaining = batch_size - len(batch)
            if remaining > 0:
                selected_socks = sum(1 for p in batch if _proxy_scheme(p) == 'socks5')
                want_socks_total = 1 if batch_size >= 3 else 0
                need_socks = max(0, want_socks_total - selected_socks)

                if need_socks:
                    for p in usable:
                        if _proxy_scheme(p) == 'socks5' and add_candidate(p):
                            need_socks -= 1
                            if need_socks <= 0 or len(batch) >= batch_size:
                                break

            # 5) Остальные слоты в первую очередь HTTP/HTTPS, затем любой protocol.
            if len(batch) < batch_size:
                for p in usable:
                    if _proxy_scheme(p) in ('http', 'https'):
                        add_candidate(p)
                        if len(batch) >= batch_size:
                            break

            if len(batch) < batch_size:
                for p in usable:
                    add_candidate(p)
                    if len(batch) >= batch_size:
                        break

            return batch

    def get_due_reprobe_candidate(self, tried_proxies, reprobed_proxies, inflight_hosts=None):
        """Возвращает один недавно успешный proxy для повторной проверки в ТОМ ЖЕ discovery.

        Это решает сценарий из реального лога: хороший proxy временно упал, получил
        короткий cooldown, был проверен в начале 75-секундного discovery, затем успел
        восстановиться, но старый tried_hosts уже не позволял попробовать его снова.

        Ограничения безопасности:
        - только proxy, который реально был успешным в последний час;
        - только если он уже пробовался в этом discovery и после этого успел выйти из cooldown;
        - максимум ОДИН re-probe для конкретного proxy за discovery;
        - только при fail_streak 1-2 (нестабильные 3+ не получают быстрый повтор);
        - не запускаем второй запрос на тот же host, пока первый ещё in-flight.
        """
        tried_proxies = set(tried_proxies or ())
        reprobed_proxies = set(reprobed_proxies or ())
        inflight_hosts = set(inflight_hosts or ())
        if not tried_proxies:
            return None

        now = time.time()
        with self.lock:
            self._cleanup_bad_locked()
            candidates = []
            for p in tried_proxies:
                if p in reprobed_proxies:
                    continue
                if not self._is_recent_good_locked(p, now):
                    continue
                streak = self.fail_streak.get(p, 0)
                if streak < 1 or streak > 2:
                    continue
                if self.bad_until.get(p, 0) > now:
                    continue
                host = _proxy_host(p)
                if not host or host in inflight_hosts:
                    continue
                if self.host_bad_until.get(host, 0) > now:
                    continue
                candidates.append(p)

            if not candidates:
                return None

            candidates.sort(key=lambda p: self._candidate_score_locked(p, now), reverse=True)
            proxy = candidates[0]
            self.last_used[proxy] = now
            return proxy

    def get_due_transient_reprobe_candidate(
        self,
        tried_proxies,
        reprobed_proxies,
        inflight_hosts=None,
        min_age=None,
    ):
        """Один второй шанс только для transient transport-сбоев в emergency-mode.

        Жёсткие ошибки (SSL/MITM, CONNECT rejected, 403/429) сюда никогда не попадают.
        Это позволяет временно ожившему бесплатному proxy вернуться в том же длинном
        discovery, не превращая поиск в бесконечный круг по одним адресам.
        """
        tried_proxies = set(tried_proxies or ())
        reprobed_proxies = set(reprobed_proxies or ())
        inflight_hosts = set(inflight_hosts or ())
        min_age = PROXY_EMERGENCY_TRANSIENT_MIN_AGE if min_age is None else float(min_age)
        if not tried_proxies:
            return None

        now = time.time()
        with self.lock:
            self._cleanup_bad_locked()
            candidates = []
            for p in tried_proxies:
                if p in reprobed_proxies:
                    continue
                if self.last_failure_result.get(p) not in ('proxy_timeout', 'proxy_error'):
                    continue
                failed_at = self.last_failure_at.get(p, 0)
                if failed_at <= 0 or (now - failed_at) < min_age:
                    continue
                if self.fail_streak.get(p, 0) > 2:
                    continue
                if self.bad_until.get(p, 0) > now:
                    continue
                host = _proxy_host(p)
                if not host or host in inflight_hosts:
                    continue
                if self.host_bad_until.get(host, 0) > now:
                    continue
                candidates.append(p)

            if not candidates:
                return None
            candidates.sort(key=lambda p: self._candidate_score_locked(p, now), reverse=True)
            proxy = candidates[0]
            self.last_used[proxy] = now
            return proxy

    def get_final_known_good_reprobe(self, tried_proxies, inflight_hosts=None):
        """Последний один шанс known-good после исчерпания обычных 120 попыток."""
        tried_proxies = set(tried_proxies or ())
        inflight_hosts = set(inflight_hosts or ())
        now = time.time()
        with self.lock:
            self._cleanup_bad_locked()
            candidates = []
            for p in tried_proxies:
                if not self._is_recent_good_locked(p, now):
                    continue
                if self.bad_until.get(p, 0) > now:
                    continue
                host = _proxy_host(p)
                if not host or host in inflight_hosts:
                    continue
                if self.host_bad_until.get(host, 0) > now:
                    continue
                if self.fail_streak.get(p, 0) > 3:
                    continue
                candidates.append(p)
            if not candidates:
                return None
            candidates.sort(key=lambda p: self._candidate_score_locked(p, now), reverse=True)
            proxy = candidates[0]
            self.last_used[proxy] = now
            return proxy

    def mark_success(self, proxy):
        if not proxy:
            return
        now = time.time()
        host = _proxy_host(proxy)
        with self.lock:
            self.success_score[proxy] = min(20, self.success_score.get(proxy, 0) + 2)
            self.last_success_at[proxy] = now
            self.fail_streak[proxy] = 0
            self.last_failure_result.pop(proxy, None)
            self.last_failure_at.pop(proxy, None)
            self.bad_until.pop(proxy, None)
            # Реальный eBay success сильнее любого preflight: endpoint точно проводит HTTPS.
            self.preflight_ok_until[proxy] = now + PROXY_PREFLIGHT_OK_TTL
            self.preflight_bad_until.pop(proxy, None)
            self.preflight_last_result.pop(proxy, None)
            self.quality_ok_until[proxy] = now + PROXY_QUALITY_OK_TTL
            self.quality_bad_until.pop(proxy, None)
            self.quality_last_result.pop(proxy, None)
            # Успех завершает outage — следующий будущий сбой должен начинать с чистой памяти.
            self.outage_host_last_probe.clear()
            self.outage_proxy_last_probe.clear()
            # Если этот же IP только что доказал работоспособность, снимаем host cooldown/soft penalty.
            self.host_bad_until.pop(host, None)
            self.soft_host_penalty_until.pop(host, None)
            if proxy not in self.proxies:
                self.proxies.append(proxy)

    def mark_failure(self, proxy, result, reason=None):
        """Адаптивный cooldown: хороший proxy после одного сбоя возвращается быстро."""
        if not proxy:
            return 0

        now = time.time()
        host = _proxy_host(proxy)
        with self.lock:
            known_good = self._is_recent_good_locked(proxy, now)
            streak = self.fail_streak.get(proxy, 0) + 1
            self.fail_streak[proxy] = streak
            self.last_failure_result[proxy] = result
            self.last_failure_at[proxy] = now

            # Единичный сбой недавно успешного proxy не уничтожает его репутацию.
            old_score = self.success_score.get(proxy, 0)
            self.success_score[proxy] = max(1 if known_good else 0, old_score - 1)

            if result == 'proxy_ssl':
                cooldown = SSL_PROXY_COOLDOWN
                host_cooldown = cooldown
            elif result == 'rate_limited':
                if known_good:
                    cooldown = [120, 300, 600, 1200][min(streak - 1, 3)]
                else:
                    cooldown = [600, 1200, 1800, 1800][min(streak - 1, 3)]
                host_cooldown = cooldown
            elif result == 'blocked':
                # 403 / Pardon — это уже сигнал eBay по exit IP, поэтому даже ранее
                # успешный адрес не возвращаем слишком быстро: минимум 5 минут.
                if known_good:
                    cooldown = [300, 600, 900, 1200][min(streak - 1, 3)]
                else:
                    cooldown = [300, 600, 1200, 1800][min(streak - 1, 3)]
                host_cooldown = cooldown
            elif result == 'proxy_rejected':
                # CONNECT 400/405/500/aborted — жёсткий отказ туннеля. Это не обычный
                # transient timeout, поэтому не возвращаем endpoint через 20-45 сек.
                # Ранее успешному proxy оставляем умеренно более короткий cooldown,
                # но всё равно значительно длиннее transport-timeout/error.
                if known_good:
                    cooldown = [120, 300, 600, 900][min(streak - 1, 3)]
                else:
                    cooldown = [600, 900, 1200, 1800][min(streak - 1, 3)]
                host_cooldown = 0
            elif result == 'proxy_timeout':
                if known_good:
                    cooldown = [20, 45, 120, 300][min(streak - 1, 3)]
                else:
                    cooldown = [45, 90, 180, 300][min(streak - 1, 3)]
                host_cooldown = 0
            elif result == 'proxy_error':
                if known_good:
                    cooldown = [15, 30, 90, 300][min(streak - 1, 3)]
                else:
                    cooldown = [45, 90, 180, 300][min(streak - 1, 3)]
                host_cooldown = 0
            else:
                if known_good:
                    cooldown = [30, 90, 180, 300][min(streak - 1, 3)]
                else:
                    cooldown = [180, 300, 600, 900][min(streak - 1, 3)]
                host_cooldown = 0

            # Если только что умерла fixed-session, на 15 секунд не пробуем другой
            # порт того же exit-IP. В логе :8080 умер, а :1080 того же IP ушёл в probe
            # немедленно и впустую занял worker. После 15 сек. host снова доступен.
            if (
                result in ('proxy_timeout', 'proxy_error')
                and reason and 'fixed session' in str(reason)
            ):
                host_cooldown = max(host_cooldown, 15)

            # Transport/TLS failure означает, что старый warm-quality сигнал уже устарел.
            # 403/429 сюда не относятся: туннель технически исправен, eBay лишь отклонил IP.
            if result in ('proxy_ssl', 'proxy_rejected', 'proxy_timeout', 'proxy_error'):
                self.quality_ok_until.pop(proxy, None)
                self.quality_latency_ms.pop(proxy, None)
                self.quality_bad_until[proxy] = max(
                    self.quality_bad_until.get(proxy, 0),
                    now + PROXY_QUALITY_BAD_TTL,
                )
                self.quality_last_result[proxy] = result

            self.bad_until[proxy] = now + cooldown
            if host_cooldown:
                self.host_bad_until[host] = max(self.host_bad_until.get(host, 0), now + host_cooldown)
            if proxy in self.proxies:
                self.proxies.remove(proxy)

            label = reason or result
            available_now = sum(
                1 for p in self.proxies
                if self.bad_until.get(p, 0) <= now
                and self.host_bad_until.get(_proxy_host(p), 0) <= now
            )
            logging.info(
                f"Proxy {_proxy_log_name(proxy)} cooldown {cooldown} сек. "
                f"Причина: {label}; known_good={known_good}, fail_streak={streak}. "
                f"Осталось {available_now} реально доступных proxy"
            )
            return cooldown


proxy_manager = ProxyManager(PROXY_LIST_URL)

# ============ ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ============
fixed_proxy = None
fixed_profile = None
fixed_session = None
# Auction-refinement может аккуратно воспользоваться текущей рабочей fixed Session,
# но НИКОГДА не одновременно с основным search request. Оба пути сериализованы одним
# lock; auction ждёт только ограниченное окно и затем уходит на reserve IP.
main_fixed_request_lock = threading.Lock()

# V6.17: отдельная репутация reserve-proxy только для auction fetch.
# Основной ProxyManager намеренно не загрязняем: proxy может прекрасно работать на
# общей выдаче и одновременно получать challenge на item/search auction URL.
auction_proxy_state_lock = threading.Lock()
auction_proxy_bad_until = {}
auction_proxy_host_bad_until = {}
auction_proxy_fail_streak = {}
auction_proxy_success_at = {}


def _auction_proxy_soft_host_penalty(proxy, reason, seconds=EBAY_CROSS_SOFT_PENALTY):
    """Мягко понижает host для auction после 403/429 main-monitor, не создавая hard blacklist."""
    if not proxy or reason not in ('blocked', 'rate_limited'):
        return
    host = _proxy_host(proxy)
    if not host:
        return
    now = time.time()
    with auction_proxy_state_lock:
        auction_proxy_host_bad_until[host] = max(
            auction_proxy_host_bad_until.get(host, 0),
            now + max(1, int(seconds)),
        )


def close_session(session):
    if session is None:
        return
    try:
        session.close()
    except Exception:
        pass

# ============ БАЗА ДАННЫХ ============
def get_db_connection(application_name='ebay_uk_bot'):
    """Короткие DB-timeout без изменений в панели Aiven."""
    return psycopg2.connect(
        DATABASE_URL,
        connect_timeout=DB_CONNECT_TIMEOUT,
        application_name=application_name,
        options=(
            f"-c statement_timeout={DB_STATEMENT_TIMEOUT_MS} "
            f"-c lock_timeout={DB_LOCK_TIMEOUT_MS}"
        ),
    )


def init_db():
    with get_db_connection('ebay_uk_bot_init') as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS seen_items (item_id TEXT PRIMARY KEY, first_seen TIMESTAMP)")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bot_state (
                    state_key TEXT PRIMARY KEY,
                    state_value TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS auction_reminders (
                    item_id TEXT PRIMARY KEY,
                    url TEXT NOT NULL,
                    title TEXT NOT NULL,
                    end_time_utc TIMESTAMPTZ NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    reminder_60_sent BOOLEAN NOT NULL DEFAULT FALSE,
                    reminder_30_sent BOOLEAN NOT NULL DEFAULT FALSE,
                    reminder_10_sent BOOLEAN NOT NULL DEFAULT FALSE,
                    reminder_5_sent BOOLEAN NOT NULL DEFAULT FALSE,
                    last_status_check TIMESTAMPTZ NULL
                )
                """
            )
            cur.execute(
                "ALTER TABLE auction_reminders "
                "ADD COLUMN IF NOT EXISTS last_status_check TIMESTAMPTZ NULL"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_auction_reminders_end_time "
                "ON auction_reminders (end_time_utc)"
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS auction_pending (
                    item_id TEXT PRIMARY KEY,
                    url TEXT NOT NULL,
                    title TEXT NOT NULL,
                    observed_at_utc TIMESTAMPTZ NOT NULL,
                    end_earliest_utc TIMESTAMPTZ NOT NULL,
                    end_latest_utc TIMESTAMPTZ NOT NULL,
                    remaining_text TEXT NOT NULL DEFAULT '',
                    clock_text TEXT NOT NULL DEFAULT '',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    last_check TIMESTAMPTZ NULL,
                    next_check TIMESTAMPTZ NOT NULL
                )
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_auction_pending_next_check "
                "ON auction_pending (next_check)"
            )
            cur.execute(
                "ALTER TABLE auction_pending "
                "ADD COLUMN IF NOT EXISTS status_message_id BIGINT NULL"
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS auction_link_queue (
                    queue_key TEXT PRIMARY KEY,
                    item_id TEXT NULL,
                    url TEXT NOT NULL,
                    status_message_id BIGINT NULL,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT NOT NULL DEFAULT '',
                    next_attempt TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_auction_link_queue_next_attempt "
                "ON auction_link_queue (next_attempt, created_at)"
            )
        conn.commit()


def get_bot_state(key, default=None):
    with get_db_connection('ebay_uk_bot_state_read') as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT state_value FROM bot_state WHERE state_key=%s", (key,))
            row = cur.fetchone()
            return row[0] if row else default


def set_bot_state(key, value):
    with get_db_connection('ebay_uk_bot_state_write') as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bot_state (state_key, state_value, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (state_key) DO UPDATE
                SET state_value=EXCLUDED.state_value, updated_at=NOW()
                """,
                (key, str(value)),
            )
        conn.commit()


def initialize_seen_count_cache():
    """Один раз получает точное количество seen_items при старте leader-копии."""
    global seen_count_cache
    try:
        with get_db_connection('ebay_uk_seen_count_init') as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM seen_items")
                count = int(cur.fetchone()[0])
        with seen_count_lock:
            seen_count_cache = count
        logging.info(f"📚 В базе seen_items: {count} товаров")
        return count
    except Exception as e:
        # Сбой счётчика не должен останавливать мониторинг. Все основные операции БД
        # продолжают работать как раньше; просто временно не показываем число.
        logging.warning(f"Не удалось получить количество seen_items: {e}")
        with seen_count_lock:
            seen_count_cache = None
        return None


def get_seen_count_cached():
    with seen_count_lock:
        return seen_count_cache


def _increment_seen_count_cache(delta):
    global seen_count_cache
    if not delta:
        return get_seen_count_cached()
    with seen_count_lock:
        if seen_count_cache is None:
            return None
        seen_count_cache += int(delta)
        return seen_count_cache


def claim_new_seen_ids(item_ids):
    """Атомарно резервирует только действительно новые eBay item_id.

    PRIMARY KEY + ON CONFLICT DO NOTHING защищает от повторной отправки даже если
    две Render-копии на несколько секунд пересеклись во время deploy.
    """
    unique_ids = list(dict.fromkeys(str(x) for x in item_ids if x))
    if not unique_ids:
        return set()
    with get_db_connection('ebay_uk_seen_claim') as conn:
        with conn.cursor() as cur:
            data = [(item_id, datetime.now(timezone.utc).replace(tzinfo=None)) for item_id in unique_ids]
            returned = execute_values(
                cur,
                "INSERT INTO seen_items (item_id, first_seen) VALUES %s "
                "ON CONFLICT (item_id) DO NOTHING RETURNING item_id",
                data,
                fetch=True,
            )
        conn.commit()
    claimed = {row[0] for row in returned}
    total = _increment_seen_count_cache(len(claimed))
    if claimed and total is not None:
        logging.info(f"📚 В базе seen_items: {total} товаров (+{len(claimed)})")
    return claimed


def add_seen_ids_batch(item_ids):
    # Для начального snapshot: помечаем текущую выдачу увиденной, но ничего не отправляем.
    claim_new_seen_ids(item_ids)


def is_db_empty():
    cached = get_seen_count_cached()
    if cached is not None:
        return cached == 0
    with get_db_connection('ebay_uk_db_empty') as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT NOT EXISTS (SELECT 1 FROM seen_items LIMIT 1)")
            return bool(cur.fetchone()[0])


# ============ SINGLE LEADER ДЛЯ RENDER ============
def try_acquire_leader_lock():
    """Возвращает отдельное соединение, которое держит PostgreSQL advisory lock."""
    conn = None
    try:
        conn = get_db_connection('ebay_uk_leader_lock')
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (LEADER_LOCK_KEY,))
            acquired = bool(cur.fetchone()[0])
        if acquired:
            return conn
    except Exception as e:
        logging.warning(f"Не удалось проверить leader-lock: {e}")
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
    return None


# ============ АУКЦИОНЫ: БД И ВРЕМЯ ============
def _ensure_aware_utc(dt):
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def format_kyiv_datetime(dt):
    dt = _ensure_aware_utc(dt).astimezone(KYIV_TZ)
    return dt.strftime("%d.%m.%Y в %H:%M")


def format_remaining(seconds, with_seconds=True):
    seconds = max(0, int(seconds))
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days} дн.")
    if hours or days:
        parts.append(f"{hours} ч.")
    parts.append(f"{minutes} мин.")
    if with_seconds and not days:
        parts.append(f"{secs} сек.")
    return " ".join(parts)


def _initial_reminder_flags(end_time_utc):
    # V6.17: новые exact-аукционы начинают с НЕотправленных порогов. Сам scheduler
    # уже умеет безопасный catch-up без пачки старых сообщений: если exact время
    # удалось уточнить, например, за 58 минут до конца, он сразу отправит актуальное
    # напоминание "за 1 час"; если лот добавлен за 22 минуты — только "за 30 минут";
    # за 8 минут — только "за 10 минут". После отправки все более старые due-пороги
    # помечаются закрытыми. Это устраняет потерю 60-минутного reminder при pending->exact.
    _ = end_time_utc  # параметр оставляем для совместимости вызовов
    return {60: False, 30: False, 10: False, 5: False}


def save_auction_reminder(item_id, url, title, end_time_utc):
    end_time_utc = _ensure_aware_utc(end_time_utc)
    flags = _initial_reminder_flags(end_time_utc)
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO auction_reminders (
                    item_id, url, title, end_time_utc,
                    reminder_60_sent, reminder_30_sent, reminder_10_sent, reminder_5_sent
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (item_id) DO UPDATE SET
                    url = EXCLUDED.url,
                    title = EXCLUDED.title,
                    end_time_utc = EXCLUDED.end_time_utc,
                    updated_at = NOW(),
                    reminder_60_sent = CASE
                        WHEN auction_reminders.end_time_utc = EXCLUDED.end_time_utc
                        THEN auction_reminders.reminder_60_sent
                        ELSE EXCLUDED.reminder_60_sent END,
                    reminder_30_sent = CASE
                        WHEN auction_reminders.end_time_utc = EXCLUDED.end_time_utc
                        THEN auction_reminders.reminder_30_sent
                        ELSE EXCLUDED.reminder_30_sent END,
                    reminder_10_sent = CASE
                        WHEN auction_reminders.end_time_utc = EXCLUDED.end_time_utc
                        THEN auction_reminders.reminder_10_sent
                        ELSE EXCLUDED.reminder_10_sent END,
                    reminder_5_sent = CASE
                        WHEN auction_reminders.end_time_utc = EXCLUDED.end_time_utc
                        THEN auction_reminders.reminder_5_sent
                        ELSE EXCLUDED.reminder_5_sent END
                """,
                (
                    item_id, url, title, end_time_utc,
                    flags[60], flags[30], flags[10], flags[5],
                ),
            )
            # Exact schedule supersedes any earlier coarse/pending observation.
            cur.execute("DELETE FROM auction_pending WHERE item_id = %s", (item_id,))
        conn.commit()
    wake_auction_workers()


def get_exact_auction_by_id(item_id):
    with get_db_connection('ebay_uk_auction_exact_get') as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT item_id, url, title, end_time_utc FROM auction_reminders WHERE item_id=%s",
                (item_id,),
            )
            return cur.fetchone()


def list_active_auctions(limit=20):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT item_id, url, title, end_time_utc
                FROM auction_reminders
                WHERE end_time_utc > NOW()
                ORDER BY end_time_utc ASC
                LIMIT %s
                """,
                (limit,),
            )
            return cur.fetchall()


def delete_auction_reminder(item_id):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM auction_reminders WHERE item_id = %s RETURNING item_id", (item_id,))
            deleted = cur.fetchone() is not None
        conn.commit()
    wake_auction_workers()
    return deleted



def _auction_queue_key(url, item_id=None):
    item_id = str(item_id or '').strip()
    if item_id:
        return f"item:{item_id}"
    digest = hashlib.sha256(str(url or '').encode('utf-8', 'ignore')).hexdigest()[:32]
    return f"url:{digest}"


def enqueue_auction_link(url):
    """Надёжно ставит пользовательскую auction-ссылку в PostgreSQL-очередь.

    Повторная отправка того же ItemID не создаёт второй job. Уже существующий job
    просто будится немедленно, при этом его Telegram status_message_id сохраняется.
    """
    item_id = extract_ebay_item_id_any(url or '')
    canonical = f"https://www.ebay.co.uk/itm/{item_id}" if item_id else str(url or '')
    key = _auction_queue_key(canonical, item_id)
    with get_db_connection('ebay_uk_auction_link_enqueue') as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status_message_id FROM auction_link_queue WHERE queue_key=%s FOR UPDATE",
                (key,),
            )
            row = cur.fetchone()
            existing_message_id = row[0] if row else None
            if row:
                cur.execute(
                    """
                    UPDATE auction_link_queue
                    SET item_id=%s, url=%s, next_attempt=NOW(), updated_at=NOW()
                    WHERE queue_key=%s
                    """,
                    (item_id, canonical, key),
                )
                is_new = False
            else:
                cur.execute(
                    """
                    INSERT INTO auction_link_queue (queue_key, item_id, url, next_attempt)
                    VALUES (%s,%s,%s,NOW())
                    """,
                    (key, item_id, canonical),
                )
                is_new = True
        conn.commit()
    auction_link_wakeup_event.set()
    return key, item_id, canonical, existing_message_id, is_new


def set_auction_link_status_message(queue_key, message_id):
    if not queue_key or not message_id:
        return False
    with get_db_connection('ebay_uk_auction_link_message') as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE auction_link_queue
                SET status_message_id=%s, updated_at=NOW()
                WHERE queue_key=%s
                """,
                (int(message_id), queue_key),
            )
            changed = cur.rowcount > 0
        conn.commit()
    return changed


def get_next_auction_link_job_state():
    """Возвращает один due-job либо сколько секунд безопасно ждать до следующего.

    V6.15: раньше пустая очередь опрашивала Aiven каждые ~5 сек. отдельным TLS-
    соединением. Теперь одним SELECT смотрим ближайший next_attempt и спим до него;
    новая ссылка всё равно мгновенно будит worker через auction_link_wakeup_event.
    Это резко уменьшает connection churn, не замедляя пользовательские ссылки.
    """
    with get_db_connection('ebay_uk_auction_link_due') as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT queue_key, item_id, url, status_message_id, attempt_count, created_at,
                       next_attempt,
                       GREATEST(EXTRACT(EPOCH FROM (next_attempt - NOW())), 0)
                FROM auction_link_queue
                ORDER BY next_attempt ASC, created_at ASC
                LIMIT 1
                """
            )
            row = cur.fetchone()

    if not row:
        return None, float(AUCTION_LINK_WORKER_IDLE)

    wait_seconds = max(0.0, float(row[7] or 0.0))
    if wait_seconds > 0.25:
        return None, min(wait_seconds, float(AUCTION_LINK_WORKER_IDLE))

    return row[:6], 0.0


def list_queued_auction_links(limit=20):
    with get_db_connection('ebay_uk_auction_link_list') as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT item_id, url, created_at, next_attempt
                FROM auction_link_queue
                WHERE item_id IS NOT NULL
                ORDER BY created_at ASC
                LIMIT %s
                """,
                (max(1, int(limit)),),
            )
            return cur.fetchall()


def postpone_auction_link_job(queue_key, attempt_count, reason='temporary_unavailable'):
    attempt_count = max(0, int(attempt_count or 0)) + 1
    # 15 -> 30 -> 60 сек., затем держим максимум 60. Небольшой jitter не создаёт
    # синхронный ритм и не мешает следующей ссылке в очереди получить свой шанс.
    delay = min(AUCTION_LINK_RETRY_MAX, AUCTION_LINK_RETRY_BASE * (2 ** min(attempt_count - 1, 2)))
    delay = max(AUCTION_LINK_RETRY_BASE, int(delay + random.uniform(0, min(5, delay * 0.15))))
    with get_db_connection('ebay_uk_auction_link_retry') as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE auction_link_queue
                SET attempt_count=%s,
                    last_error=%s,
                    next_attempt=NOW() + (%s * INTERVAL '1 second'),
                    updated_at=NOW()
                WHERE queue_key=%s
                """,
                (attempt_count, str(reason or '')[:300], delay, queue_key),
            )
        conn.commit()
    logging.info(
        f"🟡 Auction link остаётся в очереди: key={queue_key}, attempt={attempt_count}, "
        f"повтор примерно через {delay} сек.; reason={reason}"
    )
    return delay


def delete_auction_link_job(queue_key):
    with get_db_connection('ebay_uk_auction_link_delete') as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM auction_link_queue WHERE queue_key=%s RETURNING queue_key", (queue_key,))
            deleted = cur.fetchone() is not None
        conn.commit()
    return deleted


def transfer_auction_link_status_to_pending(item_id):
    """Страховочная DB-передача message_id перед удалением queue-row."""
    if not item_id:
        return False
    with get_db_connection('ebay_uk_auction_link_message_transfer') as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE auction_pending AS p
                SET status_message_id = COALESCE(p.status_message_id, q.status_message_id),
                    updated_at = NOW()
                FROM auction_link_queue AS q
                WHERE p.item_id=%s
                  AND q.item_id=p.item_id
                  AND q.status_message_id IS NOT NULL
                """,
                (str(item_id),),
            )
            changed = cur.rowcount > 0
        conn.commit()
    return changed


def get_auction_link_status_message(item_id):
    if not item_id:
        return None
    with get_db_connection('ebay_uk_auction_link_message_get') as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status_message_id FROM auction_link_queue WHERE item_id=%s ORDER BY created_at DESC LIMIT 1",
                (str(item_id),),
            )
            row = cur.fetchone()
            return row[0] if row else None


def get_pending_status_message(item_id):
    with get_db_connection('ebay_uk_auction_pending_message_get') as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status_message_id FROM auction_pending WHERE item_id=%s", (str(item_id),))
            row = cur.fetchone()
            return row[0] if row else None


def set_pending_status_message(item_id, message_id):
    if not item_id or not message_id:
        return False
    with get_db_connection('ebay_uk_auction_pending_message_set') as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE auction_pending SET status_message_id=%s, updated_at=NOW() WHERE item_id=%s",
                (int(message_id), str(item_id)),
            )
            changed = cur.rowcount > 0
        conn.commit()
    return changed


def _pending_next_check(now_utc, end_latest_utc, window_seconds):
    now_utc = _ensure_aware_utc(now_utc)
    end_latest_utc = _ensure_aware_utc(end_latest_utc)
    remaining_latest = (end_latest_utc - now_utc).total_seconds()
    # V6.17: если eBay уже показывает минуты и до конца <=2ч, это критическое окно
    # для reminder 60/30/10/5. Уточняем примерно раз в 2 минуты, но всё ещё только
    # lightweight exact-item search и под main-session lock.
    if window_seconds <= 120:
        if remaining_latest <= 2 * 3600:
            return now_utc + timedelta(seconds=AUCTION_PENDING_CLOSE_RETRY)
        return now_utc + timedelta(seconds=max(90, min(AUCTION_PENDING_RETRY, 180)))
    # Как только даже верхняя граница меньше 24ч, проверяем без агрессивного burst.
    if remaining_latest <= 24 * 3600:
        return now_utc + timedelta(seconds=AUCTION_PENDING_RETRY)
    # До этого не мучаем eBay: будим pending около 23ч30м до самого позднего
    # возможного конца. Тогда реальный лот гарантированно уже находится <24ч.
    target = end_latest_utc - timedelta(seconds=AUCTION_PENDING_REFINE_TARGET)
    minimum = now_utc + timedelta(seconds=AUCTION_PENDING_MIN_DELAY)
    return max(minimum, target)


def save_pending_auction(
    item_id,
    url,
    title,
    observed_at_utc,
    low_seconds,
    high_seconds,
    remaining_text='',
    clock_text='',
):
    """Сохраняет НЕ точное время как безопасное окно, не включая reminders.

    Например ``4d 14h`` означает [4d14h, 4d15h). Мы сознательно не выбираем
    случайную минуту из этого часа. Запись переживает deploy/restart в PostgreSQL и
    позже автоматически уточняется lightweight search-card запросом.
    """
    observed = _ensure_aware_utc(observed_at_utc or datetime.now(timezone.utc))
    low_seconds = max(0.0, float(low_seconds))
    high_seconds = max(low_seconds + 1.0, float(high_seconds))
    raw_window_seconds = max(1.0, high_seconds - low_seconds)
    margin = float(
        AUCTION_PENDING_MINUTE_MARGIN
        if raw_window_seconds <= 60.0
        else AUCTION_PENDING_WINDOW_MARGIN
    )
    new_earliest = observed + timedelta(seconds=max(0.0, low_seconds - margin))
    new_latest = observed + timedelta(seconds=high_seconds + margin)
    window_seconds = max(1.0, new_latest.timestamp() - new_earliest.timestamp())
    canonical_url = f"https://www.ebay.co.uk/itm/{item_id}"
    url = canonical_url if item_id else url

    with get_db_connection('ebay_uk_auction_pending_save') as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT end_earliest_utc, end_latest_utc FROM auction_pending "
                "WHERE item_id=%s FOR UPDATE",
                (item_id,),
            )
            old = cur.fetchone()
            earliest, latest = new_earliest, new_latest
            if old:
                old_earliest = _ensure_aware_utc(old[0])
                old_latest = _ensure_aware_utc(old[1])
                # Повторные countdown-наблюдения сужают окно. Если окна не пересекаются,
                # eBay мог изменить окончание — тогда безопаснее заменить старое новым.
                overlap_earliest = max(old_earliest, new_earliest)
                overlap_latest = min(old_latest, new_latest)
                if overlap_earliest < overlap_latest:
                    earliest, latest = overlap_earliest, overlap_latest
            window_seconds = max(1.0, (latest - earliest).total_seconds())
            next_check = _pending_next_check(observed, latest, window_seconds)
            cur.execute(
                """
                INSERT INTO auction_pending (
                    item_id, url, title, observed_at_utc,
                    end_earliest_utc, end_latest_utc,
                    remaining_text, clock_text, last_check, next_check
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s)
                ON CONFLICT (item_id) DO UPDATE SET
                    url=EXCLUDED.url,
                    title=EXCLUDED.title,
                    observed_at_utc=EXCLUDED.observed_at_utc,
                    end_earliest_utc=EXCLUDED.end_earliest_utc,
                    end_latest_utc=EXCLUDED.end_latest_utc,
                    remaining_text=EXCLUDED.remaining_text,
                    clock_text=EXCLUDED.clock_text,
                    last_check=NOW(),
                    next_check=EXCLUDED.next_check,
                    updated_at=NOW()
                """,
                (
                    item_id, url, title or f'eBay item {item_id}', observed,
                    earliest, latest,
                    str(remaining_text or ''), str(clock_text or ''), next_check,
                ),
            )
        conn.commit()
    wake_auction_workers()
    return earliest, latest, next_check


def _pending_window_single_minute(earliest, latest):
    """Возвращает UTC minute, только если ВСЁ сохранённое окно лежит в одной минуте."""
    earliest = _ensure_aware_utc(earliest)
    latest = _ensure_aware_utc(latest)
    if latest <= earliest:
        return None
    last_possible = latest - timedelta(microseconds=1)
    first_minute = earliest.replace(second=0, microsecond=0)
    last_minute = last_possible.replace(second=0, microsecond=0)
    return first_minute if first_minute == last_minute else None


def list_pending_auctions(limit=20):
    with get_db_connection('ebay_uk_auction_pending_list') as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT item_id, url, title, end_earliest_utc, end_latest_utc,
                       remaining_text, clock_text, next_check
                FROM auction_pending
                WHERE end_latest_utc > NOW() - INTERVAL '30 minutes'
                ORDER BY end_earliest_utc ASC
                LIMIT %s
                """,
                (limit,),
            )
            return cur.fetchall()


def get_due_pending_auctions(limit=1):
    with get_db_connection('ebay_uk_auction_pending_due') as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT item_id, url, title, end_earliest_utc, end_latest_utc,
                       remaining_text, clock_text, next_check
                FROM auction_pending
                WHERE next_check <= NOW()
                  AND end_latest_utc > NOW() - INTERVAL '30 minutes'
                ORDER BY next_check ASC
                LIMIT %s
                """,
                (limit,),
            )
            return cur.fetchall()


def postpone_pending_auction(item_id, delay_seconds=None):
    delay_seconds = int(delay_seconds or AUCTION_PENDING_RETRY)
    with get_db_connection('ebay_uk_auction_pending_retry') as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE auction_pending
                SET last_check=NOW(), next_check=NOW() + (%s * INTERVAL '1 second'), updated_at=NOW()
                WHERE item_id=%s
                """,
                (delay_seconds, item_id),
            )
        conn.commit()


def delete_pending_auction(item_id, wake=True):
    with get_db_connection('ebay_uk_auction_pending_delete') as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM auction_pending WHERE item_id=%s RETURNING item_id", (item_id,))
            deleted = cur.fetchone() is not None
        conn.commit()
    if wake:
        wake_auction_workers()
    return deleted


def delete_auction_any(item_id):
    """Удаляет exact, pending и ещё не проверенный queue-job одного ItemID."""
    with get_db_connection('ebay_uk_auction_delete_any') as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM auction_reminders WHERE item_id=%s RETURNING item_id", (item_id,))
            exact = cur.fetchone() is not None
            cur.execute("DELETE FROM auction_pending WHERE item_id=%s RETURNING item_id", (item_id,))
            pending = cur.fetchone() is not None
            cur.execute("DELETE FROM auction_link_queue WHERE item_id=%s RETURNING queue_key", (item_id,))
            queued = cur.fetchone() is not None
        conn.commit()
    wake_auction_workers()
    auction_link_wakeup_event.set()
    return exact or pending or queued


def delete_all_auctions():
    """Удаляет exact, pending и waiting queue одной транзакцией после подтверждения."""
    status_message_ids = []
    with get_db_connection('ebay_uk_auction_delete_all') as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM auction_reminders RETURNING item_id")
            exact = [row[0] for row in cur.fetchall()]
            cur.execute("DELETE FROM auction_pending RETURNING item_id, status_message_id")
            pending_rows = cur.fetchall()
            pending = [row[0] for row in pending_rows]
            status_message_ids.extend(row[1] for row in pending_rows if row[1])
            cur.execute("DELETE FROM auction_link_queue RETURNING queue_key, status_message_id")
            queued_rows = cur.fetchall()
            queued = [row[0] for row in queued_rows]
            status_message_ids.extend(row[1] for row in queued_rows if row[1])
        conn.commit()

    # Удаляем только технические жёлтые status-сообщения. Exact/reminder сообщения
    # не трогаем: у них нет отдельного transient message_id в БД.
    for message_id in dict.fromkeys(status_message_ids):
        try:
            delete_telegram_message(message_id)
        except Exception as e:
            logging.warning(f"Не удалось удалить auction status message_id={message_id}: {e}")

    wake_auction_workers()
    auction_link_wakeup_event.set()
    return len(exact), len(pending), len(queued)


def mark_auction_status_checked(item_id):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE auction_reminders SET last_status_check = NOW(), updated_at = NOW() WHERE item_id = %s",
                (item_id,),
            )
        conn.commit()


def update_verified_auction(item_id, title, new_end_time_utc):
    """Обновляет title/end time и возвращает (exists, changed, old_end, new_end).

    Если eBay сдвинул окончание, заново активируем только те reminders, чей новый
    порог ещё впереди. Уже прошедшие пороги не шлём задним числом.
    """
    new_end_time_utc = _ensure_aware_utc(new_end_time_utc)
    now = datetime.now(timezone.utc)
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT end_time_utc, reminder_60_sent, reminder_30_sent,
                       reminder_10_sent, reminder_5_sent
                FROM auction_reminders WHERE item_id = %s FOR UPDATE
                """,
                (item_id,),
            )
            row = cur.fetchone()
            if not row:
                return False, False, None, new_end_time_utc
            old_end, s60, s30, s10, s5 = row
            old_end = _ensure_aware_utc(old_end)
            changed = old_end.replace(second=0, microsecond=0) != new_end_time_utc.replace(second=0, microsecond=0)
            if changed:
                sent = {60: s60, 30: s30, 10: s10, 5: s5}
                for mins in sent:
                    if (new_end_time_utc - now).total_seconds() > mins * 60:
                        sent[mins] = False
                cur.execute(
                    """
                    UPDATE auction_reminders
                    SET title=%s, end_time_utc=%s, last_status_check=NOW(), updated_at=NOW(),
                        reminder_60_sent=%s, reminder_30_sent=%s,
                        reminder_10_sent=%s, reminder_5_sent=%s
                    WHERE item_id=%s
                    """,
                    (title, new_end_time_utc, sent[60], sent[30], sent[10], sent[5], item_id),
                )
            else:
                cur.execute(
                    "UPDATE auction_reminders SET title=%s, last_status_check=NOW(), updated_at=NOW() WHERE item_id=%s",
                    (title, item_id),
                )
        conn.commit()
    if changed:
        wake_auction_workers()
    return True, changed, old_end, new_end_time_utc


def get_auctions_for_status_check(limit=100):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT item_id, url, title, end_time_utc, last_status_check
                FROM auction_reminders
                WHERE end_time_utc > NOW()
                ORDER BY end_time_utc ASC,
                         COALESCE(last_status_check, TIMESTAMPTZ '1970-01-01') ASC
                LIMIT %s
                """,
                (limit,),
            )
            return cur.fetchall()


def _status_check_interval_seconds(remaining):
    # После 5-минутного reminder запись удаляется, поэтому сверхчастые проверки
    # в последние минуты больше не нужны. До этого момента контроль остаётся лёгким.
    if remaining <= 5 * 60:
        return None
    if remaining <= 10 * 60:
        return 120
    if remaining <= 30 * 60:
        return 300
    if remaining <= 60 * 60:
        return 600
    if remaining <= 6 * 3600:
        return 900
    if remaining <= 24 * 3600:
        return 1800
    return 3600


def _reminder_column(minutes):
    mapping = {
        60: 'reminder_60_sent',
        30: 'reminder_30_sent',
        10: 'reminder_10_sent',
        5: 'reminder_5_sent',
    }
    return mapping[minutes]


def _mark_reminder_sent(conn, item_id, minutes):
    column = _reminder_column(minutes)
    # column приходит только из жёсткого mapping выше, пользовательского SQL здесь нет.
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE auction_reminders SET {column} = TRUE, updated_at = NOW() WHERE item_id = %s",
            (item_id,),
        )
    conn.commit()


def _mark_reminders_sent(conn, item_id, minutes_list):
    minutes_list = [m for m in AUCTION_REMINDER_MINUTES if m in set(minutes_list)]
    if not minutes_list:
        return
    assignments = ", ".join(f"{_reminder_column(m)} = TRUE" for m in minutes_list)
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE auction_reminders SET {assignments}, updated_at = NOW() WHERE item_id = %s",
            (item_id,),
        )
    conn.commit()


def _delete_expired_auctions(conn, now_utc):
    cutoff = now_utc - timedelta(seconds=AUCTION_END_GRACE)
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM auction_reminders WHERE end_time_utc <= %s RETURNING item_id",
            (cutoff,),
        )
        deleted = cur.fetchall()
    conn.commit()
    if deleted:
        logging.info(f"🧹 Удалено завершённых аукционов из reminders: {len(deleted)}")


# ============ TELEGRAM ============
def _telegram_post(method, payload, timeout=10, max_attempts=3):
    """Аккуратные retry без агрессивного спама Telegram.

    Повторяем только случаи, где повтор относительно безопасен: явный 429/5xx и
    ConnectTimeout (соединение не установлено). При ReadTimeout/ConnectionError ответ
    мог потеряться уже ПОСЛЕ принятия сообщения Telegram, поэтому автоматический
    немедленный дубль не делаем.
    """
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    attempts = max(1, int(max_attempts))
    for attempt in range(1, attempts + 1):
        try:
            r = requests.post(url, json=payload, timeout=timeout)
        except requests.exceptions.ConnectTimeout as e:
            logging.warning(f"Telegram connect timeout ({method}), попытка {attempt}/{attempts}: {e}")
            if attempt < attempts:
                time.sleep(min(2.0, 0.5 * attempt))
                continue
            return None
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
            logging.error(
                f"Telegram {method}: неоднозначная сетевая ошибка, без немедленного retry "
                f"во избежание дубля: {e}"
            )
            return None
        except Exception as e:
            logging.error(f"Telegram {method}: ошибка: {e}")
            return None

        if r.status_code == 200:
            try:
                return r.json()
            except Exception:
                return {'ok': True}

        if r.status_code == 429 and attempt < attempts:
            retry_after = 1
            try:
                retry_after = int((r.json().get('parameters') or {}).get('retry_after') or 1)
            except Exception:
                pass
            wait = max(1, min(retry_after, 15))
            logging.warning(f"Telegram 429, повтор через {wait} сек. ({attempt}/{attempts})")
            time.sleep(wait)
            continue

        if 500 <= r.status_code <= 599 and attempt < attempts:
            wait = min(3.0, float(attempt))
            logging.warning(f"Telegram HTTP {r.status_code}, повтор через {wait:.1f} сек.")
            time.sleep(wait)
            continue

        logging.error(f"Ошибка Telegram {method}: HTTP {r.status_code}: {r.text[:500]}")
        return None
    return None


def _apply_link_preview_payload(payload, disable_preview=False, preview_url=None, preview_small=True):
    if preview_url and not disable_preview:
        payload['link_preview_options'] = {
            'is_disabled': False,
            'url': str(preview_url),
            'prefer_small_media': bool(preview_small),
            'show_above_text': False,
        }
    else:
        payload['disable_web_page_preview'] = bool(disable_preview)


def send_telegram_message(
    message,
    parse_mode='HTML',
    reply_markup=None,
    disable_preview=False,
    preview_url=None,
    preview_small=True,
    return_message_id=False,
):
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message,
        'parse_mode': parse_mode,
    }
    _apply_link_preview_payload(payload, disable_preview, preview_url, preview_small)
    if reply_markup is not None:
        payload['reply_markup'] = reply_markup
    result = _telegram_post('sendMessage', payload, timeout=10, max_attempts=3)
    ok = bool(result and result.get('ok', True))
    if return_message_id:
        if not ok:
            return None
        try:
            return int((result.get('result') or {}).get('message_id'))
        except Exception:
            return None
    return ok


def edit_telegram_message(
    message_id,
    message,
    parse_mode='HTML',
    reply_markup=None,
    disable_preview=False,
    preview_url=None,
    preview_small=True,
):
    """Безопасно заменяет собственное status-сообщение бота новым содержимым."""
    if not message_id:
        return False
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'message_id': int(message_id),
        'text': message,
        'parse_mode': parse_mode,
    }
    _apply_link_preview_payload(payload, disable_preview, preview_url, preview_small)
    if reply_markup is not None:
        payload['reply_markup'] = reply_markup
    result = _telegram_post('editMessageText', payload, timeout=10, max_attempts=2)
    return bool(result and result.get('ok', True))


def delete_telegram_message(message_id):
    if not message_id:
        return False
    result = _telegram_post(
        'deleteMessage',
        {'chat_id': TELEGRAM_CHAT_ID, 'message_id': int(message_id)},
        timeout=8,
        max_attempts=2,
    )
    return bool(result and result.get('ok', True))


def publish_auction_status(
    item_id,
    message,
    reply_markup=None,
    disable_preview=False,
    preview_url=None,
    preview_small=True,
    existing_message_id=None,
    persist_pending=False,
):
    """Обновляет одно status-сообщение вместо накопления нескольких в чате.

    Сначала пытаемся отредактировать уже показанное жёлтое сообщение. Если Telegram
    не позволяет edit, отправляем новое и только после успешной отправки удаляем старое.
    """
    old_id = existing_message_id
    if not old_id:
        try:
            old_id = get_pending_status_message(item_id) or get_auction_link_status_message(item_id)
        except Exception as e:
            logging.warning(f"Не удалось прочитать status_message_id auction {item_id}: {e}")
            old_id = None

    if old_id and edit_telegram_message(
        old_id,
        message,
        reply_markup=reply_markup,
        disable_preview=disable_preview,
        preview_url=preview_url,
        preview_small=preview_small,
    ):
        if persist_pending:
            try:
                set_pending_status_message(item_id, old_id)
            except Exception as e:
                logging.warning(f"Не удалось сохранить pending status_message_id={old_id}: {e}")
        return True, old_id

    new_id = send_telegram_message(
        message,
        reply_markup=reply_markup,
        disable_preview=disable_preview,
        preview_url=preview_url,
        preview_small=preview_small,
        return_message_id=True,
    )
    if not new_id:
        return False, None
    if old_id and old_id != new_id:
        delete_telegram_message(old_id)
    if persist_pending:
        try:
            set_pending_status_message(item_id, new_id)
        except Exception as e:
            logging.warning(f"Не удалось сохранить pending status_message_id={new_id}: {e}")
    return True, new_id


def answer_callback_query(callback_query_id, text=None, show_alert=False):
    if not callback_query_id:
        return False
    payload = {'callback_query_id': callback_query_id, 'show_alert': bool(show_alert)}
    if text:
        payload['text'] = str(text)[:200]
    result = _telegram_post('answerCallbackQuery', payload, timeout=8, max_attempts=2)
    return bool(result and result.get('ok', True))


def edit_message_reply_markup(message_id, reply_markup=None):
    if not message_id:
        return False
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'message_id': message_id,
        'reply_markup': reply_markup or {'inline_keyboard': []},
    }
    result = _telegram_post('editMessageReplyMarkup', payload, timeout=8, max_attempts=2)
    return bool(result and result.get('ok', True))


def auction_message_keyboard(item_id, url, include_list=False):
    rows = [[{'text': '🔗 Открыть eBay', 'url': url}]]
    bottom = [{'text': '❌ Удалить', 'callback_data': f'aucdel:{item_id}'}]
    if include_list:
        bottom.append({'text': '📋 Все аукционы', 'callback_data': 'auclist'})
    rows.append(bottom)
    return {'inline_keyboard': rows}


def auction_list_only_keyboard():
    return {'inline_keyboard': [[{'text': '📋 Все аукционы', 'callback_data': 'auclist'}]]}


def auction_queue_keyboard(url):
    return {'inline_keyboard': [[{'text': '🔗 Открыть eBay', 'url': url}]]}


def auction_open_list_keyboard(url):
    # Для финального 5-минутного reminder запись после успешной отправки удаляется,
    # поэтому кнопка удаления уже не нужна.
    return {'inline_keyboard': [[{'text': '🔗 Открыть eBay', 'url': url}]]}

def connection_watchdog():
    """Шлёт ровно одно предупреждение на один непрерывный outage >= 10 минут."""
    global connection_alert_sent
    logging.info(
        f"🩺 Watchdog eBay запущен: предупреждение после {CONNECTION_ALERT_AFTER // 60} мин без успеха"
    )
    while True:
        try:
            if not is_paused:
                elapsed, already_sent, had_success = _connection_outage_snapshot()
                if elapsed >= CONNECTION_ALERT_AFTER and not already_sent:
                    # Ставим флаг ДО Telegram-запроса, чтобы два прохода watchdog
                    # никогда не отправили дубль. Если Telegram не принял сообщение,
                    # флаг вернём назад и попробуем снова на следующем проходе.
                    should_send = False
                    with connection_state_lock:
                        if not connection_alert_sent:
                            connection_alert_sent = True
                            should_send = True
                    if not should_send:
                        time.sleep(CONNECTION_WATCHDOG_INTERVAL)
                        continue

                    minutes = max(10, int(elapsed // 60))
                    if had_success:
                        status_line = f"⏱ Уже <b>{minutes} мин.</b> нет успешного подключения к eBay UK."
                    else:
                        status_line = f"⏱ Уже <b>{minutes} мин.</b> после запуска нет успешного подключения к eBay UK."

                    message = (
                        "⚠️ <b>eBay UK — проверьте подключение</b> 🇬🇧\n\n"
                        f"{status_line}\n"
                        "🤖 Бот продолжает работать и автоматически искать рабочий proxy.\n\n"
                        "👉 <b>Пожалуйста, проверьте сайт/подключение вручную.</b>\n\n"
                        "Следующее такое предупреждение будет отправлено только если связь "
                        "сначала восстановится, а затем снова пропадёт более чем на 10 минут."
                    )
                    if send_telegram_message(message):
                        logging.warning(
                            f"📨 Отправлено Telegram-предупреждение: eBay недоступен {elapsed:.0f} сек."
                        )
                    else:
                        with connection_state_lock:
                            connection_alert_sent = False
            time.sleep(CONNECTION_WATCHDOG_INTERVAL)
        except Exception as e:
            logging.error(f"Ошибка connection watchdog: {e}", exc_info=True)
            time.sleep(CONNECTION_WATCHDOG_INTERVAL)



# ============ TELEGRAM -> EBAY AUCTION REMINDERS ============
def _is_allowed_ebay_url(url):
    try:
        host = (urlsplit(url).hostname or '').lower().rstrip('.')
    except Exception:
        return False
    allowed = ('ebay.co.uk', 'ebay.com', 'ebay.us')
    return any(host == suffix or host.endswith('.' + suffix) for suffix in allowed)


def extract_ebay_urls(text):
    urls = []
    for raw in re.findall(r'https?://[^\s<>]+', text or '', flags=re.I):
        url = raw.rstrip(').,;]>}\"\'')
        if _is_allowed_ebay_url(url) and url not in urls:
            urls.append(url)
    return urls


def extract_ebay_item_id_any(url, html=None):
    for source in (url or '', html or ''):
        patterns = (
            r'/itm/(?:[^/?#]+/)?(\d{9,19})(?:[/?#]|$)',
            r'[?&](?:item|itemid|item_id)=(\d{9,19})(?:&|$)',
            r'eBay\s+item\s+number\s*:?\s*(\d{9,19})',
            r'"itemId"\s*:\s*"?(\d{9,19})"?',
        )
        for pattern in patterns:
            m = re.search(pattern, source, re.I)
            if m:
                return m.group(1)
    return None


def _auction_proxy_cleanup_locked(now=None):
    now = time.time() if now is None else float(now)
    stale = [p for p, until in auction_proxy_bad_until.items() if until <= now]
    for p in stale:
        auction_proxy_bad_until.pop(p, None)
    stale_hosts = [h for h, until in auction_proxy_host_bad_until.items() if until <= now]
    for h in stale_hosts:
        auction_proxy_host_bad_until.pop(h, None)
    stale_success = [p for p, ts in auction_proxy_success_at.items() if now - ts > AUCTION_PROXY_GOOD_MEMORY]
    for p in stale_success:
        auction_proxy_success_at.pop(p, None)


def _auction_proxy_is_available(proxy, now=None):
    if not proxy:
        return False
    now = time.time() if now is None else float(now)
    host = _proxy_host(proxy)
    with auction_proxy_state_lock:
        _auction_proxy_cleanup_locked(now)
        if auction_proxy_bad_until.get(proxy, 0) > now:
            return False
        if host and auction_proxy_host_bad_until.get(host, 0) > now:
            return False
    return True


def _auction_proxy_blocked_hosts(now=None):
    now = time.time() if now is None else float(now)
    with auction_proxy_state_lock:
        _auction_proxy_cleanup_locked(now)
        return {h for h, until in auction_proxy_host_bad_until.items() if until > now}


def _auction_recent_good_proxies(limit=2, excluded_hosts=None):
    limit = max(0, int(limit))
    if limit <= 0:
        return []
    excluded_hosts = set(excluded_hosts or ())
    now = time.time()
    with auction_proxy_state_lock:
        _auction_proxy_cleanup_locked(now)
        rows = sorted(auction_proxy_success_at.items(), key=lambda kv: kv[1], reverse=True)
        result = []
        used_hosts = set(excluded_hosts)
        for proxy, _ts in rows:
            host = _proxy_host(proxy)
            if not host or host in used_hosts:
                continue
            if auction_proxy_bad_until.get(proxy, 0) > now:
                continue
            if auction_proxy_host_bad_until.get(host, 0) > now:
                continue
            result.append(proxy)
            used_hosts.add(host)
            if len(result) >= limit:
                break
        return result


def _auction_proxy_mark_success(proxy):
    if not proxy:
        return
    host = _proxy_host(proxy)
    now = time.time()
    with auction_proxy_state_lock:
        auction_proxy_bad_until.pop(proxy, None)
        auction_proxy_fail_streak[proxy] = 0
        auction_proxy_success_at[proxy] = now
        # Реальный HTTP success доказывает, что этот exit-host сейчас пригоден для auction.
        if host:
            auction_proxy_host_bad_until.pop(host, None)


def _auction_proxy_mark_failure(proxy, reason):
    """Auction-only cooldown + мягкий cross-surface signal для main monitor."""
    if not proxy or reason in (None, '', 'main_busy', 'main_unavailable', 'auction_cooldown', 'fixed_changed'):
        return 0
    reason = str(reason)
    now = time.time()
    host = _proxy_host(proxy)
    with auction_proxy_state_lock:
        _auction_proxy_cleanup_locked(now)
        streak = int(auction_proxy_fail_streak.get(proxy, 0)) + 1
        auction_proxy_fail_streak[proxy] = streak

        if reason == 'proxy_ssl':
            cooldown = 3600
            host_cooldown = 3600
        elif reason in ('blocked', 'rate_limited'):
            cooldown = (300, 600, 900, 1800)[min(streak - 1, 3)]
            host_cooldown = cooldown
        elif reason == 'proxy_rejected':
            cooldown = (600, 900, 1800)[min(streak - 1, 2)]
            host_cooldown = min(cooldown, 600)
        elif reason in ('proxy_timeout', 'proxy_error'):
            cooldown = (45, 90, 180, 300)[min(streak - 1, 3)]
            host_cooldown = min(cooldown, 90)
        else:
            cooldown = (120, 300, 600)[min(streak - 1, 2)]
            host_cooldown = min(cooldown, 300)

        auction_proxy_bad_until[proxy] = max(auction_proxy_bad_until.get(proxy, 0), now + cooldown)
        if host and host_cooldown:
            auction_proxy_host_bad_until[host] = max(
                auction_proxy_host_bad_until.get(host, 0), now + host_cooldown
            )

    # 403/429 — сигнал именно eBay по exit IP. Не делаем hard-ban main monitor,
    # а лишь временно понижаем этот host: если альтернатив нет, он всё равно сможет вернуться.
    if host and reason in ('blocked', 'rate_limited'):
        proxy_manager.mark_soft_host_penalty(
            host, EBAY_CROSS_SOFT_PENALTY, reason=f'auction {reason}'
        )

    logging.info(
        f"Auction-only cooldown {_proxy_log_name(proxy)}: {cooldown} сек., причина={reason}, "
        f"fail_streak={streak}; основной monitor получает только soft penalty"
    )
    return cooldown


def _auction_transport_result(exc):
    low = str(exc).lower()
    if 'curl: (28)' in low or 'timed out' in low:
        return 'proxy_timeout'
    if 'curl: (60)' in low or 'certificate' in low or 'self signed' in low:
        return 'proxy_ssl'
    if (
        'connect tunnel failed' in low or
        'proxy connect aborted' in low or
        'wrong_version_number' in low or
        'wrong version number' in low or
        re.search(r'connect[^\n]*(?:response )?(?:400|405|500|501|502|503)', low)
    ):
        return 'proxy_rejected'
    return 'proxy_error'


def _classify_auction_response(response, proxy):
    final_url = str(getattr(response, 'url', '') or '')
    status = int(getattr(response, 'status_code', 0) or 0)
    if status not in (200, 404, 410):
        logging.info(f"Auction page через {_proxy_log_name(proxy)}: HTTP {status}, url={final_url[:160]}")
        if status == 403:
            return None, final_url, 'blocked'
        if status == 429:
            return None, final_url, 'rate_limited'
        return None, final_url, 'http_error'

    blocked, reason = _is_ebay_block_page(response)
    if blocked:
        logging.info(f"Auction page через {_proxy_log_name(proxy)}: eBay block/challenge ({reason})")
        return None, final_url, 'blocked'
    if final_url and not _is_allowed_ebay_url(final_url):
        logging.info(f"Auction page через {_proxy_log_name(proxy)}: неожиданный redirect {final_url[:160]}")
        return None, final_url, 'unexpected_redirect'
    final_lower = final_url.lower()
    if ('signin.ebay.' in final_lower or 'ebayisapi.dll?signin' in final_lower or '/signin/' in final_lower):
        logging.info(f"Auction page через {_proxy_log_name(proxy)}: eBay перенаправил на Sign In; этот HTML не используем")
        return None, final_url, 'sign_in'

    logging.info(
        f"✅ Auction page получена через {_proxy_log_name(proxy)}: HTTP {status}, "
        f"bytes={len(response.content or b'')}, url={final_url[:160]}"
    )
    return response.text, final_url, 'success'


def _request_auction_page_once(url, proxy, profile, connect_timeout=None, read_timeout=None):
    """Один reserve-request в собственной Session; возвращает html, final_url, result."""
    session = None
    connect_timeout = AUCTION_FETCH_CONNECT_TIMEOUT if connect_timeout is None else connect_timeout
    read_timeout = AUCTION_FETCH_READ_TIMEOUT if read_timeout is None else read_timeout
    try:
        session = _create_session(proxy, profile, timeout=(connect_timeout, read_timeout))
        response = session.get(
            url,
            timeout=(connect_timeout, read_timeout),
            allow_redirects=True,
        )
        return _classify_auction_response(response, proxy)
    except Exception as e:
        logging.info(f"Auction page через {_proxy_log_name(proxy)} не получена: {e}")
        return None, '', _auction_transport_result(e)
    finally:
        close_session(session)


def _request_auction_via_main_session(url, connect_timeout=None, read_timeout=None, wait_timeout=None):
    """Пробует auction search через РЕАЛЬНУЮ fixed Session основного monitor.

    Никакой новой Session/CONNECT для того же proxy не создаётся. Доступ сериализован
    main_fixed_request_lock, поэтому curl_cffi Session не используется параллельно.
    Возвращает (html, final_url, result, proxy_used).
    """
    connect_timeout = AUCTION_FETCH_CONNECT_TIMEOUT if connect_timeout is None else connect_timeout
    read_timeout = AUCTION_FETCH_READ_TIMEOUT if read_timeout is None else read_timeout
    wait_timeout = AUCTION_MAIN_PROXY_WAIT if wait_timeout is None else max(0.0, float(wait_timeout))

    acquired = main_fixed_request_lock.acquire(timeout=wait_timeout)
    if not acquired:
        current = fixed_proxy
        if current:
            logging.info(
                f"Auction: main fixed Session {current} занята; ждали {wait_timeout:.1f} сек., "
                "переходим к reserve без вмешательства в основной monitor"
            )
        return None, '', 'main_busy', current

    try:
        proxy = fixed_proxy
        profile = fixed_profile
        session = fixed_session
        if not proxy or profile is None or session is None:
            return None, '', 'main_unavailable', proxy
        if not _auction_proxy_is_available(proxy):
            logging.info(f"Auction: main proxy {_proxy_log_name(proxy)} временно в auction-only cooldown")
            return None, '', 'auction_cooldown', proxy

        try:
            logging.info(f"🎯 Auction: используем текущую fixed Session {_proxy_log_name(proxy)}")
            response = session.get(
                url,
                timeout=(connect_timeout, read_timeout),
                allow_redirects=True,
            )
            html, final_url, result = _classify_auction_response(response, proxy)
        except Exception as e:
            logging.info(f"Auction page через main fixed Session {_proxy_log_name(proxy)} не получена: {e}")
            html, final_url, result = None, '', _auction_transport_result(e)

        if result == 'success':
            _auction_proxy_mark_success(proxy)
        elif result not in ('main_busy', 'main_unavailable'):
            _auction_proxy_mark_failure(proxy, result)
        return html, final_url, result, proxy
    finally:
        main_fixed_request_lock.release()


def _canonical_auction_url(url):
    item_id = extract_ebay_item_id_any(url or '')
    if item_id:
        return f"https://www.ebay.co.uk/itm/{item_id}"
    return url


def fetch_auction_pages(
    url,
    max_reserve_proxies=None,
    connect_timeout=None,
    read_timeout=None,
    max_pages=1,
    canonicalize_item=True,
    prefer_current_fixed=False,
    exclude_hosts=None,
):
    """Получает auction/search HTML без полного proxy-discovery.

    V6.17:
    1) lightweight exact-item search сначала использует настоящую уже открытую fixed
       Session основного monitor (под тем же lock), а не новый CONNECT через тот же IP;
    2) reserve-proxy имеют отдельный auction-only cooldown, поэтому один и тот же 403/
       timeout/challenge не повторяется на каждом queue retry;
    3) если пока шёл reserve-fetch основной monitor нашёл НОВЫЙ рабочий proxy, перед
       возвратом retry делается один late-main шанс через новую fixed Session.

    Основной ProxyManager/его cooldown не меняются auction-запросами.
    """
    if not _is_allowed_ebay_url(url):
        return []

    profile = get_preferred_profile()
    if profile is None:
        return []

    if max_reserve_proxies is None:
        max_reserve_proxies = AUCTION_FETCH_MAX_PROXIES
    max_reserve_proxies = max(0, min(int(max_reserve_proxies), 8))
    max_pages = max(1, min(int(max_pages), AUCTION_VERIFY_MAX_PAGES))

    canonical_url = _canonical_auction_url(url) if canonicalize_item else url
    urls_to_try = [canonical_url]
    pages = []
    main_proxy_tried = None

    # Для exact search сначала немного ждём свободное окно между main requests и
    # используем уже живую fixed Session. Это не создаёт второй CONNECT через тот же IP.
    if prefer_current_fixed:
        for candidate_url in urls_to_try:
            html, final_url, result, proxy_used = _request_auction_via_main_session(
                candidate_url,
                connect_timeout=connect_timeout,
                read_timeout=read_timeout,
                wait_timeout=AUCTION_MAIN_PROXY_WAIT,
            )
            main_proxy_tried = proxy_used or main_proxy_tried
            if html:
                pages.append((html, final_url or candidate_url, proxy_used))
                if len(pages) >= max_pages:
                    return pages
                break

    # Reserve не использует текущий fixed host свежей Session: по реальному логу это
    # как раз давало CONNECT aborted/403 при продолжающей работать persistent Session.
    current_fixed_now = fixed_proxy
    excluded_hosts = set(exclude_hosts or ()) | _auction_proxy_blocked_hosts()
    if current_fixed_now:
        excluded_hosts.add(_proxy_host(current_fixed_now))
    if main_proxy_tried:
        excluded_hosts.add(_proxy_host(main_proxy_tried))

    candidates = []
    candidate_hosts = set()

    def add_candidate(candidate):
        if not candidate or not _auction_proxy_is_available(candidate):
            return False
        host = _proxy_host(candidate)
        if not host or host in excluded_hosts or host in candidate_hosts:
            return False
        candidates.append(candidate)
        candidate_hosts.add(host)
        return True

    # Сначала один-два reserve, которые уже реально отдавали auction HTML раньше.
    for candidate in _auction_recent_good_proxies(
        limit=min(2, max_reserve_proxies), excluded_hosts=excluded_hosts
    ):
        add_candidate(candidate)

    need = max(0, max_reserve_proxies - len(candidates))
    if need:
        reserve = proxy_manager.get_candidate_batch(
            need,
            tried_hosts=excluded_hosts | candidate_hosts,
        )
        for candidate in reserve:
            add_candidate(candidate)

    # Если после auction-only cooldown кандидатов мало, один emergency merge расширяет
    # список, но не запускает полный main discovery и не меняет fixed_proxy.
    if len(candidates) < min(2, max_reserve_proxies) and max_reserve_proxies > len(candidates):
        proxy_manager.refresh_proxies(force=True, emergency=True)
        extra_need = max(0, max_reserve_proxies - len(candidates))
        if extra_need:
            extra = proxy_manager.get_candidate_batch(
                extra_need,
                tried_hosts=excluded_hosts | candidate_hosts | _auction_proxy_blocked_hosts(),
            )
            for candidate in extra:
                add_candidate(candidate)

    def worker(proxy):
        last_result = None
        for candidate_url in urls_to_try:
            html, final_url, result = _request_auction_page_once(
                candidate_url,
                proxy,
                profile,
                connect_timeout=connect_timeout,
                read_timeout=read_timeout,
            )
            last_result = result
            if html:
                _auction_proxy_mark_success(proxy)
                return html, final_url or candidate_url, proxy, result
            _auction_proxy_mark_failure(proxy, result)
        return None, None, proxy, last_result

    pending_candidates = list(candidates[:max_reserve_proxies])
    if pending_candidates:
        executor = ThreadPoolExecutor(
            max_workers=min(AUCTION_FETCH_PARALLEL, len(pending_candidates)),
            thread_name_prefix='auction-fetch',
        )
        future_to_proxy = {}

        def submit_next():
            while pending_candidates and len(future_to_proxy) < AUCTION_FETCH_PARALLEL:
                proxy = pending_candidates.pop(0)
                future_to_proxy[executor.submit(worker, proxy)] = proxy

        submit_next()
        try:
            while future_to_proxy and len(pages) < max_pages:
                done, _ = wait(tuple(future_to_proxy.keys()), timeout=1.0, return_when=FIRST_COMPLETED)
                if not done:
                    continue
                for future in done:
                    proxy = future_to_proxy.pop(future, None)
                    try:
                        html, final_url, proxy_used, _result = future.result()
                    except Exception as e:
                        logging.info(f"Auction fetch worker {_proxy_log_name(proxy)} завершился ошибкой: {e}")
                        html, final_url, proxy_used = None, None, proxy
                        _auction_proxy_mark_failure(proxy, 'proxy_error')
                    if html:
                        pages.append((html, final_url, proxy_used))
                        if len(pages) >= max_pages:
                            break
                submit_next()
        finally:
            for future in list(future_to_proxy):
                future.cancel()
            executor.shutdown(wait=False, cancel_futures=True)

    if pages:
        return pages

    # Ключевой race из реального лога: auction attempt мог начаться во время main
    # discovery, а через несколько секунд monitor уже нашёл новый хороший fixed proxy.
    # Старый код этого нового proxy не видел до следующего queue retry. Теперь перед
    # возвратом 'retry' даём РОВНО один шанс новой/current fixed Session.
    if prefer_current_fixed:
        latest_proxy = fixed_proxy
        if latest_proxy and latest_proxy != main_proxy_tried and _auction_proxy_is_available(latest_proxy):
            logging.info(
                f"Auction: появился новый main proxy {_proxy_log_name(latest_proxy)}; "
                "даём один late-main шанс до переноса ссылки на следующий retry"
            )
            for candidate_url in urls_to_try:
                html, final_url, _result, proxy_used = _request_auction_via_main_session(
                    candidate_url,
                    connect_timeout=connect_timeout,
                    read_timeout=read_timeout,
                    wait_timeout=min(4.0, AUCTION_MAIN_PROXY_WAIT),
                )
                if html:
                    pages.append((html, final_url or candidate_url, proxy_used))
                    break

    return pages


def fetch_auction_page(url, max_reserve_proxies=None, connect_timeout=None, read_timeout=None, canonicalize_item=True, prefer_current_fixed=False, exclude_hosts=None):
    """Совместимый wrapper: возвращает первую полученную auction page."""
    pages = fetch_auction_pages(
        url,
        max_reserve_proxies=max_reserve_proxies,
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
        max_pages=1,
        canonicalize_item=canonicalize_item,
        prefer_current_fixed=prefer_current_fixed,
        exclude_hosts=exclude_hosts,
    )
    if not pages:
        return None, None, None
    return pages[0]

def _parse_iso_datetime(value):
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            number = float(value)
            if number > 10**12:
                number /= 1000.0
            return datetime.fromtimestamp(number, tz=timezone.utc)

        text = html_lib.unescape(str(value)).strip().strip('"\'')
        # Декодируем только \uXXXX и escaped slash/quotes, не трогая произвольные байты.
        text = re.sub(
            r'\\u([0-9a-fA-F]{4})',
            lambda m: chr(int(m.group(1), 16)),
            text,
        )
        text = text.replace('\\/', '/').replace('\\"', '"')
        if re.fullmatch(r'\d{10,13}(?:\.\d+)?', text):
            number = float(text)
            if number > 10**12:
                number /= 1000.0
            return datetime.fromtimestamp(number, tz=timezone.utc)

        normalized = text.replace('Z', '+00:00')
        dt = datetime.fromisoformat(normalized)
        return _ensure_aware_utc(dt)
    except Exception:
        return None




def _ebay_decoded_variants(raw_html):
    """Несколько безопасных представлений eBay HTML/embedded JSON.

    Современный View Item может хранить hydration JSON не только как обычный JSON,
    но и HTML-entity / JS-unicode escaped. V6.6 искал ключи только после замены \\" и
    из-за этого мог не увидеть реальный itemEndDate/endTime в полноценной 700+ KB странице.
    """
    if not raw_html:
        return []
    variants = []
    current = str(raw_html)
    for _ in range(4):
        if current not in variants:
            variants.append(current)
        decoded = html_lib.unescape(current)
        decoded = re.sub(r'\\u0022', '"', decoded, flags=re.I)
        decoded = re.sub(r'\\u0027', "'", decoded, flags=re.I)
        decoded = re.sub(r'\\u003a', ':', decoded, flags=re.I)
        decoded = re.sub(r'\\u002f', '/', decoded, flags=re.I)
        decoded = re.sub(r'\\u003d', '=', decoded, flags=re.I)
        decoded = re.sub(r'\\u0026', '&', decoded, flags=re.I)
        decoded = decoded.replace('\\/', '/').replace('\\"', '"')
        if decoded == current:
            break
        current = decoded
    return variants


def _parse_human_tz_datetime(value):
    """Парсит абсолютное человекочитаемое время с явной TZ.

    V6.12 принимает точность ДО МИНУТЫ: секунды могут присутствовать, но не обязательны.
    Это соответствует реальной задаче reminders: пользователю важны дата, час и минута.
    """
    if value is None:
        return None
    text = html_lib.unescape(str(value))
    text = re.sub(r'\s+', ' ', text).strip(' \t\r\n,;|')
    text = re.sub(r'\bat\b', ' ', text, flags=re.I)
    text = re.sub(r'\s+', ' ', text)
    m = re.search(r'\b(BST|GMT|UTC|PDT|PST|MDT|MST|CDT|CST|EDT|EST)\b\s*$', text, re.I)
    if not m:
        return None
    tz_name = m.group(1).upper()
    core = text[:m.start()].strip(' ,')
    fixed_offsets = {
        'UTC': 0, 'GMT': 0, 'BST': 1,
        'PDT': -7, 'PST': -8, 'MDT': -6, 'MST': -7,
        'CDT': -5, 'CST': -6, 'EDT': -4, 'EST': -5,
    }
    offset = timezone(timedelta(hours=fixed_offsets[tz_name]))
    formats = (
        '%d %b %Y %H:%M:%S', '%d %B %Y %H:%M:%S',
        '%d %b, %Y %H:%M:%S', '%d %B, %Y %H:%M:%S',
        '%b %d %Y %H:%M:%S', '%B %d %Y %H:%M:%S',
        '%b %d, %Y %H:%M:%S', '%B %d, %Y %H:%M:%S',
        '%d %b %Y %I:%M:%S %p', '%d %B %Y %I:%M:%S %p',
        '%b %d, %Y %I:%M:%S %p', '%B %d, %Y %I:%M:%S %p',
        # Minute precision (seconds omitted by eBay UI/search cards).
        '%d %b %Y %H:%M', '%d %B %Y %H:%M',
        '%d %b, %Y %H:%M', '%d %B, %Y %H:%M',
        '%b %d %Y %H:%M', '%B %d %Y %H:%M',
        '%b %d, %Y %H:%M', '%B %d, %Y %H:%M',
        '%d %b %Y %I:%M %p', '%d %B %Y %I:%M %p',
        '%b %d, %Y %I:%M %p', '%B %d, %Y %I:%M %p',
    )
    for fmt in formats:
        try:
            return datetime.strptime(core, fmt).replace(tzinfo=offset).astimezone(timezone.utc)
        except ValueError:
            continue
    return None

def _find_absolute_datetime_values(fragment):
    """Ищет абсолютный timestamp возле semantic key; секунды необязательны."""
    values = []
    if not fragment:
        return values
    # ISO 8601 with timezone / Z, to seconds OR minutes.
    for m in re.finditer(
        r'20\d{2}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})',
        fragment,
    ):
        dt = _parse_iso_datetime(m.group(0))
        if dt:
            values.append(dt)
    # Epoch sec/ms. Ограничиваемся 10-13 digits и только контекстом semantic key.
    for m in re.finditer(r'(?<!\d)(\d{10,13})(?!\d)', fragment):
        dt = _parse_iso_datetime(m.group(1))
        if dt and 2020 <= dt.year <= 2040:
            values.append(dt)
    # Human date/time with explicit timezone, seconds optional.
    human_patterns = (
        re.compile(
            r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)?[,]?\s*'
            r'\d{1,2}\s+[A-Za-z]{3,9}[,]?\s+20\d{2}[,]?\s+'
            r'\d{1,2}:\d{2}(?::\d{2})?(?:\s*(?:AM|PM))?\s*'
            r'(?:BST|GMT|UTC|PDT|PST|MDT|MST|CDT|CST|EDT|EST)',
            re.I,
        ),
        re.compile(
            r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)?[,]?\s*'
            r'[A-Za-z]{3,9}\s+\d{1,2}[,]?\s+20\d{2}[,]?\s+'
            r'\d{1,2}:\d{2}(?::\d{2})?(?:\s*(?:AM|PM))?\s*'
            r'(?:BST|GMT|UTC|PDT|PST|MDT|MST|CDT|CST|EDT|EST)',
            re.I,
        ),
    )
    for pat in human_patterns:
        for m in pat.finditer(fragment):
            dt = _parse_human_tz_datetime(m.group(0))
            if dt:
                values.append(dt)
    return values

def _extract_keyed_datetimes(raw_html, keys, item_id=None, whole_page=False):
    """Достаёт exact datetime возле набора semantic keys из escaped/plain HTML."""
    if not raw_html:
        return []
    key_alt = '|'.join(re.escape(k) for k in keys)
    values = []
    for variant in _ebay_decoded_variants(raw_html):
        sources = []
        if item_id:
            for mm in re.finditer(re.escape(str(item_id)), variant):
                sources.append(variant[max(0, mm.start()-7000): min(len(variant), mm.end()+26000)])
                if len(sources) >= 12:
                    break
        if whole_page or not sources:
            sources.append(variant)
        for source in sources:
            key_re = re.compile(rf'(?i)(?:["\']|\b)({key_alt})(?:["\']|\b)\s*[:=]')
            for km in key_re.finditer(source):
                # Time value may be scalar or nested {value: ...}; 700 chars is enough
                # while keeping unrelated recommendation timestamps out.
                frag = source[km.end():km.end()+700]
                values.extend(_find_absolute_datetime_values(frag))
                # Некоторые hydration fragments содержат локализованный HH:MM без TZ.
                # V6.12 его намеренно НЕ принимает как London: реальный ux-timer может
                # отображаться в timezone клиента/proxy. Здесь берём только явную TZ.
                try:
                    local_dt = _parse_visible_market_datetime(frag, default_tz=None)
                except NameError:
                    local_dt = None
                if local_dt:
                    values.append(local_dt)
                if len(values) >= 40:
                    return values
    return values


def _extract_visible_exact_start(visible):
    if not visible:
        return None
    label_re = re.compile(r'\b(?:Started|Start(?:ed)?\s*time|Listing\s+started)\b\s*:?\s*', re.I)
    for m in label_re.finditer(visible):
        frag = visible[m.end():m.end()+150]
        # Drop leading weekday punctuation only; parser requires explicit TZ + seconds.
        human = re.search(
            r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)?[,]?\s*\d{1,2}\s+[A-Za-z]{3,9}[,]?\s+20\d{2}[,]?\s+'
            r'\d{1,2}:\d{2}:\d{2}(?:\s*(?:AM|PM))?\s*(?:BST|GMT|UTC)',
            frag, re.I,
        )
        if human:
            dt = _parse_human_tz_datetime(human.group(0))
            if dt:
                return dt
    return None


def _extract_exact_start_time(raw_html, item_id=None):
    if not raw_html:
        return None, 'none'
    soup = BeautifulSoup(raw_html, 'html.parser')
    visible = re.sub(r'\s+', ' ', soup.get_text(' ', strip=True))
    visible_start = _extract_visible_exact_start(visible)
    if visible_start:
        return visible_start, 'visible_exact_start'

    strong_keys = (
        'itemStartDate', 'listingStartTime', 'auctionStartTime',
        'listingStartDate', 'startDateTime', 'startTime', 'startDate',
    )
    vals = _extract_keyed_datetimes(raw_html, strong_keys, item_id=item_id, whole_page=False)
    if vals:
        clusters = _cluster_datetimes(vals)
        if len(clusters) == 1:
            return clusters[0][0], 'structured_start_near_item'

    # itemCreationDate может отличаться от фактического StartTime у scheduled listing.
    # Поэтому возвращаем его отдельным source: derive-функция не имеет права строить
    # end только из creation+duration без дополнительного corroboration.
    vals = _extract_keyed_datetimes(
        raw_html, ('itemCreationDate',), item_id=item_id, whole_page=False
    )
    if vals:
        clusters = _cluster_datetimes(vals)
        if len(clusters) == 1:
            return clusters[0][0], 'structured_item_creation_near_item'

    # Whole page only strong keys; generic analytics timestamps не принимаем.
    vals = _extract_keyed_datetimes(raw_html, strong_keys, item_id=None, whole_page=True)
    if vals:
        clusters = _cluster_datetimes(vals)
        if len(clusters) == 1:
            return clusters[0][0], 'structured_start_unique_page'
    return None, 'none'


def _extract_listing_duration_days(raw_html, item_id=None):
    """Возвращает только стандартную auction duration, если она однозначна."""
    if not raw_html:
        return None, 'none'
    allowed = {1, 3, 5, 7, 10, 30}
    found = []
    variants = _ebay_decoded_variants(raw_html)
    keys = ('listingDuration', 'auctionDuration', 'listingPeriod', 'durationDays')
    key_alt = '|'.join(re.escape(k) for k in keys)

    for variant in variants:
        sources = []
        if item_id:
            for mm in re.finditer(re.escape(str(item_id)), variant):
                sources.append(variant[max(0, mm.start()-7000): min(len(variant), mm.end()+26000)])
                if len(sources) >= 10:
                    break
        if not sources:
            sources = [variant]
        for src in sources:
            for km in re.finditer(rf'(?i)(?:["\']|\b)(?:{key_alt})(?:["\']|\b)\s*[:=]', src):
                frag = src[km.end():km.end()+180]
                patterns = (
                    r'(?i)Days[_\s-]?(\d{1,2})',
                    r'(?i)P(\d{1,2})D\b',
                    r'(?i)(\d{1,2})\s*days?\b',
                    r'^[\s"\']*(\d{1,2})(?:[\s,"\'}]|$)',
                )
                for pat in patterns:
                    m = re.search(pat, frag)
                    if m:
                        d = int(m.group(1))
                        if d in allowed:
                            found.append(d)
                            break

    soup = BeautifulSoup(raw_html, 'html.parser')
    visible = re.sub(r'\s+', ' ', soup.get_text(' ', strip=True))
    for m in re.finditer(r'\bDuration\b\s*:?\s*(\d{1,2})\s*days?\b', visible, re.I):
        d = int(m.group(1))
        if d in allowed:
            found.append(d)
    unique = sorted(set(found))
    if len(unique) == 1:
        return unique[0], 'listing_duration'
    return None, 'none'


def _extract_relative_time_left(visible):
    """Парсит только отображаемый countdown; сам по себе он НЕ считается exact end time."""
    if not visible:
        return None
    candidates = []
    anchor_re = re.compile(r'\b(?:Time\s+left|Ends?\s+in)\b\s*:?\s*', re.I)
    for anchor in anchor_re.finditer(visible):
        frag = visible[anchor.end():anchor.end()+100]
        vals = {}
        unit_patterns = (
            ('d', r'(\d+)\s*(?:d\b|days?\b)'),
            ('h', r'(\d+)\s*(?:h\b|hrs?\b|hours?\b)'),
            ('m', r'(\d+)\s*(?:m\b|mins?\b|minutes?\b)'),
            ('s', r'(\d+)\s*(?:s\b|secs?\b|seconds?\b)'),
        )
        for unit, pat in unit_patterns:
            mm = re.search(pat, frag, re.I)
            if mm:
                vals[unit] = int(mm.group(1))
        if not vals:
            continue
        seconds = vals.get('d', 0)*86400 + vals.get('h', 0)*3600 + vals.get('m', 0)*60 + vals.get('s', 0)
        if seconds < 0:
            continue
        if 's' in vals:
            tolerance = 45
        elif 'm' in vals:
            tolerance = 150
        elif 'h' in vals:
            tolerance = 3900
        else:
            tolerance = 90000
        candidates.append((len(vals), seconds, tolerance, dict(vals)))
    if not candidates:
        return None
    candidates.sort(reverse=True, key=lambda x: x[0])
    _, seconds, tolerance, vals = candidates[0]
    return seconds, tolerance, vals


def _parse_visible_market_datetime(text, observed_at=None, default_tz=None):
    """Парсит абсолютную дату/время минимум до минуты.

    КРИТИЧНО V6.12: отсутствие timezone больше НЕ означает Europe/London. Скриншот
    реального ebay.co.uk показывает ``Monday, 18:42`` в локальном времени браузера,
    поэтому через proxy такой clock может зависеть от географии/локали. Без явной TZ
    значение принимается только если caller сознательно передал default_tz.
    """
    if not text:
        return None
    observed_at = _ensure_aware_utc(observed_at or datetime.now(timezone.utc))
    raw = html_lib.unescape(str(text))
    raw = re.sub(r'\s+', ' ', raw).strip()

    # Today / Tomorrow at 19:35 [BST]
    rel = re.search(
        r'\b(Today|Tomorrow)\b(?:\s+at)?\s+(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM)?\s*'
        r'(BST|GMT|UTC)?\b',
        raw, re.I,
    )
    if rel:
        word, hh, mm, ss, ampm, tz_name = rel.groups()
        if not tz_name and default_tz is None:
            return None
        hh, mm, ss = int(hh), int(mm), int(ss or 0)
        if ampm:
            ap = ampm.upper()
            hh = hh % 12 + (12 if ap == 'PM' else 0)
        context_tz = default_tz or LONDON_TZ
        local_now = observed_at.astimezone(context_tz)
        day = local_now.date() + timedelta(days=1 if word.lower() == 'tomorrow' else 0)
        if tz_name:
            parsed = _parse_human_tz_datetime(
                f"{day.day} {day.strftime('%b')} {day.year} {hh:02d}:{mm:02d}:{ss:02d} {tz_name}"
            )
            return parsed.replace(second=ss, microsecond=0) if parsed else None
        return datetime(day.year, day.month, day.day, hh, mm, ss, tzinfo=default_tz).astimezone(timezone.utc)

    # Day-first / month-first with optional year and explicit/known timezone.
    day_first = re.search(
        r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)?[,]?\s*'
        r'(\d{1,2})\s+([A-Za-z]{3,9})(?:[,]?\s+(20\d{2}))?[,]?\s*(?:at\s*)?'
        r'(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM)?\s*(BST|GMT|UTC)?\b',
        raw, re.I,
    )
    month_first = re.search(
        r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)?[,]?\s*'
        r'([A-Za-z]{3,9})\s+(\d{1,2})(?:[,]?\s+(20\d{2}))?[,]?\s*(?:at\s*)?'
        r'(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM)?\s*'
        r'(BST|GMT|UTC|PDT|PST|MDT|MST|CDT|CST|EDT|EST)?\b',
        raw, re.I,
    )
    match = day_first or month_first
    if not match:
        return None
    if match is day_first:
        day, month_text, year, hh, mm, ss, ampm, tz_name = match.groups()
    else:
        month_text, day, year, hh, mm, ss, ampm, tz_name = match.groups()

    if not tz_name and default_tz is None:
        return None

    day = int(day)
    hh, mm, ss = int(hh), int(mm), int(ss or 0)
    if ampm:
        ap = ampm.upper()
        hh = hh % 12 + (12 if ap == 'PM' else 0)

    month = None
    for fmt in ('%b', '%B'):
        try:
            month = datetime.strptime(month_text[:3] if fmt == '%b' else month_text, fmt).month
            break
        except ValueError:
            continue
    if not month:
        return None

    context_tz = default_tz or LONDON_TZ
    local_now = observed_at.astimezone(context_tz)
    if year:
        year = int(year)
    else:
        year = local_now.year
        try:
            provisional = datetime(year, month, day, hh, mm, ss, tzinfo=context_tz)
            if provisional < local_now - timedelta(days=2):
                year += 1
        except ValueError:
            return None

    if tz_name:
        core = f"{day} {datetime(2000, month, 1).strftime('%b')} {year} {hh:02d}:{mm:02d}:{ss:02d} {tz_name}"
        return _parse_human_tz_datetime(core)
    try:
        return datetime(year, month, day, hh, mm, ss, tzinfo=default_tz).astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None

def _extract_visible_end_minute(visible, observed_at=None, default_tz=None):
    """Абсолютный end-date/time до минуты; секунды не требуются.

    Никакой относительный countdown (например 2d 5h) здесь не используется.
    """
    if not visible:
        return None
    label_re = re.compile(
        r'\b(?:Ends?|Ending|End\s*time|Auction\s+ends?|Listing\s+ends?)(?:\s+on)?\s*:?[，,\s]*',
        re.I,
    )
    for lm in label_re.finditer(visible):
        frag = visible[lm.end():lm.end()+220]
        dt = _parse_visible_market_datetime(frag, observed_at=observed_at, default_tz=default_tz)
        if dt:
            return dt.replace(microsecond=0)
    return None


# Реальные civil UTC offsets, которые используются современными часовыми поясами.
# Нужны только для inference локализованного eBay clock без IP-geolocation API.
_CIVIL_UTC_OFFSETS_MINUTES = (
    -720, -660, -600, -570, -540, -480, -420, -360, -300, -240, -210,
    -180, -150, -120, -60, 0, 60, 120, 180, 210, 240, 270, 300, 330, 345,
    360, 390, 420, 480, 525, 540, 570, 600, 630, 660, 720, 765, 780, 825,
    840,
)
_WEEKDAY_INDEX = {
    'monday': 0, 'mon': 0,
    'tuesday': 1, 'tue': 1, 'tues': 1,
    'wednesday': 2, 'wed': 2,
    'thursday': 3, 'thu': 3, 'thur': 3, 'thurs': 3,
    'friday': 4, 'fri': 4,
    'saturday': 5, 'sat': 5,
    'sunday': 6, 'sun': 6,
}


def _parse_weekday_clock(text):
    """Возвращает (weekday, hour, minute) для ``Monday, 18:42`` / ``Mon 6:42 PM``."""
    if not text:
        return None
    raw = html_lib.unescape(str(text))
    raw = re.sub(r'\s+', ' ', raw).strip()
    m = re.search(
        r'\b(Monday|Mon|Tuesday|Tue(?:s)?|Wednesday|Wed|Thursday|Thu(?:rs?)?|Friday|Fri|Saturday|Sat|Sunday|Sun)\b'
        r'\s*[,]?\s*(\d{1,2}):(\d{2})\s*(AM|PM)?\b',
        raw, re.I,
    )
    if not m:
        return None
    wd = _WEEKDAY_INDEX.get(m.group(1).lower())
    hh = int(m.group(2))
    mm = int(m.group(3))
    ampm = (m.group(4) or '').upper()
    if mm > 59 or hh > 23:
        return None
    if ampm:
        if hh > 12:
            return None
        hh = hh % 12 + (12 if ampm == 'PM' else 0)
    return wd, hh, mm


def _parse_relative_day_clock(text):
    """Возвращает (day_delta, hour, minute) для ``Today 07:43`` / ``Tomorrow at 6:42 PM``.

    eBay search-card реально использует ``(Today 07:43)``. Это локальное время
    страницы/proxy, поэтому его нельзя считать London/Kyiv напрямую. Оно используется
    только вместе с timezone-independent countdown для вывода единственного UTC minute.
    """
    if not text:
        return None
    raw = html_lib.unescape(str(text))
    raw = re.sub(r'\s+', ' ', raw).strip()
    m = re.search(
        r'\b(Today|Tomorrow)\b\s*[,]?\s*(?:at\s*)?(\d{1,2}):(\d{2})\s*(AM|PM)?\b',
        raw, re.I,
    )
    if not m:
        return None
    hh = int(m.group(2))
    mm = int(m.group(3))
    ampm = (m.group(4) or '').upper()
    if mm > 59 or hh > 23:
        return None
    if ampm:
        if hh > 12:
            return None
        hh = hh % 12 + (12 if ampm == 'PM' else 0)
    return (1 if m.group(1).lower() == 'tomorrow' else 0), hh, mm


def _extract_localized_clock_text(text):
    """Извлекает локализованный eBay clock: weekday либо Today/Tomorrow + HH:MM."""
    if not text:
        return ''
    raw = html_lib.unescape(str(text))
    raw = re.sub(r'\s+', ' ', raw).strip()
    m = re.search(
        r'(?:'
        r'\b(?:Monday|Mon|Tuesday|Tue(?:s)?|Wednesday|Wed|Thursday|Thu(?:rs?)?|Friday|Fri|Saturday|Sat|Sunday|Sun)\b'
        r'\s*[,]?\s*'
        r'|'
        r'\b(?:Today|Tomorrow)\b\s*[,]?\s*(?:at\s*)?'
        r')'
        r'\d{1,2}:\d{2}(?:\s*(?:AM|PM))?\b',
        raw, re.I,
    )
    return m.group(0).strip() if m else ''


def _relative_floor_window(relative):
    """Для eBay countdown возвращает [floor_seconds, ceiling_seconds).

    ``6d 23h`` трактуется как floor до часа: реальный остаток 167h..168h.
    Если eBay показывает минуты — окно уже 60 сек. Это НЕ конечный timestamp,
    а только независимая проверка локализованного weekday/clock.
    """
    if not relative:
        return None
    seconds, _old_tolerance, vals = relative
    if 's' in vals:
        width = 1
    elif 'm' in vals:
        width = 60
    elif 'h' in vals:
        width = 3600
    else:
        width = 86400
    return float(seconds), float(seconds + width), vals


def _localized_clock_utc_candidates(relative_text, clock_text, observed_at=None):
    """Строит ВСЕ правдоподобные UTC-minute для локализованного eBay timer.

    Поддерживает как weekday-clock (``Sat, 09:19``), так и реальный search-card
    формат eBay ``Today 07:43`` / ``Tomorrow 07:43``. Сам clock локализован
    страницей/proxy, поэтому ни London, ни Kyiv не предполагаются. UTC определяется
    только пересечением с timezone-independent countdown.
    """
    weekday_clock = _parse_weekday_clock(clock_text)
    relative_day_clock = _parse_relative_day_clock(clock_text)
    if not weekday_clock and not relative_day_clock:
        return set(), None

    relative = _extract_relative_time_left(relative_text)
    floor_window = _relative_floor_window(relative)
    if not floor_window:
        return set(), None

    observed = _ensure_aware_utc(observed_at or datetime.now(timezone.utc))
    low, high, vals = floor_window
    # При минутном countdown eBay отбрасывает секунды. 90 секунд покрывают сетевую
    # задержку + округление, не расширяя окно до соседнего часового offset.
    slack = 90.0 if ('m' in vals or 's' in vals) else 30.0
    candidates = set()

    for offset_minutes in _CIVIL_UTC_OFFSETS_MINUTES:
        fixed_tz = timezone(timedelta(minutes=offset_minutes))

        if relative_day_clock:
            day_delta, hh, mm = relative_day_clock
            local_now = observed.astimezone(fixed_tz)
            local_date = local_now.date() + timedelta(days=day_delta)
            try:
                local_end = datetime(
                    local_date.year, local_date.month, local_date.day,
                    hh, mm, 0, tzinfo=fixed_tz,
                )
            except ValueError:
                continue
            end_utc = local_end.astimezone(timezone.utc).replace(second=0, microsecond=0)
            remaining = (end_utc - observed).total_seconds()
            if low - slack <= remaining < high + slack:
                candidates.add(end_utc)
            continue

        target_wd, hh, mm = weekday_clock
        # eBay auctions в этом bot use-case имеют горизонт в пределах нескольких дней;
        # +14 дней оставляет запас и покрывает weekly rollover/DST edge cases.
        utc_base_date = observed.date()
        for day_delta in range(-1, 15):
            local_date = utc_base_date + timedelta(days=day_delta)
            if local_date.weekday() != target_wd:
                continue
            try:
                local_end = datetime(
                    local_date.year, local_date.month, local_date.day,
                    hh, mm, 0, tzinfo=fixed_tz,
                )
            except ValueError:
                continue
            end_utc = local_end.astimezone(timezone.utc).replace(second=0, microsecond=0)
            remaining = (end_utc - observed).total_seconds()
            if low - slack <= remaining < high + slack:
                candidates.add(end_utc)

    diagnostic = {
        'relative': re.sub(r'\s+', ' ', str(relative_text)).strip(),
        'clock': re.sub(r'\s+', ' ', str(clock_text)).strip(),
        'values': vals,
    }
    return candidates, diagnostic


def _extract_localized_timer_observations(raw_html, observed_at=None):
    """Извлекает современный eBay timer и безопасные UTC candidate sets.

    Поддерживаются оба реально встреченных формата:
      * ``Ends in 6d 23h`` + ``Monday, 18:42`` (View Item);
      * ``Time left 4d 14h left (Sat, 14:26)`` (точная search-card).
    Сам clock может быть локализован proxy/браузером — London не предполагаем.
    """
    if not raw_html:
        return []
    soup = BeautifulSoup(raw_html, 'html.parser')
    observed = _ensure_aware_utc(observed_at or datetime.now(timezone.utc))
    observations = []
    seen_pairs = set()

    clock_regex = (
        r'(?:'
        r'\b(?:Monday|Mon|Tuesday|Tue(?:s)?|Wednesday|Wed|Thursday|Thu(?:rs?)?|Friday|Fri|Saturday|Sat|Sunday|Sun)\b\s*[,]?\s*'
        r'|'
        r'\b(?:Today|Tomorrow)\b\s*[,]?\s*(?:at\s*)?'
        r')'
        r'\d{1,2}:\d{2}(?:\s*(?:AM|PM))?'
    )
    relative_regex = (
        r'\b(?:Ends?\s+in|Time\s+left)\s*:?[\s]*'
        r'(?:(?:\d+)\s*(?:d|days?|h|hrs?|hours?|m|mins?|minutes?|s|secs?|seconds?)\s*){1,4}'
    )

    containers = soup.select(
        '[data-testid="x-end-time"], .x-end-time, [data-testid="ux-timer"], .ux-timer'
    )
    for container in containers:
        rel_node = (
            container.select_one('[data-testid="ux-timer__text"]')
            or container.select_one('.ux-timer__text')
        )
        clock_node = (
            container.select_one('.ux-timer__time-left')
            or container.select_one('[data-testid="ux-timer__time-left"]')
        )
        rel_text = rel_node.get_text(' ', strip=True) if rel_node else ''
        clock_text = clock_node.get_text(' ', strip=True) if clock_node else ''
        if not rel_text or not clock_text:
            whole = container.get_text(' ', strip=True)
            rel_m = re.search(relative_regex, whole, re.I)
            clock_m = re.search(clock_regex, whole, re.I)
            if rel_m and not rel_text:
                rel_text = rel_m.group(0)
            if clock_m and not clock_text:
                clock_text = clock_m.group(0)
        pair = (rel_text.strip(), clock_text.strip())
        if not all(pair) or pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        candidates, diagnostic = _localized_clock_utc_candidates(
            rel_text, clock_text, observed_at=observed
        )
        if candidates:
            observations.append({
                'candidates': candidates,
                'relative': diagnostic['relative'],
                'clock': diagnostic['clock'],
                'observed_at': observed,
                'source': 'ux_timer',
            })

    # Search-card fallback. В реальном логе eBay текст ровно такой:
    # ``Time left 4d 14h left (Sat, 14:26)``. предыдущий regex видел строку, но искал
    # только ``Ends in``, поэтому точный UTC candidate вообще не строился.
    visible = re.sub(r'\s+', ' ', soup.get_text(' ', strip=True))
    pat = re.compile(
        rf'({relative_regex})\s*(?:left\b)?\s*[\(\[]?\s*.{{0,20}}?'
        rf'({clock_regex})\s*[\)\]]?',
        re.I,
    )
    for m in pat.finditer(visible):
        pair = (m.group(1).strip(), m.group(2).strip())
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        candidates, diagnostic = _localized_clock_utc_candidates(
            pair[0], pair[1], observed_at=observed
        )
        if candidates:
            observations.append({
                'candidates': candidates,
                'relative': diagnostic['relative'],
                'clock': diagnostic['clock'],
                'observed_at': observed,
                'source': 'visible_timer',
            })
    return observations


def _resolve_localized_timer_evidence(html_pages, observed_at=None):
    """Консервативно сводит localized timer из нескольких независимых HTML.

    Одна страница принимается только при singleton UTC candidate. Для нескольких
    страниц сначала требуем строгий intersection; если один HTML слегка отличается,
    допускаем единственный candidate с поддержкой >=2 страниц и большинством.
    """
    observations = []
    for raw_html in (html_pages or []):
        observations.extend(_extract_localized_timer_observations(raw_html, observed_at=observed_at))

    if not observations:
        return None, 'ux_timer:none', []

    sets = [set(o['candidates']) for o in observations if o['candidates']]
    if not sets:
        return None, 'ux_timer:none', observations
    if len(sets) == 1:
        if len(sets[0]) == 1:
            return next(iter(sets[0])), 'ux_timer:unique_offset', observations
        return None, 'ux_timer:ambiguous', observations

    inter = set.intersection(*sets)
    if len(inter) == 1:
        return next(iter(inter)), f'ux_timer:intersection_{len(sets)}', observations

    support = {}
    for candidate_set in sets:
        for candidate in candidate_set:
            minute = _ensure_aware_utc(candidate).replace(second=0, microsecond=0)
            support[minute] = support.get(minute, 0) + 1
    if support:
        best_support = max(support.values())
        best = [k for k, v in support.items() if v == best_support]
        majority_needed = max(2, (len(sets) + 1) // 2)
        if best_support >= majority_needed and len(best) == 1:
            return best[0], f'ux_timer:consensus_{best_support}_of_{len(sets)}', observations

    return None, 'ux_timer:ambiguous', observations

def _log_timer_observations(item_id, observations, prefix='Auction timer'):
    for idx, obs in enumerate(observations[:6], 1):
        cands = sorted(x.strftime('%Y-%m-%dT%H:%MZ') for x in obs['candidates'])
        logging.info(
            f"🧭 {prefix} #{idx}: item={item_id}, relative='{obs['relative']}', "
            f"clock='{obs['clock']}', UTC-кандидаты={cands}"
        )


def _page_has_active_auction_evidence(raw_html, item_id=None):
    if not raw_html:
        return False
    soup = BeautifulSoup(raw_html, 'html.parser')
    visible = re.sub(r'\s+', ' ', soup.get_text(' ', strip=True))
    return bool(
        re.search(r'\b\d+\s+bids?\b', visible, re.I)
        or re.search(r'\bsubmit\s+bid\b', visible, re.I)
        or re.search(r'\bplace\s+(?:a\s+)?bid\b', visible, re.I)
        or _structured_auction_evidence(raw_html, item_id=item_id)
    )


def _derive_exact_end_from_start_duration(raw_html, item_id=None, supporting_htmls=None, observed_at=None):
    """Консервативный exact fallback без приблизительного countdown.

    Приоритет:
      1) exact start + explicit eBay listing duration;
      2) exact start + абсолютное displayed end до минуты, где секунду берём ИЗ start;
      3) exact start + unique standard duration, подтверждённую отображаемым Time left.

    Вариант 3 не использует countdown как конечное время: countdown только выбирает
    duration из стандартных eBay 1/3/5/7/10/30 дней, а exact seconds приходят из start.
    Если выбор не однозначен — ничего не сохраняем.
    """
    blobs = [raw_html] + [x for x in (supporting_htmls or []) if x]
    start = None
    start_source = 'none'
    start_blob = None
    for blob in blobs:
        st, src = _extract_exact_start_time(blob, item_id=item_id)
        if st:
            start, start_source, start_blob = st, src, blob
            break
    if not start:
        return None, 'none'

    creation_only_start = 'item_creation' in start_source
    for blob in blobs:
        days, dur_source = _extract_listing_duration_days(blob, item_id=item_id)
        if days and not creation_only_start:
            end = start + timedelta(days=days)
            if end > datetime.now(timezone.utc) - timedelta(hours=1):
                return end, f'derived:{start_source}+{dur_source}:{days}d'

    # eBay UI sometimes hides seconds in the displayed Ends line. Since fixed-duration
    # auctions preserve the start second, combine the displayed minute with exact start second.
    for blob in blobs:
        soup = BeautifulSoup(blob, 'html.parser')
        visible = re.sub(r'\s+', ' ', soup.get_text(' ', strip=True))
        minute_end = _extract_visible_end_minute(visible)
        if minute_end and not creation_only_start:
            candidate = minute_end.replace(second=start.second, microsecond=start.microsecond)
            if candidate > datetime.now(timezone.utc) - timedelta(hours=1):
                return candidate, f'derived:{start_source}+visible_end_minute'

    now = _ensure_aware_utc(observed_at or datetime.now(timezone.utc))
    standard_days = (1, 3, 5, 7, 10, 30)
    for blob in blobs:
        soup = BeautifulSoup(blob, 'html.parser')
        visible = re.sub(r'\s+', ' ', soup.get_text(' ', strip=True))
        rel = _extract_relative_time_left(visible)
        if not rel:
            continue
        rel_seconds, tolerance, _ = rel
        matches = []
        for days in standard_days:
            candidate = start + timedelta(days=days)
            remaining = (candidate - now).total_seconds()
            if remaining >= -60 and abs(remaining - rel_seconds) <= tolerance:
                matches.append((days, candidate))
        if len(matches) == 1:
            days, candidate = matches[0]
            return candidate, f'derived:{start_source}+time_left_unique_duration:{days}d'
    return None, 'none'

def _cluster_datetimes(values, tolerance_seconds=5):
    clusters = []
    for dt in sorted((_ensure_aware_utc(v) for v in values if v), key=lambda x: x.timestamp()):
        placed = False
        for cluster in clusters:
            if abs((dt - cluster[0]).total_seconds()) <= tolerance_seconds:
                cluster.append(dt)
                placed = True
                break
        if not placed:
            clusters.append([dt])
    return clusters


def _extract_semantic_attribute_end_times(html):
    """Читает end-time из HTML attributes/time tags, включая data-end-time.

    V6.7 проверял значение data-end-time, но только если class/id самого элемента уже
    содержали слово end. Современный eBay часто хранит timestamp только в имени data-attr.
    """
    if not html:
        return []
    soup = BeautifulSoup(html, 'html.parser')
    values = []
    end_attr_name = re.compile(
        r'^(?:data[-_])?(?:item[-_]?|listing[-_]?|auction[-_]?)?'
        r'(?:end|ending|ends)(?:[-_]?(?:date|time|datetime|at|timestamp))?$',
        re.I,
    )
    marker_re = re.compile(r'(?:^|[-_\s])(?:end|ending|ends|time[-_\s]?left)(?:$|[-_\s])', re.I)
    for tag in soup.find_all(True):
        attrs = tag.attrs or {}
        attrs_lower = {str(k).lower(): v for k, v in attrs.items()}
        marker_text = ' '.join(
            str(attrs_lower.get(k, '')) for k in
            ('itemprop', 'data-testid', 'id', 'class', 'name', 'property', 'aria-label', 'title')
        )
        for raw_name, raw_value in attrs.items():
            name = str(raw_name).lower()
            if not (end_attr_name.search(name) or (name == 'datetime' and (tag.name == 'time' or marker_re.search(marker_text)))):
                continue
            candidates = raw_value if isinstance(raw_value, (list, tuple)) else [raw_value]
            for raw in candidates:
                dt = _parse_iso_datetime(raw) or _parse_human_tz_datetime(raw)
                if dt and 2020 <= dt.year <= 2040:
                    values.append(dt)
        # aria-label/title/class can itself contain "Ending 21 Sep at 19:35".
        if marker_re.search(marker_text):
            marker_dt = _extract_visible_end_minute(marker_text)
            if marker_dt:
                values.append(marker_dt)

        # <time datetime="..."> whose nearby visible text is Ends/Ending/Time left.
        if tag.name == 'time' and tag.get('datetime'):
            nearby = ' '.join([
                marker_text,
                tag.get_text(' ', strip=True),
                tag.parent.get_text(' ', strip=True)[:180] if isinstance(tag.parent, Tag) else '',
            ])
            if marker_re.search(nearby):
                dt = _parse_iso_datetime(tag.get('datetime')) or _parse_human_tz_datetime(tag.get('datetime'))
                if dt and 2020 <= dt.year <= 2040:
                    values.append(dt)
    return values


def _choose_unique_plausible_end(values, horizon_days=45):
    if not values:
        return None, False
    clusters = _cluster_datetimes(values, tolerance_seconds=65)
    now = datetime.now(timezone.utc)
    plausible = [c for c in clusters if now - timedelta(days=1) <= c[0] <= now + timedelta(days=horizon_days)]
    if len(plausible) == 1:
        return plausible[0][0], False
    if len(plausible) > 1:
        return None, True
    return None, False


def _extract_structured_end_time(html, item_id=None):
    """Ищет exact UTC end time в plain/escaped structured data eBay.

    V6.12 понимает HTML entities, JS-unicode escapes и новые semantic key names.
    Whole-page fallback остаётся строгим: generic ``endTime`` по всей странице не
    принимается, чтобы recommendation cards не подменили основной лот.
    """
    if not html:
        return None, 'none', False

    contextual_keys = (
        'itemEndDate', 'listingEndTime', 'auctionEndTime', 'listingEndDate',
        'auctionEndDate', 'listingEndsAt', 'auctionEndsAt', 'endDateTime',
        'endTime', 'endDate', 'endsAt', 'endAt', 'endTimestamp', 'endTimeUtc',
    )
    strong_page_keys = (
        'itemEndDate', 'listingEndTime', 'auctionEndTime', 'listingEndDate',
        'auctionEndDate', 'listingEndsAt', 'auctionEndsAt', 'endDateTime',
        'endTimeUtc',
    )

    contextual = _extract_keyed_datetimes(
        html, contextual_keys, item_id=item_id, whole_page=False
    )
    if contextual:
        clusters = _cluster_datetimes(contextual)
        if len(clusters) == 1:
            return clusters[0][0], 'structured_near_item', False
        # Не считаем timestamps из прошлого/далёкого будущего, если среди clusters есть
        # ровно один правдоподобный current-auction end. Это помогает, когда возле item id
        # соседствует analytics timestamp, но не ослабляет защиту от реального конфликта.
        now = datetime.now(timezone.utc)
        plausible = [
            c for c in clusters
            if now - timedelta(days=1) <= c[0] <= now + timedelta(days=45)
        ]
        if len(plausible) == 1:
            return plausible[0][0], 'structured_near_item_plausible', False
        return None, 'structured_near_item_conflict', True

    strict_values = _extract_keyed_datetimes(
        html, strong_page_keys, item_id=None, whole_page=True
    )
    if strict_values:
        clusters = _cluster_datetimes(strict_values)
        if len(clusters) == 1:
            return clusters[0][0], 'structured_unique_page', False
        now = datetime.now(timezone.utc)
        plausible = [
            c for c in clusters
            if now - timedelta(days=1) <= c[0] <= now + timedelta(days=45)
        ]
        if len(plausible) == 1:
            return plausible[0][0], 'structured_unique_page_plausible', False
        return None, 'structured_page_conflict', True

    # Semantic HTML / microdata fallback. Accept only elements explicitly named as end.
    soup = BeautifulSoup(html, 'html.parser')
    semantic_values = []
    for tag in soup.find_all(True):
        attrs = {str(k).lower(): str(v) for k, v in (tag.attrs or {}).items()}
        marker = ' '.join([
            attrs.get('itemprop', ''), attrs.get('data-testid', ''), attrs.get('id', ''),
            attrs.get('class', ''), attrs.get('name', ''), attrs.get('property', ''),
        ]).lower()
        if not re.search(r'(?:item|listing|auction)?[-_ ]?end(?:date|time|ing|s|at)?', marker):
            continue
        for field in ('content', 'datetime', 'data-end-time', 'data-end-date', 'value', 'aria-label', 'title'):
            raw = attrs.get(field)
            if not raw:
                continue
            dt = _parse_iso_datetime(raw) or _parse_human_tz_datetime(raw)
            if dt:
                semantic_values.append(dt)
    if semantic_values:
        clusters = _cluster_datetimes(semantic_values, tolerance_seconds=65)
        if len(clusters) == 1:
            return clusters[0][0], 'semantic_end_time', False

    attr_values = _extract_semantic_attribute_end_times(html)
    attr_end, attr_conflict = _choose_unique_plausible_end(attr_values)
    if attr_conflict:
        return None, 'semantic_attribute_conflict', True
    if attr_end:
        return attr_end, 'semantic_attribute_end_time', False

    return None, 'none', False


def _extract_visible_exact_uk_end(visible):
    """Парсит абсолютное UK/UTC end time только когда страница показывает seconds."""
    if not visible:
        return None, None
    label_re = re.compile(r'\b(Ends|Ended|Ending|End\s*time)(?:\s+on)?\s*:?[,\s]*', re.I)
    human_pat = re.compile(
        r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)?[,]?\s*'
        r'\d{1,2}\s+[A-Za-z]{3,9}[,]?\s+20\d{2}[,]?\s+'
        r'\d{1,2}:\d{2}:\d{2}(?:\s*(?:AM|PM))?\s*(?:BST|GMT|UTC)',
        re.I,
    )
    for lm in label_re.finditer(visible):
        frag = visible[lm.end():lm.end()+170]
        hm = human_pat.search(frag)
        if hm:
            dt = _parse_human_tz_datetime(hm.group(0))
            if dt:
                kind = 'ended' if lm.group(1).lower().startswith('ended') else 'ends'
                return dt, kind
    return None, None


def _structured_auction_evidence(html, item_id=None):
    if not html:
        return False
    normalized = html.replace('\\"', '"')
    windows = []
    if item_id:
        for mm in re.finditer(re.escape(str(item_id)), normalized):
            windows.append(normalized[max(0, mm.start()-4000): min(len(normalized), mm.end()+14000)])
            if len(windows) >= 10:
                break
    if not windows:
        windows = [normalized]

    patterns = (
        r'"buyingOptions"\s*:\s*\[[^\]]*"AUCTION"',
        r'"listingType"\s*:\s*"(?:Auction|Chinese)"',
        r'"format"\s*:\s*"AUCTION"',
        r'"auction"\s*:\s*true',
        r'"currentBidPrice"\s*:',
        r'"bidCount"\s*:\s*\d+',
    )
    return any(re.search(p, w, re.I) for w in windows for p in patterns)


def _auction_times_compatible(a, b, tolerance_seconds=65):
    if not a or not b:
        return True
    return abs((_ensure_aware_utc(a) - _ensure_aware_utc(b)).total_seconds()) <= tolerance_seconds


def _auction_timing_snippet(text, limit=220):
    """Короткий diagnostic fragment без огромного HTML."""
    if not text:
        return ''
    clean = re.sub(r'\s+', ' ', str(text)).strip()
    m = re.search(r'\b(?:Ends?|Ending|End time|Time left|Auction ends?|Listing ends?)\b', clean, re.I)
    if m:
        clean = clean[max(0, m.start()-50):m.start()+limit]
    return clean[:limit]


def _clean_auction_title(title, item_id=None):
    """Чистит только известные служебные подписи eBay, не меняя название товара."""
    clean = re.sub(r'\s+', ' ', str(title or '')).strip()
    if not clean:
        return f'eBay item {item_id}' if item_id else 'eBay item'

    # Accessibility-текст нередко попадает внутрь heading search-card:
    # "Rico ... Opens in a new window or tab". Это не часть названия лота.
    noise_patterns = (
        r'\s+Opens in a new window or tab\s*$',
        r'\s+Opens in new window or tab\s*$',
        r'^\s*New Listing\s*[-:|]?\s*',
    )
    previous = None
    while previous != clean:
        previous = clean
        for pattern in noise_patterns:
            clean = re.sub(pattern, '', clean, flags=re.I).strip()

    clean = re.sub(r'\s*\|\s*eBay(?:\s+UK)?\s*$', '', clean, flags=re.I).strip()
    return clean[:300] or (f'eBay item {item_id}' if item_id else 'eBay item')


def parse_auction_page(html, final_url):
    """Возвращает item_id, title, end_time_utc, source, status.

    V6.12 считает достаточной подтверждённую абсолютную дату+время ДО МИНУТЫ.
    Секунды больше не обязательны. Относительный countdown вроде "2d 5h" сам по
    себе по-прежнему не используется как точное время окончания.
    """
    if not html:
        return None, None, None, 'empty', 'unknown'

    soup = BeautifulSoup(html, 'html.parser')
    visible = re.sub(r'\s+', ' ', soup.get_text(' ', strip=True))
    item_id = extract_ebay_item_id_any(final_url, html)

    title = ''
    h1 = soup.find('h1')
    if h1:
        title = re.sub(r'\s+', ' ', h1.get_text(' ', strip=True)).strip()
    if not title:
        title = _response_title(html)
        title = re.sub(r'\s*\|\s*eBay(?:\s+UK)?\s*$', '', title, flags=re.I).strip()
    title = _clean_auction_title(title, item_id)

    visible_exact_utc, visible_kind = _extract_visible_exact_uk_end(visible)
    visible_minute_utc = _extract_visible_end_minute(visible)
    structured_end_utc, structured_source, structured_conflict = _extract_structured_end_time(
        html, item_id=item_id
    )
    if structured_conflict:
        return item_id, title, None, structured_source, 'time_conflict'

    visible_candidate = visible_exact_utc or visible_minute_utc
    if visible_candidate and structured_end_utc and not _auction_times_compatible(visible_candidate, structured_end_utc):
        return item_id, title, None, 'visible_vs_structured_conflict', 'time_conflict'

    if structured_end_utc:
        end_time_utc = structured_end_utc
        source = structured_source
        if visible_exact_utc:
            source = f'visible_exact_uk_time+{structured_source}'
        elif visible_minute_utc:
            source = f'visible_explicit_tz_minute+{structured_source}'
    elif visible_exact_utc:
        end_time_utc = visible_exact_utc
        source = 'visible_exact_uk_time'
    elif visible_minute_utc:
        end_time_utc = visible_minute_utc.replace(second=0, microsecond=0)
        source = 'visible_explicit_tz_end_minute'
    else:
        # Современный View Item (подтверждено реальным DOM):
        #   ux-timer__text      -> "Ends in 6d 23h"
        #   ux-timer__time-left -> "Monday, 18:42"
        # Clock может быть локализован по клиенту/proxy, поэтому London не предполагаем.
        timer_end, timer_source, _timer_obs = _resolve_localized_timer_evidence([html])
        if timer_end:
            end_time_utc = timer_end
            source = timer_source
        else:
            derived_end_utc, derived_source = _derive_exact_end_from_start_duration(
                html, item_id=item_id
            )
            if derived_end_utc:
                end_time_utc = derived_end_utc
                source = derived_source
            else:
                end_time_utc = None
                source = timer_source if timer_source != 'ux_timer:none' else 'none'

    bid_box_marker = bool(
        re.search(r'\bplace\s+(?:a\s+)?bid\b', visible, re.I)
        or re.search(r'\bcurrent\s+bid\b', visible, re.I)
        or re.search(r'\bstarting\s+bid\b', visible, re.I)
        or re.search(r'\b\d+\s+bids?\b', visible, re.I)
        or re.search(r'\btime\s+left\b', visible, re.I) and re.search(r'\bbid\b', visible, re.I)
    )
    structured_auction = _structured_auction_evidence(html, item_id=item_id)

    explicit_closed = bool(
        visible_kind == 'ended'
        or re.search(r'\bthis\s+listing\s+(?:has\s+)?ended\b', visible, re.I)
        or re.search(r'\bthis\s+listing\s+was\s+ended\b', visible, re.I)
        or re.search(r'\bthis\s+item\s+is\s+no\s+longer\s+available\b', visible, re.I)
        or re.search(r'\bthe\s+listing\s+you(?:’|\'|\s)?re\s+looking\s+for\s+has\s+ended\b', visible, re.I)
        or re.search(r'\bwe\s+looked\s+everywhere.*looks\s+like\s+this\s+page\s+is\s+missing\b', visible, re.I)
    )

    fixed_price_evidence = bool(
        re.search(r'\bbuy\s+it\s+now\b', visible, re.I)
        and not bid_box_marker
        and not structured_auction
        and re.search(r'"buyingOptions"\s*:\s*\[[^\]]*"FIXED_PRICE"', html.replace('\\"', '"'), re.I)
    )

    if not item_id:
        return None, title, end_time_utc, source, 'no_item_id'
    if explicit_closed:
        return item_id, title, end_time_utc, source, 'ended'
    if not end_time_utc:
        return item_id, title, None, source, 'no_exact_end_time'
    if bid_box_marker or structured_auction:
        return item_id, title, end_time_utc, source, 'active'
    if fixed_price_evidence:
        return item_id, title, end_time_utc, source, 'not_auction'
    return item_id, title, end_time_utc, source, 'no_exact_end_time'

def _search_target_card_html(html, item_id):
    if not html or not item_id:
        return None
    soup = BeautifulSoup(html, 'html.parser')
    for a in soup.find_all('a', href=True):
        href = str(a.get('href') or '')
        if str(item_id) not in href:
            continue
        card = (
            a.find_parent('li', class_=lambda c: c and ('s-item' in str(c) or 's-card' in str(c)))
            or a.find_parent('div', class_=lambda c: c and 'su-card-container' in str(c))
        )
        if card:
            return str(card)
    return None


def _search_target_card_tag(html, item_id):
    if not html or not item_id:
        return None
    soup = BeautifulSoup(html, 'html.parser')
    for a in soup.find_all('a', href=True):
        href = str(a.get('href') or '')
        if str(item_id) not in href:
            continue
        card = (
            a.find_parent('li', class_=lambda c: c and ('s-item' in str(c) or 's-card' in str(c)))
            or a.find_parent('div', class_=lambda c: c and 'su-card-container' in str(c))
        )
        if card:
            return card
    return None


def _search_card_title(target_card, item_id):
    title = ''
    if target_card:
        title_node = target_card.select_one('.s-item__title, .s-card__title, .su-styled-text.primary')
        if title_node:
            title = re.sub(r'\s+', ' ', title_node.get_text(' ', strip=True)).strip()
    return _clean_auction_title(title, item_id)


def parse_auction_search_fallback(html, item_id, observed_at=None):
    """Точная verification через search-card конкретного ItemID.

    V6.12 дополнительно понимает реальный формат ``Time left 4d 14h left
    (Sat, 14:26)``. Даже если countdown округлён до часа, weekday+HH:MM позволяет
    построить UTC-кандидаты для всех civil timezone offsets. Если кандидат ровно один,
    это уже безопасная точность до минуты — геолокация proxy не требуется.
    """
    if not html or not item_id:
        return None
    target_card = _search_target_card_tag(html, item_id)
    if not target_card:
        return None

    observed = _ensure_aware_utc(observed_at or datetime.now(timezone.utc))
    visible = re.sub(r'\s+', ' ', target_card.get_text(' ', strip=True))
    card_html = str(target_card)
    auction_marker = bool(
        re.search(r'\b\d+\s+bids?\b', visible, re.I)
        or re.search(r'\btime\s+left\b', visible, re.I)
        or re.search(r'\bplace\s+(?:a\s+)?bid\b', visible, re.I)
        or _structured_auction_evidence(card_html, item_id=item_id)
    )
    if not auction_marker:
        return None

    card_end, card_source, card_conflict = _extract_structured_end_time(card_html, item_id=item_id)
    if card_conflict:
        return None

    visible_end = _extract_visible_end_minute(visible, observed_at=observed)
    if card_end and visible_end and not _auction_times_compatible(card_end, visible_end):
        return None

    if card_end:
        end_time = card_end
        source = f'card:{card_source}'
    elif visible_end:
        end_time = visible_end.replace(second=0, microsecond=0)
        source = 'card:visible_end_minute'
    else:
        end_time, source, conflict = _extract_structured_end_time(html, item_id=item_id)
        if conflict:
            return None
        if not end_time:
            # ВАЖНО: сначала современный localized timer самой target-card.
            timer_end, timer_source, timer_obs = _resolve_localized_timer_evidence(
                [card_html], observed_at=observed
            )
            _log_timer_observations(item_id, timer_obs, prefix='Auction search timer')
            if timer_end:
                end_time = timer_end
                source = f'card:{timer_source}'
        if not end_time:
            end_time, derived_source = _derive_exact_end_from_start_duration(
                card_html, item_id=item_id, supporting_htmls=[html], observed_at=observed
            )
            if end_time:
                source = derived_source

    if not end_time:
        logging.info(
            f"🧪 Auction target-card timing: item={item_id}, text='{_auction_timing_snippet(visible)}'"
        )
        return None

    title = _search_card_title(target_card, item_id)
    return str(item_id), title, end_time, f'search_fallback:{source}', 'active'


def extract_coarse_auction_search_observation(html, item_id, observed_at=None):
    """Возвращает безопасное coarse-окно, если exact minute пока недоступна."""
    if not html or not item_id:
        return None
    target_card = _search_target_card_tag(html, item_id)
    if not target_card:
        return None
    observed = _ensure_aware_utc(observed_at or datetime.now(timezone.utc))
    visible = re.sub(r'\s+', ' ', target_card.get_text(' ', strip=True))
    card_html = str(target_card)
    active = bool(
        re.search(r'\b\d+\s+bids?\b', visible, re.I)
        or re.search(r'\btime\s+left\b', visible, re.I)
        or re.search(r'\bplace\s+(?:a\s+)?bid\b', visible, re.I)
        or _structured_auction_evidence(card_html, item_id=item_id)
    )
    if not active:
        return None
    relative = _extract_relative_time_left(visible)
    floor_window = _relative_floor_window(relative)
    if not floor_window:
        return None
    low, high, vals = floor_window

    unit_order = (('d', 'd'), ('h', 'h'), ('m', 'm'), ('s', 's'))
    remaining_text = ' '.join(f"{vals[k]}{label}" for k, label in unit_order if k in vals)
    clock_text = _extract_localized_clock_text(visible)
    return {
        'item_id': str(item_id),
        'title': _search_card_title(target_card, item_id),
        'observed_at': observed,
        'low_seconds': low,
        'high_seconds': high,
        'values': vals,
        'remaining_text': remaining_text,
        'clock_text': clock_text,
        'visible_snippet': _auction_timing_snippet(visible),
    }


def _future_reminder_labels(end_time_utc):
    remaining = (_ensure_aware_utc(end_time_utc) - datetime.now(timezone.utc)).total_seconds()
    labels = []
    for mins, label in ((60, '1 час'), (30, '30 минут'), (10, '10 минут'), (5, '5 минут')):
        if remaining > mins * 60:
            labels.append(label)
    return labels


def _auction_exact_search_url(item_id):
    return (
        "https://www.ebay.co.uk/sch/i.html?"
        + urlencode({
            '_nkw': str(item_id),
            'LH_Auction': '1',
            '_sop': '1',
            '_ipg': '60',
        })
    )


def extract_coarse_auction_page_observation(html, final_url, expected_item_id=None, observed_at=None):
    if not html:
        return None
    observed = _ensure_aware_utc(observed_at or datetime.now(timezone.utc))
    soup = BeautifulSoup(html, 'html.parser')
    visible = re.sub(r'\s+', ' ', soup.get_text(' ', strip=True))
    item_id = extract_ebay_item_id_any(final_url or '', html) or expected_item_id
    if expected_item_id and item_id and str(item_id) != str(expected_item_id):
        return None
    if not item_id or not _page_has_active_auction_evidence(html, item_id=item_id):
        return None
    relative = _extract_relative_time_left(visible)
    floor_window = _relative_floor_window(relative)
    if not floor_window:
        return None
    low, high, vals = floor_window
    h1 = soup.find('h1')
    title = re.sub(r'\s+', ' ', h1.get_text(' ', strip=True)).strip() if h1 else ''
    title = _clean_auction_title(title, item_id)
    remaining_text = ' '.join(
        f"{vals[k]}{label}" for k, label in (('d','d'),('h','h'),('m','m'),('s','s')) if k in vals
    )
    clock_text = _extract_localized_clock_text(visible)
    return {
        'item_id': str(item_id),
        'title': title,
        'observed_at': observed,
        'low_seconds': low,
        'high_seconds': high,
        'values': vals,
        'remaining_text': remaining_text,
        'clock_text': clock_text,
        'visible_snippet': _auction_timing_snippet(visible),
    }


def _format_pending_remaining_ru(remaining_text):
    vals = {}
    for number, unit in re.findall(r'(\d+)\s*([dhms])\b', str(remaining_text or ''), re.I):
        vals[unit.lower()] = int(number)
    parts = []
    if 'd' in vals:
        parts.append(f"{vals['d']} дн.")
    if 'h' in vals:
        parts.append(f"{vals['h']} ч.")
    if 'm' in vals:
        parts.append(f"{vals['m']} мин.")
    if 's' in vals and 'd' not in vals:
        parts.append(f"{vals['s']} сек.")
    return ' '.join(parts) or str(remaining_text or 'уточняется')


def _save_pending_from_observation(observation, original_url, notify=True, refined=False):
    item_id = observation['item_id']
    title = _clean_auction_title(observation.get('title'), item_id)
    canonical_url = f"https://www.ebay.co.uk/itm/{item_id}"
    existing_exact = get_exact_auction_by_id(item_id)
    if existing_exact:
        if notify:
            publish_auction_status(
                item_id,
                "ℹ️ <b>Аукцион уже сохранён</b> 🇬🇧\n\n"
                f"📦 <b>{html_lib.escape(_clean_auction_title(existing_exact[2], item_id))}</b>\n\n"
                f"🕒 Окончание по Киеву: {format_kyiv_datetime(existing_exact[3])}",
                reply_markup=auction_message_keyboard(item_id, canonical_url),
                preview_url=canonical_url,
            )
        return 'existing_exact'
    earliest, latest, next_check = save_pending_auction(
        item_id=item_id,
        url=canonical_url,
        title=title,
        observed_at_utc=observation.get('observed_at') or datetime.now(timezone.utc),
        low_seconds=observation['low_seconds'],
        high_seconds=observation['high_seconds'],
        remaining_text=observation.get('remaining_text', ''),
        clock_text=observation.get('clock_text', ''),
    )
    resolved_minute = _pending_window_single_minute(earliest, latest)
    if resolved_minute and 'm' in (observation.get('values') or {}):
        logging.info(
            f"✅ Pending minute-window сошлось в одну UTC минуту: item={item_id}, "
            f"end={resolved_minute.isoformat()}"
        )
        _finish_exact_auction_save(
            item_id, title, resolved_minute,
            'pending:relative_minute_window_consensus',
            notify=(True if refined else notify),
            refined=refined,
        )
        return 'exact'

    logging.info(
        f"🟡 Auction pending сохранён: item={item_id}, remaining={observation.get('remaining_text')!r}, "
        f"window={earliest.isoformat()}..{latest.isoformat()}, next_check={next_check.isoformat()}"
    )
    if notify:
        safe_title = html_lib.escape(title)
        shown_remaining = _format_pending_remaining_ru(observation.get('remaining_text'))
        publish_auction_status(
            item_id,
            "🟡 <b>Аукцион сохранён</b> 🇬🇧\n\n"
            f"📦 <b>{safe_title}</b>\n\n"
            f"⏳ Сейчас по eBay: <b>{html_lib.escape(shown_remaining)}</b>\n\n"
            "🔄 Точное время окончания уточню автоматически.",
            reply_markup=auction_message_keyboard(item_id, canonical_url),
            preview_url=canonical_url,
            persist_pending=True,
        )
    return 'pending'


def _finish_exact_auction_save(item_id, title, end_time_utc, parse_source, notify=True, refined=False):
    end_time_utc = _ensure_aware_utc(end_time_utc).replace(microsecond=0)
    remaining = (end_time_utc - datetime.now(timezone.utc)).total_seconds()
    canonical_url = f"https://www.ebay.co.uk/itm/{item_id}"
    title = _clean_auction_title(title, item_id)
    safe_title = html_lib.escape(title)
    # Берём message_id ДО save_auction_reminder(), потому что exact save удаляет pending row.
    try:
        status_message_id = get_pending_status_message(item_id) or get_auction_link_status_message(item_id)
    except Exception:
        status_message_id = None

    if remaining <= 0:
        delete_pending_auction(item_id, wake=False)
        if notify:
            publish_auction_status(
                item_id,
                "⌛ <b>Этот аукцион уже завершён.</b>\n\n"
                f"🕒 Окончание по Киеву: {format_kyiv_datetime(end_time_utc)}",
                existing_message_id=status_message_id,
                disable_preview=True,
            )
        return False

    if remaining <= 5 * 60:
        delete_pending_auction(item_id, wake=False)
        if notify:
            publish_auction_status(
                item_id,
                "‼️ <b>До конца аукциона меньше 5 минут</b>\n\n"
                f"📦 <b>{safe_title}</b>\n\n"
                f"⏳ Осталось: {format_remaining(remaining, with_seconds=False)}\n\n"
                f"🕒 Окончание по Киеву: {format_kyiv_datetime(end_time_utc)}",
                reply_markup=auction_open_list_keyboard(canonical_url),
                preview_url=canonical_url,
                existing_message_id=status_message_id,
            )
        return False

    save_auction_reminder(item_id, canonical_url, title or f'eBay item {item_id}', end_time_utc)
    mark_auction_status_checked(item_id)
    if notify:
        header = "✅ <b>Время аукциона уточнено</b> 🇬🇧" if refined else "✅ <b>Аукцион сохранён</b> 🇬🇧"
        publish_auction_status(
            item_id,
            header + "\n\n"
            f"📦 <b>{safe_title}</b>\n\n"
            f"🕒 Окончание по Киеву: {format_kyiv_datetime(end_time_utc)}\n\n"
            f"⏳ Осталось: {format_remaining(remaining, with_seconds=False)}\n\n"
            "🔔 Напоминания: <b>60 / 30 / 10 / 5 мин.</b>",
            reply_markup=auction_message_keyboard(item_id, canonical_url),
            preview_url=canonical_url,
            existing_message_id=status_message_id,
        )
    logging.info(
        f"⏰ Auction reminder сохранён: item={item_id}, end_utc={end_time_utc.isoformat()}, "
        f"source={parse_source}; refined={refined}"
    )
    return True


def _evaluate_search_pages_for_auction(search_pages, item_id):
    exact = None
    coarse = None
    had_target = False
    for search_html, _, proxy_used in search_pages:
        observed = datetime.now(timezone.utc)
        target = _search_target_card_tag(search_html, item_id)
        if target:
            had_target = True
        result = parse_auction_search_fallback(search_html, item_id, observed_at=observed)
        if result:
            logging.info(
                f"✅ Auction search-card подтвердил item={item_id}, end={result[2].isoformat()}, "
                f"source={result[3]}, proxy={_proxy_log_name(proxy_used)}"
            )
            exact = result
            break
        observation = extract_coarse_auction_search_observation(
            search_html, item_id, observed_at=observed
        )
        if observation:
            logging.info(
                f"🟡 Auction coarse search-card: item={item_id}, proxy={_proxy_log_name(proxy_used)}, "
                f"remaining={observation['remaining_text']!r}, clock={observation['clock_text']!r}, "
                f"text='{observation['visible_snippet']}'"
            )
            coarse = observation
            break
    return exact, coarse, had_target


def _fetch_exact_item_search_pages(item_id, reserve_if_needed=True):
    search_url = _auction_exact_search_url(item_id)
    # Сначала ОДИН запрос: current fixed, если main сейчас свободен. Если он занят,
    # fetch helper без ожидания берёт небольшой reserve. Никакого полного discovery.
    pages = fetch_auction_pages(
        search_url,
        max_reserve_proxies=2,
        connect_timeout=min(AUCTION_FETCH_CONNECT_TIMEOUT, 4.0),
        read_timeout=min(AUCTION_FETCH_READ_TIMEOUT, 10.0),
        max_pages=1,
        canonicalize_item=False,
        prefer_current_fixed=True,
    )
    exact, coarse, had_target = _evaluate_search_pages_for_auction(pages, item_id)
    if exact or coarse or not reserve_if_needed:
        return exact, coarse, had_target, pages

    # Только если первая HTML вообще не дала target-card/timing — максимум две reserve
    # страницы. Это существенно легче старого 8-proxy item-page + Bid History пути.
    first_pass_hosts = {_proxy_host(p) for _html, _url, p in pages if p}
    reserve_pages = fetch_auction_pages(
        search_url,
        max_reserve_proxies=2,
        connect_timeout=min(AUCTION_FETCH_CONNECT_TIMEOUT, 4.0),
        read_timeout=min(AUCTION_FETCH_READ_TIMEOUT, 10.0),
        max_pages=2,
        canonicalize_item=False,
        prefer_current_fixed=False,
        exclude_hosts=first_pass_hosts,
    )
    exact2, coarse2, had_target2 = _evaluate_search_pages_for_auction(reserve_pages, item_id)
    return exact2, coarse2, had_target or had_target2, pages + reserve_pages


def process_auction_link(url):
    if not _is_allowed_ebay_url(url):
        return 'ignored'

    expected_item_id = extract_ebay_item_id_any(url or '')

    # 1) Самый лёгкий и полезный путь — exact search-card по ItemID. Именно здесь
    # в реальном логе была строка ``Time left 4d 14h left (Sat, 14:26)``.
    search_pages = []
    if expected_item_id:
        exact, coarse, had_target, search_pages = _fetch_exact_item_search_pages(expected_item_id)
        if exact:
            item_id, title, end_time_utc, source, status = exact
            return _finish_exact_auction_save(item_id, title, end_time_utc, source, notify=True)
        if coarse:
            return _save_pending_from_observation(coarse, url, notify=True)

    # 2) Если search-card недоступна/изменилась, оставляем консервативный item-page
    # fallback. Bid History больше НЕ вызываем: логи доказали redirect на Sign In и
    # лишний proxy-трафик без полезного end time.
    search_hosts = {_proxy_host(p) for _html, _url, p in search_pages if p}
    pages = fetch_auction_pages(
        url,
        max_reserve_proxies=min(3, AUCTION_FETCH_MAX_PROXIES),
        max_pages=min(2, AUCTION_VERIFY_MAX_PAGES),
        prefer_current_fixed=False,
        exclude_hosts=search_hosts,
    )

    parsed_results = []
    timing_support_htmls = [html for html, _, _ in pages if html]
    coarse_page_observation = None
    for html, final_url, proxy_used in pages:
        parse_url = final_url if extract_ebay_item_id_any(final_url or '') else url
        result = parse_auction_page(html, parse_url)
        item_id, title, end_time_utc, parse_source, auction_status = result
        logging.info(
            f"🧪 Auction parse: proxy={_proxy_log_name(proxy_used)}, item={item_id}, status={auction_status}, "
            f"source={parse_source}, end={end_time_utc.isoformat() if end_time_utc else None}"
        )
        if expected_item_id and item_id and item_id != expected_item_id:
            logging.warning(
                f"Auction URL mismatch: ожидали item={expected_item_id}, получили item={item_id}; страницу игнорируем"
            )
            continue
        parsed_results.append((result, proxy_used))
        if not end_time_utc and not coarse_page_observation:
            coarse_page_observation = extract_coarse_auction_page_observation(
                html, parse_url, expected_item_id=expected_item_id, observed_at=datetime.now(timezone.utc)
            )

    for result, _proxy_used in parsed_results:
        item_id, title, end_time_utc, parse_source, auction_status = result
        if auction_status == 'active' and item_id and end_time_utc:
            return _finish_exact_auction_save(item_id, title, end_time_utc, parse_source, notify=True)

    if timing_support_htmls:
        timer_end, timer_source, timer_observations = _resolve_localized_timer_evidence(
            timing_support_htmls
        )
        _log_timer_observations(expected_item_id, timer_observations)
        if timer_end and any(
            _page_has_active_auction_evidence(h, item_id=expected_item_id)
            for h in timing_support_htmls
        ):
            representative = next(
                (r for r, _ in parsed_results if r[0] and (not expected_item_id or r[0] == expected_item_id)),
                None,
            )
            timer_item = expected_item_id or (representative[0] if representative else None)
            timer_title = (representative[1] if representative else '') or f'eBay item {timer_item}'
            if timer_item:
                return _finish_exact_auction_save(
                    timer_item, timer_title, timer_end, timer_source, notify=True
                )

    if coarse_page_observation:
        return _save_pending_from_observation(coarse_page_observation, url, notify=True)

    if not pages:
        logging.warning(
            f"Auction item={expected_item_id}: ни search-card, ни item-page сейчас не получены; "
            "оставляем ссылку в надёжной очереди без отказа пользователю"
        )
        return 'retry'

    statuses = [result[4] for result, _ in parsed_results]
    if 'time_conflict' in statuses:
        # Конфликт времени может быть кратковременным из-за разных eBay edge/proxy шаблонов.
        # Не тревожим пользователя и не угадываем: повторяем job позже.
        logging.warning(
            f"Auction item={expected_item_id}: временный time_conflict; оставляем в очереди"
        )
        return 'retry'
    if 'ended' in statuses:
        ended_result = next(r for r, _ in parsed_results if r[4] == 'ended')
        line = (
            f"\n\n🕒 Окончание по Киеву: {format_kyiv_datetime(ended_result[2])}"
            if ended_result[2] else ''
        )
        msg = "⌛ <b>Этот аукцион уже завершён.</b>" + line
        publish_auction_status(
            expected_item_id or ended_result[0], msg,
            disable_preview=True,
        )
        return 'ended'
    if 'not_auction' in statuses:
        publish_auction_status(
            expected_item_id,
            "ℹ️ <b>Это не активный аукцион со ставками.</b>\n\n"
            "Buy It Now / Best Offer без режима торгов не сохраняется.",
            disable_preview=True,
        )
        return 'not_auction'

    logging.warning(
        f"Auction не подтверждён после lightweight search и {len(pages)} item-page вариантов; "
        f"item={expected_item_id}, statuses={statuses}. Оставляем job в очереди и повторим позже."
    )
    return 'retry'


def auction_link_worker():
    logging.info("🔗 Worker надёжной очереди eBay-аукционов запущен")
    db_ready_event.wait()
    while True:
        try:
            # Clear ДО чтения БД: если enqueue случится после этого момента,
            # Event останется установленным и последующий wait завершится сразу.
            auction_link_wakeup_event.clear()
            job, wait_seconds = get_next_auction_link_job_state()
            if job is None:
                auction_link_wakeup_event.wait(
                    timeout=max(1.0, min(float(wait_seconds), float(AUCTION_LINK_WORKER_IDLE)))
                )
                continue

            queue_key, item_id, url, status_message_id, attempt_count, created_at = job
            try:
                result = process_auction_link(url)
            except Exception as e:
                logging.error(f"Ошибка обработки auction URL {url}: {e}", exc_info=True)
                postpone_auction_link_job(queue_key, attempt_count, 'internal_error')
                continue

            if result == 'retry':
                postpone_auction_link_job(queue_key, attempt_count, 'proxy_or_html_temporarily_unavailable')
                continue

            if result == 'pending' and item_id:
                # Перед удалением queue-row ещё раз переносим его Telegram message_id
                # в pending. Это страхует редкий DB-сбой во время publish/edit.
                try:
                    transfer_auction_link_status_to_pending(item_id)
                except Exception as e:
                    logging.warning(f"Не удалось перенести auction status_message_id в pending: {e}")

            delete_auction_link_job(queue_key)
            logging.info(f"✅ Auction queue job завершён: key={queue_key}, result={result!r}")

        except psycopg2.OperationalError as e:
            # Job хранится в PostgreSQL, поэтому при временном сетевом сбое ничего
            # не теряется. Не спамим полным traceback каждые несколько секунд:
            # ждём немного или просыпаемся мгновенно по новой пользовательской ссылке.
            logging.warning(
                f"⚠️ Aiven временно недоступен для auction queue: {e}. "
                f"Очередь сохранена; повтор через ≤{AUCTION_LINK_DB_ERROR_WAIT} сек."
            )
            auction_link_wakeup_event.wait(timeout=AUCTION_LINK_DB_ERROR_WAIT)

        except Exception as e:
            logging.error(f"Ошибка auction link queue worker: {e}", exc_info=True)
            auction_link_wakeup_event.wait(timeout=AUCTION_LINK_DB_ERROR_WAIT)


def send_auction_list():
    try:
        exact_rows = list_active_auctions(limit=20)
        pending_rows = list_pending_auctions(limit=20)
        queued_rows = list_queued_auction_links(limit=20)
    except Exception as e:
        logging.error(f"Не удалось получить список аукционов: {e}")
        send_telegram_message("❌ Не удалось сейчас прочитать список аукционов из базы.")
        return

    exact_ids = {str(row[0]) for row in exact_rows}
    pending_ids = {str(row[0]) for row in pending_rows}
    # На границе успешной обработки queue-row может существовать ещё долю секунды;
    # не показываем один ItemID дважды.
    queued_rows = [row for row in queued_rows if str(row[0]) not in exact_ids | pending_ids]

    if not exact_rows and not pending_rows and not queued_rows:
        send_telegram_message("📭 <b>Активных аукционов сейчас нет.</b>")
        return

    now = datetime.now(timezone.utc)
    entries = []
    for row in exact_rows:
        entries.append(('exact', _ensure_aware_utc(row[3]), row))
    for row in pending_rows:
        entries.append(('pending', _ensure_aware_utc(row[3]), row))
    # Ещё не проверенные ссылки идут после подтверждённых аукционов.
    for row in queued_rows:
        entries.append(('queued', datetime.max.replace(tzinfo=timezone.utc), row))
    entries.sort(key=lambda x: x[1])
    entries = entries[:20]

    parts = ["⏰ <b>Мои аукционы UK</b> 🇬🇧\n"]
    delete_buttons = []
    for idx, (kind, _sort_time, row) in enumerate(entries, 1):
        if kind == 'exact':
            item_id, url, title, end_time = row
            end_time = _ensure_aware_utc(end_time)
            remaining = max(0, (end_time - now).total_seconds())
            title = _clean_auction_title(title, item_id)
            short_title = title if len(title) <= 100 else title[:97] + '…'
            parts.append(
                f"\n{idx}) <b>{html_lib.escape(short_title)}</b>\n\n"
                f"⏳ Осталось: {format_remaining(remaining, with_seconds=False)}\n"
                f"🕒 По Киеву: {format_kyiv_datetime(end_time)}\n"
                f"🔗 {html_lib.escape(url)}\n"
            )
        elif kind == 'pending':
            item_id, url, title, earliest, latest, remaining_text, clock_text, next_check = row
            title = _clean_auction_title(title, item_id)
            short_title = title if len(title) <= 100 else title[:97] + '…'
            shown = _format_pending_remaining_ru(remaining_text)
            parts.append(
                f"\n{idx}) <b>{html_lib.escape(short_title)}</b>\n\n"
                f"⏳ Осталось: {html_lib.escape(shown)}\n"
                f"🕒 По Киеву: уточняется\n"
                f"🔗 {html_lib.escape(url)}\n"
            )
        else:
            item_id, url, created_at, next_attempt = row
            parts.append(
                f"\n{idx}) <b>Название уточняется</b>\n\n"
                f"⏳ Осталось: проверяется\n"
                f"🕒 По Киеву: уточняется\n"
                f"🔗 {html_lib.escape(url)}\n"
            )
        delete_buttons.append(
            {'text': f'❌ Удалить #{idx}', 'callback_data': f'aucdel:{item_id}'}
        )

    keyboard = []
    for i in range(0, len(delete_buttons), 2):
        keyboard.append(delete_buttons[i:i + 2])
    keyboard.append([{'text': '🗑 Удалить все', 'callback_data': 'aucdelall:ask'}])

    send_telegram_message(
        ''.join(parts),
        reply_markup={'inline_keyboard': keyboard},
        disable_preview=True,
    )


def _notify_auction_closed_early(item_id, url, title, expected_end, detected_end=None):
    # DELETE ... RETURNING делает уведомление однократным даже если два worker'а
    # почти одновременно обнаружили закрытие.
    if not delete_auction_reminder(item_id):
        return False
    title = _clean_auction_title(title, item_id)
    safe_title = html_lib.escape(title)
    msg = (
        "🚫 <b>Аукцион завершён раньше времени</b>\n\n"
        f"📦 <b>{safe_title}</b>\n\n"
        f"🕒 Было запланировано: {format_kyiv_datetime(expected_end)} (Киев)"
    )
    if detected_end:
        msg += f"\n🕒 eBay: {format_kyiv_datetime(detected_end)} (Киев)"
    send_telegram_message(
        msg,
        reply_markup={'inline_keyboard': [[
            {'text': '🔗 Открыть eBay', 'url': url},
            {'text': '📋 Все аукционы', 'callback_data': 'auclist'},
        ]]},
        disable_preview=True,
    )
    return True


def verify_saved_auction(item_id, url, title, expected_end, max_reserve_proxies=1, notify_time_change=True):
    """Проверяет сохранённый лот. Никогда не удаляет запись из-за network/proxy ошибки."""
    html, final_url, proxy_used = fetch_auction_page(
        url,
        max_reserve_proxies=max_reserve_proxies,
        connect_timeout=AUCTION_STATUS_CONNECT_TIMEOUT,
        read_timeout=AUCTION_STATUS_READ_TIMEOUT,
    )
    if not html:
        return 'unverified'

    parse_url = final_url if extract_ebay_item_id_any(final_url or '') else url
    parsed_id, parsed_title, parsed_end, source, status = parse_auction_page(html, parse_url)
    if parsed_id and parsed_id != item_id:
        logging.warning(f"Auction status mismatch: expected {item_id}, page {parsed_id}")
        return 'unverified'

    now = datetime.now(timezone.utc)
    expected_end = _ensure_aware_utc(expected_end)

    if status == 'ended':
        # Сообщаем именно о досрочном закрытии. Если подошло обычное время конца,
        # scheduler просто удалит запись как завершённую.
        if now < expected_end - timedelta(seconds=30):
            _notify_auction_closed_early(item_id, url, title, expected_end, parsed_end)
            return 'closed'
        delete_auction_reminder(item_id)
        return 'ended_normally'

    if status == 'active' and parsed_end:
        exists, changed, old_end, new_end = update_verified_auction(
            item_id, parsed_title or title, parsed_end
        )
        if not exists:
            return 'missing'
        if changed:
            if notify_time_change:
                delta_seconds = int((new_end - old_end).total_seconds())
                delta_minutes = max(1, abs(delta_seconds) // 60)
                direction = 'позже' if delta_seconds > 0 else 'раньше'
                clean_title = _clean_auction_title(parsed_title or title, item_id)
                send_telegram_message(
                    "🔄 <b>Время аукциона изменилось</b>\n\n"
                    f"📦 <b>{html_lib.escape(clean_title)}</b>\n\n"
                    f"🕒 Новое время по Киеву: {format_kyiv_datetime(new_end)}\n\n"
                    f"↔️ {delta_minutes} мин. {direction}\n"
                    "🔔 Напоминания пересчитаны.",
                    reply_markup=auction_message_keyboard(item_id, url),
                    preview_url=url,
                )
            logging.info(
                f"🔄 Auction {item_id}: end time changed {old_end.isoformat()} -> {new_end.isoformat()} ({source})"
            )
            return 'time_changed'
        return 'active'

    # Страница получена, но нет достаточного положительного подтверждения закрытия —
    # НЕ удаляем лот. Это защищает от временной смены HTML eBay.
    mark_auction_status_checked(item_id)
    return status


def refine_pending_auction(item_id, url, title, end_earliest, end_latest, remaining_text='', clock_text=''):
    """Уточняет pending только lightweight search-card запросом; Bid History не нужен."""
    exact, coarse, had_target, pages = _fetch_exact_item_search_pages(item_id)
    if exact:
        exact_id, exact_title, exact_end, source, status = exact
        logging.info(
            f"✅ Pending auction уточнён: item={item_id}, end={exact_end.isoformat()}, source={source}"
        )
        return 'exact' if _finish_exact_auction_save(
            exact_id, exact_title or title, exact_end, source, notify=True, refined=True
        ) else 'finished'

    if coarse:
        pending_state = _save_pending_from_observation(coarse, url, notify=False, refined=True)
        if pending_state == 'exact':
            return 'exact'
        logging.info(
            f"🟡 Pending auction пока coarse: item={item_id}, remaining={coarse['remaining_text']!r}; "
            f"следующая проверка будет рассчитана заново"
        )
        return 'coarse'

    # Network/HTML ambiguity никогда не удаляет pending. Близко к окончанию нельзя
    # откладывать на 10 минут: иначе можно перескочить порог 60/30/10/5.
    now_utc = datetime.now(timezone.utc)
    latest_utc = _ensure_aware_utc(end_latest)
    remaining_latest = (latest_utc - now_utc).total_seconds()
    if remaining_latest <= 2 * 3600:
        retry_seconds = AUCTION_PENDING_CLOSE_RETRY
    elif remaining_latest <= 24 * 3600:
        retry_seconds = min(AUCTION_PENDING_RETRY, 180)
    else:
        retry_seconds = AUCTION_PENDING_RETRY
    postpone_pending_auction(item_id, retry_seconds)
    logging.info(
        f"🟡 Pending auction {item_id}: точная search-card пока недоступна; "
        f"повтор через {retry_seconds} сек."
    )
    return 'unverified'


def _cleanup_stale_pending_auctions():
    """Чистит только окна, которые гарантированно закончились >30 мин назад."""
    with get_db_connection('ebay_uk_auction_pending_cleanup') as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM auction_pending
                WHERE end_latest_utc <= NOW() - INTERVAL '30 minutes'
                RETURNING item_id, title
                """
            )
            rows = cur.fetchall()
        conn.commit()
    if rows:
        logging.info(f"🧹 Удалено просроченных pending-аукционов: {len(rows)}")
    return rows


def auction_status_worker():
    """Контроль exact-аукционов + редкое автоматическое уточнение pending."""
    logging.info("🛰 Worker контроля сохранённых аукционов запущен")
    db_ready_event.wait()
    while True:
        try:
            elapsed, _, had_success = _connection_outage_snapshot()
            healthy = is_paused or (had_success and elapsed < 180)
            if healthy:
                # Сначала максимум ОДИН pending: это редкая лёгкая search-card проверка.
                pending_rows = get_due_pending_auctions(limit=1)
                for row in pending_rows:
                    item_id, url, title, earliest, latest, remaining_text, clock_text, _next_check = row
                    refine_pending_auction(
                        item_id, url, title,
                        _ensure_aware_utc(earliest), _ensure_aware_utc(latest),
                        remaining_text, clock_text,
                    )
                    time.sleep(random.uniform(0.6, 1.0))

                rows = get_auctions_for_status_check(limit=100)
                now = datetime.now(timezone.utc)
                due = []
                for item_id, url, title, end_time, last_check in rows:
                    end_time = _ensure_aware_utc(end_time)
                    remaining = (end_time - now).total_seconds()
                    interval = _status_check_interval_seconds(remaining)
                    if interval is None:
                        continue
                    if last_check is None or (now - _ensure_aware_utc(last_check)).total_seconds() >= interval:
                        due.append((item_id, url, title, end_time))
                    if len(due) >= AUCTION_STATUS_BATCH:
                        break

                for item_id, url, title, end_time in due:
                    verify_saved_auction(
                        item_id, url, title, end_time,
                        max_reserve_proxies=1,
                        notify_time_change=True,
                    )
                    time.sleep(random.uniform(0.8, 1.4))

                _cleanup_stale_pending_auctions()
        except Exception as e:
            logging.error(f"Ошибка auction status worker: {e}", exc_info=True)
        auction_status_wakeup_event.wait(timeout=AUCTION_STATUS_TICK)
        auction_status_wakeup_event.clear()


DELETE_CONFIRM_WINDOW_SECONDS = 20
_delete_confirm_lock = threading.Lock()
_delete_confirm_until = {}


def _delete_confirmation_key(callback, item_id):
    user_id = str(((callback.get('from') or {}).get('id')) or '')
    return user_id, str(item_id)


def _second_delete_tap_confirmed(callback, item_id):
    """Первый tap только спрашивает, второй в течение 20 сек. подтверждает удаление."""
    now = time.monotonic()
    key = _delete_confirmation_key(callback, item_id)
    with _delete_confirm_lock:
        # Небольшая уборка in-memory state; после restart всё сбрасывается в безопасную
        # сторону — снова потребуется два нажатия.
        expired = [k for k, until in _delete_confirm_until.items() if until < now]
        for k in expired:
            _delete_confirm_until.pop(k, None)

        until = _delete_confirm_until.get(key)
        if until is not None and until >= now:
            _delete_confirm_until.pop(key, None)
            return True

        _delete_confirm_until[key] = now + DELETE_CONFIRM_WINDOW_SECONDS
        return False


DELETE_ALL_CONFIRM_WINDOW_SECONDS = 30
_delete_all_confirm_lock = threading.Lock()
_delete_all_confirm_until = {}


def _delete_all_user_key(callback):
    return str(((callback.get('from') or {}).get('id')) or '')


def _start_delete_all_confirmation(callback):
    key = _delete_all_user_key(callback)
    if not key:
        return False
    now = time.monotonic()
    with _delete_all_confirm_lock:
        expired = [k for k, until in _delete_all_confirm_until.items() if until < now]
        for k in expired:
            _delete_all_confirm_until.pop(k, None)
        _delete_all_confirm_until[key] = now + DELETE_ALL_CONFIRM_WINDOW_SECONDS
    return True


def _consume_delete_all_confirmation(callback):
    key = _delete_all_user_key(callback)
    now = time.monotonic()
    with _delete_all_confirm_lock:
        until = _delete_all_confirm_until.pop(key, None)
    return until is not None and until >= now


def _cancel_delete_all_confirmation(callback):
    key = _delete_all_user_key(callback)
    with _delete_all_confirm_lock:
        _delete_all_confirm_until.pop(key, None)


def _send_delete_all_confirmation(callback):
    _start_delete_all_confirmation(callback)
    send_telegram_message(
        "❓ <b>Удалить все аукционы?</b>",
        reply_markup={
            'inline_keyboard': [[
                {'text': '✅ Да', 'callback_data': 'aucdelall:yes'},
                {'text': '❌ Нет', 'callback_data': 'aucdelall:no'},
            ]]
        },
        disable_preview=True,
    )


def _mark_deleted_button(message, item_id):
    """После подтверждения не стирает всю клавиатуру /list, а помечает только этот лот."""
    markup = message.get('reply_markup') or {}
    rows = markup.get('inline_keyboard') or []
    changed = False
    new_rows = []
    target = f'aucdel:{item_id}'
    for row in rows:
        new_row = []
        for button in row:
            button = dict(button)
            if button.get('callback_data') == target:
                label = str(button.get('text') or '')
                suffix = ''
                m = re.search(r'(#\d+)\b', label)
                if m:
                    suffix = ' ' + m.group(1)
                button = {'text': f'✅ Удалён{suffix}', 'callback_data': 'aucnoop'}
                changed = True
            new_row.append(button)
        if new_row:
            new_rows.append(new_row)
    if changed:
        edit_message_reply_markup(message.get('message_id'), {'inline_keyboard': new_rows})


def handle_telegram_callback(callback):
    callback_id = callback.get('id')
    data = str(callback.get('data') or '')
    message = callback.get('message') or {}
    chat_id = str((message.get('chat') or {}).get('id') or '')
    if chat_id != str(TELEGRAM_CHAT_ID):
        answer_callback_query(callback_id, "Недоступно", show_alert=False)
        return

    if data == 'auclist':
        answer_callback_query(callback_id, "Открываю список…")
        send_auction_list()
        return

    if data == 'aucdelall:ask':
        answer_callback_query(callback_id, "Нужно подтверждение")
        _send_delete_all_confirmation(callback)
        return

    if data == 'aucdelall:no':
        _cancel_delete_all_confirmation(callback)
        answer_callback_query(callback_id, "Удаление отменено")
        edit_message_reply_markup(message.get('message_id'), {'inline_keyboard': []})
        return

    if data == 'aucdelall:yes':
        if not _consume_delete_all_confirmation(callback):
            answer_callback_query(
                callback_id,
                "Подтверждение истекло. Нажмите «Удалить все» ещё раз.",
                show_alert=True,
            )
            return
        exact_count, pending_count, queued_count = delete_all_auctions()
        total = exact_count + pending_count + queued_count
        answer_callback_query(callback_id, "Все аукционы удалены ✅")
        edit_message_reply_markup(message.get('message_id'), {'inline_keyboard': []})
        send_telegram_message(
            "✅ Все аукционы удалены." if total else "ℹ️ Активных аукционов уже нет."
        )
        logging.info(
            f"🗑 Удалены все аукционы по подтверждению пользователя: "
            f"exact={exact_count}, pending={pending_count}, queued={queued_count}"
        )
        return

    if data == 'aucnoop':
        answer_callback_query(callback_id)
        return

    m = re.fullmatch(r'aucdel:(\d{9,19})', data)
    if m:
        item_id = m.group(1)
        if not _second_delete_tap_confirmed(callback, item_id):
            answer_callback_query(
                callback_id,
                "Вы точно хотите удалить этот аукцион? Нажмите «Удалить» ещё раз в течение 20 секунд.",
                show_alert=True,
            )
            return

        if delete_auction_any(item_id):
            answer_callback_query(callback_id, "Аукцион удалён ✅")
            _mark_deleted_button(message, item_id)
        else:
            answer_callback_query(callback_id, "Уже удалён или завершён")
        return

    answer_callback_query(callback_id)


def auction_reminder_worker():
    """Scheduler reminders 60/30/10/5 мин. После успешного 5-мин. сообщения запись удаляется."""
    logging.info("⏰ Планировщик auction reminders запущен")
    db_ready_event.wait()
    conn = None
    while True:
        next_sleep = AUCTION_SCHEDULER_MAX_SLEEP
        try:
            if conn is None or conn.closed:
                conn = get_db_connection('ebay_uk_auction_scheduler')

            now = datetime.now(timezone.utc)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT item_id, url, title, end_time_utc,
                           reminder_60_sent, reminder_30_sent, reminder_10_sent, reminder_5_sent
                    FROM auction_reminders
                    WHERE end_time_utc > %s
                    ORDER BY end_time_utc ASC
                    LIMIT 200
                    """,
                    (now,),
                )
                rows = cur.fetchall()

            for row in rows:
                item_id, url, title, end_time, sent60, sent30, sent10, sent5 = row
                end_time = _ensure_aware_utc(end_time)
                remaining = (end_time - now).total_seconds()
                if remaining <= 0:
                    continue

                sent_map = {60: sent60, 30: sent30, 10: sent10, 5: sent5}
                due_minutes = []
                for minutes in AUCTION_REMINDER_MINUTES:
                    if sent_map[minutes]:
                        continue
                    trigger_time = end_time - timedelta(minutes=minutes)
                    seconds_to_trigger = (trigger_time - now).total_seconds()
                    if seconds_to_trigger <= 0:
                        due_minutes.append(minutes)
                    else:
                        next_sleep = min(next_sleep, max(1.0, seconds_to_trigger))

                if not due_minutes:
                    next_sleep = min(next_sleep, max(1.0, remaining))
                    continue

                # Перечитываем строку перед отправкой: status-worker мог изменить время/удалить лот.
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT end_time_utc, reminder_60_sent, reminder_30_sent,
                               reminder_10_sent, reminder_5_sent, title, url
                        FROM auction_reminders WHERE item_id=%s
                        """,
                        (item_id,),
                    )
                    current = cur.fetchone()
                if not current:
                    continue

                current_end, c60, c30, c10, c5, current_title, current_url = current
                current_end = _ensure_aware_utc(current_end)
                if abs((current_end - end_time).total_seconds()) > 5:
                    # Время изменилось — новый цикл пересчитает пороги.
                    auction_reminder_wakeup_event.set()
                    continue

                now_send = datetime.now(timezone.utc)
                remaining_send = (current_end - now_send).total_seconds()
                if remaining_send <= 0:
                    continue

                current_sent = {60: c60, 30: c30, 10: c10, 5: c5}
                due_now = [
                    m for m in AUCTION_REMINDER_MINUTES
                    if not current_sent[m]
                    and now_send >= current_end - timedelta(minutes=m)
                ]
                if not due_now:
                    continue

                # Если во время restart/короткого сбоя пропущено несколько порогов,
                # НЕ отправляем пачку старых сообщений. Берём только самый актуальный.
                latest_minutes = min(due_now)
                trigger_time = current_end - timedelta(minutes=latest_minutes)
                lateness = max(0.0, (now_send - trigger_time).total_seconds())
                label = '1 час' if latest_minutes == 60 else f'{latest_minutes} минут'

                clean_current_title = _clean_auction_title(current_title or title, item_id)
                safe_title = html_lib.escape(clean_current_title)

                # На 5 мин это последнее сообщение: после успешной доставки удаляем запись,
                # поэтому дальше никаких status-check и reminders этот аукцион не создаёт.
                is_final_five = latest_minutes == 5
                urgent = '‼️ ' if is_final_five else ''
                # V6.15: сам порог (например «за 30 минут») обычным шрифтом;
                # фактический остаток времени выделяем жирным, чтобы быстрее считывался.
                msg = (
                    f"{urgent}⏰ <b>Напоминание об аукционе UK</b> 🇬🇧\n\n"
                    f"🔔 Плановое напоминание за {label}\n\n"
                    f"📦 <b>{safe_title}</b>\n\n"
                    f"⏳ До окончания сейчас: <b>{format_remaining(remaining_send, with_seconds=False)}</b>\n\n"
                    f"🕒 Окончание по Киеву: {format_kyiv_datetime(current_end)}"
                )
                keyboard = (
                    auction_open_list_keyboard(current_url)
                    if is_final_five
                    else auction_message_keyboard(item_id, current_url)
                )

                if send_telegram_message(msg, reply_markup=keyboard, disable_preview=True):
                    if is_final_five:
                        delete_auction_reminder(item_id)
                        logging.info(f"📨 Auction {item_id}: отправлено финальное reminder 5 мин; запись удалена")
                    else:
                        _mark_reminders_sent(conn, item_id, due_now)
                        logging.info(
                            f"📨 Auction {item_id}: отправлено актуальное reminder {latest_minutes} мин; "
                            f"закрыты пороги {sorted(due_now, reverse=True)}"
                        )
                else:
                    # Для аукциона лучше повторить позже, чем навсегда потерять важное reminder.
                    next_sleep = min(next_sleep, 15)

                next_sleep = min(next_sleep, max(1.0, remaining_send))

            _delete_expired_auctions(conn, datetime.now(timezone.utc))

        except Exception as e:
            logging.error(f"Ошибка auction reminder scheduler: {e}", exc_info=True)
            try:
                if conn is not None:
                    conn.rollback()
                    conn.close()
            except Exception:
                pass
            conn = None
            next_sleep = 15

        auction_reminder_wakeup_event.wait(
            timeout=max(1.0, min(float(next_sleep), AUCTION_SCHEDULER_MAX_SLEEP))
        )
        auction_reminder_wakeup_event.clear()


def telegram_listener():
    global is_paused
    logging.info("🔁 Поток слушателя команд Telegram запущен")
    try:
        last_update_id = int(get_bot_state('telegram_last_update_id', '0') or 0)
    except Exception as e:
        logging.warning(f"Не удалось прочитать telegram_last_update_id, начинаем с 0: {e}")
        last_update_id = 0

    while True:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
            params = {'offset': last_update_id + 1, 'timeout': 30}
            r = requests.get(url, params=params, timeout=35)
            if r.status_code != 200:
                logging.warning(f"Telegram getUpdates HTTP {r.status_code}: {r.text[:300]}")
                time.sleep(3)
                continue

            updates = r.json().get('result', [])
            for update in updates:
                update_id = int(update.get('update_id', 0))
                if update_id <= last_update_id:
                    continue

                try:
                    callback = update.get('callback_query')
                    if callback:
                        handle_telegram_callback(callback)
                    else:
                        message = update.get('message')
                        if message and str(message.get('chat', {}).get('id')) == str(TELEGRAM_CHAT_ID):
                            text = message.get('text', '').strip()
                            if text == '/stop':
                                is_paused = True
                                send_telegram_message(
                                    "⏸ Основной мониторинг новых товаров приостановлен. "
                                    "Напоминания об уже сохранённых аукционах продолжают работать. "
                                    "Для возобновления отправьте /start"
                                )
                                logging.info("Команда /stop - пауза основного мониторинга")
                            elif text == '/start':
                                is_paused = False
                                reset_connection_watch_after_manual_resume()
                                send_telegram_message("▶ Основной мониторинг продолжает работу")
                                logging.info("Команда /start - продолжение; watchdog-таймер перезапущен")
                            elif text in ('/auctions', '/list'):
                                send_auction_list()
                            elif text.startswith('/delauction'):
                                parts = text.split(maxsplit=1)
                                if len(parts) != 2 or not re.fullmatch(r'\d{9,19}', parts[1].strip()):
                                    send_telegram_message("Использование: <code>/delauction НОМЕР_ЛОТА</code>")
                                else:
                                    item_id = parts[1].strip()
                                    if delete_auction_any(item_id):
                                        send_telegram_message(f"❌ Аукцион <b>{item_id}</b> удалён из списка.")
                                    else:
                                        send_telegram_message(f"ℹ️ Активный лот <b>{item_id}</b> в списке не найден.")
                            else:
                                ebay_urls = extract_ebay_urls(text)
                                if ebay_urls:
                                    for ebay_url in ebay_urls[:20]:
                                        key, item_id, canonical_url, existing_message_id, is_new = enqueue_auction_link(ebay_url)
                                        # Одно компактное жёлтое status-сообщение. Оно не накапливается:
                                        # после получения результата бот отредактирует ЭТО ЖЕ сообщение в pending/exact.
                                        if not existing_message_id:
                                            msg_id = send_telegram_message(
                                                "🟡 <b>Аукцион добавлен</b> 🇬🇧\n\n"
                                                "⏳ Проверяю время окончания автоматически.",
                                                reply_markup=auction_queue_keyboard(canonical_url),
                                                preview_url=canonical_url,
                                                return_message_id=True,
                                            )
                                            if msg_id:
                                                set_auction_link_status_message(key, msg_id)
                except Exception as e:
                    logging.error(f"Ошибка обработки Telegram update {update_id}: {e}", exc_info=True)

                # Подтверждаем обработанный update в нашей БД. Это не связано с seen_items:
                # здесь защищаем только входящие команды/кнопки от повторного проигрывания после deploy.
                # V6.17: break больше не находится внутри finally — убираем SyntaxWarning и
                # не допускаем подавления неожиданного исключения управляющим оператором finally.
                try:
                    set_bot_state('telegram_last_update_id', update_id)
                    last_update_id = update_id
                except Exception as e:
                    logging.error(f"Не удалось сохранить Telegram update_id={update_id}: {e}")
                    # Не повышаем offset в памяти без БД: лучше повторить одну команду,
                    # чем потерять входящее действие пользователя при аварийном restart.
                    break
            time.sleep(0.5)
        except Exception as e:
            logging.error(f"Ошибка в слушателе Telegram: {e}")
            time.sleep(5)


# ============ ЗАПРОС К EBAY ============

def _response_title(html):
    if not html:
        return ""
    match = re.search(r'<title[^>]*>(.*?)</title>', html, re.I | re.S)
    if not match:
        return ""
    return re.sub(r'\s+', ' ', match.group(1)).strip()[:160]


def _is_ebay_block_page(response):
    """Определяем именно защитную страницу eBay, а не слово robot в обычном JS."""
    text_lower = (response.text or "").lower()
    title_lower = _response_title(response.text).lower()
    final_url = str(getattr(response, 'url', '') or '').lower()

    if 'pardon our interruption' in text_lower:
        return True, 'pardon our interruption'
    if 'something about your browser made us think you were a bot' in text_lower:
        return True, 'browser made us think you were a bot'
    if 'access denied' in title_lower:
        return True, 'title: access denied'
    if ('captcha' in final_url or 'challenge' in final_url) and 'ebay' in final_url:
        return True, f'challenge url: {final_url[:120]}'

    return False, None


def _looks_like_search_results(html):
    """Не фиксируем пару на случайной HTTP 200 странице без карточек eBay."""
    if not html:
        return False
    lower = html.lower()
    return ('/itm/' in lower) and (
        's-item' in lower or
        's-card' in lower or
        'srp-river-results' in lower
    )


def _create_session(proxy, profile, timeout=None):
    # Session сохраняет cookies и TCP/TLS соединение уже найденного рабочего IP.
    if timeout is None:
        timeout = (FIXED_CONNECT_TIMEOUT, FIXED_READ_TIMEOUT)
    kwargs = {
        'headers': {'Accept-Language': 'en-GB,en;q=0.9'},
        'impersonate': profile['impersonate'],
        'verify': True,
        'timeout': timeout,
        'allow_redirects': True,
        'trust_env': False,
    }
    if proxy:
        kwargs['proxy'] = proxy
    return cffi_requests.Session(**kwargs)


def _make_request(proxy, profile, session=None, timeout=None):
    """
    Возвращает (result, html, session), где result:
      success       - нормальная выдача eBay
      blocked       - HTTP 403 / anti-bot
      rate_limited  - HTTP 429
      proxy_timeout  - connect/read timeout
      proxy_ssl      - SSL certificate / MITM proxy
      proxy_rejected - CONNECT tunnel 400/405/500/aborted: endpoint не годится для HTTPS
      proxy_error    - SOCKS/reset/прочая transport ошибка
      profile_error - неподдерживаемый fingerprint
      http_error    - прочий HTTP/неожиданный ответ
    """
    if timeout is None:
        timeout = (FIXED_CONNECT_TIMEOUT, FIXED_READ_TIMEOUT)

    own_session = session is None
    if session is None:
        try:
            session = _create_session(proxy, profile, timeout=timeout)
        except Exception as e:
            logging.error(f"Не удалось создать session для {profile['name']}: {e}")
            return 'profile_error', None, None

    try:
        # Параметр request() переопределяет timeout Session — это позволяет
        # discovery быстро отбрасывать медленные proxy, не затрагивая fixed session.
        response = session.get(EBAY_SEARCH_URL, timeout=timeout)

        title = _response_title(response.text)
        final_url = str(getattr(response, 'url', '') or '')
        body_len = len(response.content or b'')

        logging.info(
            f"🌐 eBay ответ: HTTP {response.status_code}, bytes={body_len}, "
            f"title={title!r}, final_url={final_url[:220]}"
        )

        if response.status_code == 200:
            blocked, reason = _is_ebay_block_page(response)
            if blocked:
                logging.warning(
                    f"🚫 ПОДТВЕРЖДЁННАЯ защита eBay ({reason}) "
                    f"для прокси {_proxy_log_name(proxy)}, профиль {profile['name']}"
                )
                if own_session:
                    close_session(session)
                return 'blocked', None, None if own_session else session

            if not _looks_like_search_results(response.text):
                logging.warning(
                    f"⚠️ HTTP 200, но выдача eBay не распознана "
                    f"(bytes={body_len}, title={title!r})"
                )
                if own_session:
                    close_session(session)
                return 'http_error', None, None if own_session else session

            logging.info(f"✅ УСПЕШНО c прокси {_proxy_log_name(proxy)}, профиль {profile['name']}")
            return 'success', response.text, session

        if response.status_code == 403:
            logging.warning(f"🚫 eBay HTTP 403 для прокси {_proxy_log_name(proxy)}, профиль {profile['name']}")
            if own_session:
                close_session(session)
            return 'blocked', None, None if own_session else session

        if response.status_code == 429:
            logging.warning(f"⏳ eBay HTTP 429 для прокси {_proxy_log_name(proxy)}, профиль {profile['name']}")
            if own_session:
                close_session(session)
            return 'rate_limited', None, None if own_session else session

        if response.status_code == 407:
            logging.warning(f"🔐 Прокси требует авторизацию: {_proxy_log_name(proxy)}")
            if own_session:
                close_session(session)
            return 'proxy_error', None, None if own_session else session

        logging.warning(
            f"⚠️ НЕУДАЧА: HTTP {response.status_code} "
            f"для прокси {_proxy_log_name(proxy)}, профиль {profile['name']}"
        )
        if own_session:
            close_session(session)
        return 'http_error', None, None if own_session else session

    except Exception as e:
        error_msg = str(e)
        low = error_msg.lower()
        logging.error(
            f"❌ ОШИБКА для прокси {_proxy_log_name(proxy)}, "
            f"профиль {profile['name']}: {error_msg}"
        )

        if 'not supported' in low:
            disable_profile(profile['name'])
            if own_session:
                close_session(session)
            return 'profile_error', None, None

        if own_session:
            close_session(session)

        if 'curl: (28)' in low or 'timed out' in low:
            return 'proxy_timeout', None, None if own_session else session
        if 'curl: (60)' in low or 'certificate' in low or 'self signed' in low:
            return 'proxy_ssl', None, None if own_session else session
        if (
            'connect tunnel failed' in low or
            'proxy connect aborted' in low or
            'wrong_version_number' in low or
            'wrong version number' in low or
            re.search(r'connect[^\n]*(?:response )?(?:400|405|500|501|502|503)', low)
        ):
            return 'proxy_rejected', None, None if own_session else session
        return 'proxy_error', None, None if own_session else session


def _tcp_preflight_proxy(proxy, force=False):
    """
    Быстрая проверка только TCP-доступности proxy endpoint.

    Никаких запросов к eBay/Google/Cloudflare: открываем TCP к ip:port proxy и сразу закрываем.
    Это специально fail-fast фильтр, а не доказательство, что eBay пропустит данный IP.
    """
    if not PROXY_PREFLIGHT_ENABLED or not proxy:
        return True, None

    if not force:
        state = proxy_manager.preflight_state(proxy)
        if state == 'ok':
            return True, None
        if state == 'bad':
            return False, proxy_manager.preflight_failure_result(proxy)

    try:
        parts = urlsplit(proxy)
        host = parts.hostname
        port = parts.port
        scheme = (parts.scheme or 'http').lower()
        if not host:
            proxy_manager.mark_preflight_result(proxy, False, 'proxy_error')
            return False, 'proxy_error'
        if port is None:
            port = 1080 if scheme == 'socks5' else (443 if scheme == 'https' else 80)

        started = time.monotonic()
        connect_timeout = proxy_manager.preflight_connect_timeout(proxy)
        sock = socket.create_connection((host, int(port)), timeout=connect_timeout)
        try:
            latency_ms = (time.monotonic() - started) * 1000.0
            proxy_manager.mark_preflight_result(proxy, True, latency_ms=latency_ms)
            return True, None
        finally:
            try:
                sock.close()
            except Exception:
                pass
    except (socket.timeout, TimeoutError):
        proxy_manager.mark_preflight_result(proxy, False, 'proxy_timeout')
        return False, 'proxy_timeout'
    except Exception:
        proxy_manager.mark_preflight_result(proxy, False, 'proxy_error')
        return False, 'proxy_error'



def _quality_recv_exact(sock, size, deadline):
    data = bytearray()
    while len(data) < size:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise socket.timeout("quality preflight deadline")
        sock.settimeout(max(0.05, remaining))
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise ConnectionError("proxy closed connection during quality preflight")
        data.extend(chunk)
    return bytes(data)


def _quality_recv_headers(sock, deadline, max_bytes=8192):
    data = bytearray()
    while b"\r\n\r\n" not in data:
        if len(data) >= max_bytes:
            raise ConnectionError("proxy CONNECT response headers too large")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise socket.timeout("quality preflight deadline")
        sock.settimeout(max(0.05, remaining))
        chunk = sock.recv(min(2048, max_bytes - len(data)))
        if not chunk:
            raise ConnectionError("proxy closed CONNECT response")
        data.extend(chunk)
    return bytes(data)


def _quality_https_preflight_proxy(proxy):
    """
    Строгий ФОНОВЫЙ health-check без HTTP-запроса к eBay:
      proxy TCP -> HTTP CONNECT/SOCKS5 CONNECT -> TLS handshake к example.com -> close.

    Это значительно сильнее простого TCP-open и ловит типичные ошибки из реального лога:
    CONNECT 400/500/503, SOCKS connect failure, TLS MITM/self-signed и медленные tunnel timeout.
    Failure здесь остаётся SOFT cache: основной ProxyManager не получает hard cooldown.
    """
    if not PROXY_QUALITY_PREFLIGHT_ENABLED or not proxy:
        return False, 'disabled'

    state = proxy_manager.quality_state(proxy)
    if state == 'ok':
        return True, None
    if state == 'bad':
        return False, proxy_manager.quality_failure_result(proxy)

    tcp_ok, tcp_reason = _tcp_preflight_proxy(proxy, force=False)
    if not tcp_ok:
        proxy_manager.mark_quality_result(proxy, False, tcp_reason or 'proxy_error')
        return False, tcp_reason or 'proxy_error'

    parts = urlsplit(proxy)
    proxy_host = parts.hostname
    proxy_port = parts.port
    scheme = (parts.scheme or 'http').lower()
    if not proxy_host:
        proxy_manager.mark_quality_result(proxy, False, 'proxy_error')
        return False, 'proxy_error'
    if proxy_port is None:
        proxy_port = 1080 if scheme == 'socks5' else (443 if scheme == 'https' else 80)

    # В текущем ProxyScrape pool реально используются http+socks5. Для редкого "https proxy"
    # не делаем ложный negative: оставляем только TCP signal и не пишем quality_bad.
    if scheme not in ('http', 'socks5'):
        return False, 'unsupported_proxy_scheme'

    started = time.monotonic()
    deadline = started + PROXY_QUALITY_TIMEOUT
    sock = None
    tls_sock = None

    try:
        remaining = max(0.05, deadline - time.monotonic())
        sock = socket.create_connection((proxy_host, int(proxy_port)), timeout=remaining)
        sock.settimeout(max(0.05, deadline - time.monotonic()))

        if scheme == 'http':
            host_port = f"{PROXY_QUALITY_HOST}:{PROXY_QUALITY_PORT}"
            request = (
                f"CONNECT {host_port} HTTP/1.1\r\n"
                f"Host: {host_port}\r\n"
                "Proxy-Connection: keep-alive\r\n"
                "User-Agent: Mozilla/5.0\r\n"
                "\r\n"
            ).encode('ascii', 'strict')
            sock.sendall(request)
            headers = _quality_recv_headers(sock, deadline)
            first_line = headers.split(b"\r\n", 1)[0].decode('iso-8859-1', 'replace')
            m = re.match(r"HTTP/\d(?:\.\d)?\s+(\d{3})", first_line, re.I)
            status = int(m.group(1)) if m else 0
            if status != 200:
                proxy_manager.mark_quality_result(proxy, False, 'proxy_rejected')
                return False, 'proxy_rejected'

        else:  # socks5
            sock.sendall(b"\x05\x01\x00")
            hello = _quality_recv_exact(sock, 2, deadline)
            if hello[0] != 0x05 or hello[1] != 0x00:
                proxy_manager.mark_quality_result(proxy, False, 'proxy_rejected')
                return False, 'proxy_rejected'

            host_bytes = PROXY_QUALITY_HOST.encode('idna')
            if len(host_bytes) > 255:
                proxy_manager.mark_quality_result(proxy, False, 'proxy_error')
                return False, 'proxy_error'
            req = (
                b"\x05\x01\x00\x03"
                + bytes([len(host_bytes)])
                + host_bytes
                + int(PROXY_QUALITY_PORT).to_bytes(2, 'big')
            )
            sock.sendall(req)
            reply = _quality_recv_exact(sock, 4, deadline)
            if reply[0] != 0x05 or reply[1] != 0x00:
                proxy_manager.mark_quality_result(proxy, False, 'proxy_rejected')
                return False, 'proxy_rejected'

            atyp = reply[3]
            if atyp == 0x01:
                _quality_recv_exact(sock, 4 + 2, deadline)
            elif atyp == 0x03:
                ln = _quality_recv_exact(sock, 1, deadline)[0]
                _quality_recv_exact(sock, ln + 2, deadline)
            elif atyp == 0x04:
                _quality_recv_exact(sock, 16 + 2, deadline)
            else:
                proxy_manager.mark_quality_result(proxy, False, 'proxy_error')
                return False, 'proxy_error'

        # Полный TLS handshake нужен именно для отсеивания MITM/self-signed proxy.
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise socket.timeout("quality preflight deadline before TLS")
        context = ssl.create_default_context()
        tls_sock = context.wrap_socket(
            sock,
            server_hostname=PROXY_QUALITY_HOST,
            do_handshake_on_connect=False,
        )
        sock = None  # ownership moved into tls_sock
        tls_sock.settimeout(max(0.05, deadline - time.monotonic()))
        tls_sock.do_handshake()

        latency_ms = (time.monotonic() - started) * 1000.0
        proxy_manager.mark_quality_result(proxy, True, latency_ms=latency_ms)
        return True, None

    except (socket.timeout, TimeoutError):
        proxy_manager.mark_quality_result(proxy, False, 'proxy_timeout')
        return False, 'proxy_timeout'
    except ssl.SSLCertVerificationError:
        proxy_manager.mark_quality_result(proxy, False, 'proxy_ssl')
        return False, 'proxy_ssl'
    except ssl.SSLError:
        proxy_manager.mark_quality_result(proxy, False, 'proxy_ssl')
        return False, 'proxy_ssl'
    except Exception:
        proxy_manager.mark_quality_result(proxy, False, 'proxy_error')
        return False, 'proxy_error'
    finally:
        if tls_sock is not None:
            try:
                tls_sock.close()
            except Exception:
                pass
        elif sock is not None:
            try:
                sock.close()
            except Exception:
                pass


def _probe_proxy(proxy, profile, timeout):
    """Одна discovery-проверка: быстрый TCP preflight -> только затем eBay."""
    ok, preflight_result = _tcp_preflight_proxy(proxy, force=False)
    if not ok:
        return preflight_result or 'proxy_error', None, None
    return _make_request(
        proxy,
        profile,
        session=None,
        timeout=timeout,
    )


def _cleanup_late_probe_future(future, proxy):
    """Закрывает/учитывает probe, который уже был в полёте, когда другой proxy победил."""
    if future.cancelled():
        return
    try:
        result, _, session = future.result()
    except Exception as e:
        logging.debug(f"Фоновый probe {_proxy_log_name(proxy)} завершился исключением: {e}")
        return

    try:
        if result == 'success':
            proxy_manager.mark_success(proxy)
            logging.info(f"🟢 Запомнен запасной успешный proxy {_proxy_log_name(proxy)} (late probe)")
        elif result != 'profile_error':
            proxy_manager.mark_failure(proxy, result, reason=f'{result} (late probe)')
            _auction_proxy_soft_host_penalty(proxy, result)
    finally:
        close_session(session)


def _discovery_replacement_pause(results):
    """Короткий jitter между replacement probe в зависимости от причины отказа."""
    results = [r for r in (results or []) if r]
    if not results:
        return 0.0
    if any(r in ('blocked', 'rate_limited') for r in results):
        return random.uniform(PROBE_BLOCK_PAUSE_MIN, PROBE_BLOCK_PAUSE_MAX)
    transport = {'proxy_rejected', 'proxy_ssl', 'proxy_error', 'proxy_timeout'}
    if all(r in transport for r in results):
        return random.uniform(PROBE_FAST_PAUSE_MIN, PROBE_FAST_PAUSE_MAX)
    return random.uniform(PROBE_OTHER_PAUSE_MIN, PROBE_OTHER_PAUSE_MAX)


def fetch_ebay_html_with_fixed_pair():
    global fixed_proxy, fixed_profile, fixed_session

    # Свежий background-резерв используется ПЕРВЫМ после падения fixed proxy.
    # На холодном старте список пуст: обычный discovery работает как раньше.
    fast_standby_queue = []

    # 1) Максимально долго держим реально рабочую session.
    if fixed_proxy is not None and fixed_profile is not None:
        logging.info(
            f"🔁 Используем зафиксированную пару: "
            f"proxy {_proxy_log_name(fixed_proxy)}, профиль {fixed_profile['name']}"
        )

        old_proxy = fixed_proxy
        old_profile = fixed_profile
        fixed_attempt_started = time.monotonic()
        with main_fixed_request_lock:
            result, html, returned_session = _make_request(
                old_proxy,
                old_profile,
                session=fixed_session,
                timeout=(FIXED_CONNECT_TIMEOUT, FIXED_READ_TIMEOUT),
            )
        fixed_attempt_elapsed = time.monotonic() - fixed_attempt_started

        if result == 'success':
            fixed_session = returned_session
            proxy_manager.mark_success(old_proxy)
            record_ebay_success()
            return html

        # Частая реальная ситуация: умерло только старое TCP/TLS соединение Session,
        # а сам proxy всё ещё жив. Быстрый reset получает ОДИН шанс с новой Session.
        # Но если первая попытка уже висела >= FIXED_RECOVERY_SKIP_AFTER, второй 12-секундный
        # recovery только откладывает failover. В новом логе это стоило ~36 сек. до discovery.
        can_recover_fixed = (
            result in ('proxy_timeout', 'proxy_error')
            and proxy_manager.is_recent_good(old_proxy)
        )
        if can_recover_fixed and fixed_attempt_elapsed >= FIXED_RECOVERY_SKIP_AFTER:
            logging.info(
                f"⚡ Fixed proxy уже ждал {fixed_attempt_elapsed:.1f} сек.; "
                f"recovery пропускаем и сразу переключаемся на reserve/discovery"
            )
        elif can_recover_fixed:
            logging.info(
                f"♻️ Недавно успешный proxy {_proxy_log_name(old_proxy)}: "
                "пересоздаём session и даём один быстрый шанс"
            )
            close_session(fixed_session)
            fixed_session = None
            time.sleep(random.uniform(0.4, 0.8))

            with main_fixed_request_lock:
                retry_result, retry_html, retry_session = _make_request(
                    old_proxy,
                    old_profile,
                    session=None,
                    timeout=(FIXED_RECOVERY_CONNECT_TIMEOUT, FIXED_RECOVERY_READ_TIMEOUT),
                )
            provider_manager.record_result(
                old_proxy, retry_result,
                len(retry_html.encode('utf-8', errors='ignore')) if retry_html else 0
            )
            provider_manager.maybe_warn_webshare_usage()
            if retry_result == 'success':
                fixed_proxy = old_proxy
                fixed_profile = old_profile
                fixed_session = retry_session
                proxy_manager.mark_success(old_proxy)
                record_ebay_success()
                logging.info("✅ Proxy восстановился после пересоздания session")
                return retry_html

            close_session(retry_session)
            result = retry_result

        close_session(fixed_session)
        fixed_session = None
        fixed_proxy = None
        fixed_profile = None

        if result == 'profile_error':
            logging.warning(
                f"Профиль {old_profile['name']} недоступен; proxy {_proxy_log_name(old_proxy)} не штрафуем"
            )
        else:
            proxy_manager.mark_failure(
                old_proxy,
                result,
                reason=f'{result} на ранее успешной fixed session',
            )
            _auction_proxy_soft_host_penalty(old_proxy, result)

        # Не ждём нового ProxyScrape snapshot, если background worker уже подготовил резерв.
        # Берём только свежие known-good / HTTPS-TLS-ready endpoint-ы (+ максимум 1 TCP-only).
        fast_standby_queue = proxy_manager.get_warm_standby_candidates(
            max(PROXY_PREFLIGHT_RESERVE_TARGET, PROBE_DEEP_CONCURRENCY),
            excluded_hosts={_proxy_host(old_proxy)},
        )
        if fast_standby_queue:
            quality_ready = sum(
                1 for p in fast_standby_queue
                if proxy_manager.quality_state(p) == 'ok'
            )
            logging.info(
                f"⚡ Готов warm reserve: {len(fast_standby_queue)} proxy "
                f"(HTTPS/TLS-ready={quality_ready}); пробуем его раньше fresh discovery"
            )

        logging.info("Ищем новую рабочую пару...")

    # 2) Rolling discovery V6.19: warm reserve -> обычные 4 worker -> 5 после 10 сек.;
    # только при затянувшемся outage с уже включённым emergency pool -> максимум 6.
    # Сначала расширяемся до ProxyScrape 3000 ms, а 4000 ms используем только как
    # последний deep-emergency tier. Hard cooldown никогда не снимаются.
    # V6.20: сначала обновляем управляемые источники. Ошибка их API полностью fail-open:
    # старый бесплатный ProxyScrape остаётся независимым fallback.
    provider_manager.refresh_all(force=False)

    # На свежем старте сначала ОБЯЗАТЕЛЬНО загружаем обычный Render PROXY_LIST
    # (timeout=1500). Emergency 3000 не имеет права включаться на пустом 0/0 pool.
    if not proxy_manager.standard_pool_loaded():
        proxy_manager.refresh_proxies(force=True, emergency=False)

    started = time.monotonic()
    tried_hosts = set()
    tried_proxies = set()
    reprobed_proxies = set()
    transient_reprobed_proxies = set()
    attempts = 0
    refreshed_after_exhaustion = False
    emergency_loaded = False
    deep_emergency_loaded = False
    final_reprobe_used = False
    standard_mid_refresh_used = False
    profile = get_preferred_profile()
    if profile is None:
        logging.error("Нет поддерживаемого browser-профиля curl_cffi")
        return None

    executor = ThreadPoolExecutor(
        max_workers=PROBE_DEEP_CONCURRENCY,
        thread_name_prefix='proxy-probe',
    )
    future_to_proxy = {}

    def desired_concurrency():
        elapsed_now = time.monotonic() - started
        if emergency_loaded and elapsed_now >= PROBE_DEEP_ESCALATE_AFTER:
            return PROBE_DEEP_CONCURRENCY
        if elapsed_now >= PROBE_ESCALATE_AFTER:
            return PROBE_ESCALATED_CONCURRENCY
        return PROBE_CONCURRENCY

    def maybe_mid_refresh_standard():
        nonlocal standard_mid_refresh_used
        if standard_mid_refresh_used:
            return False
        elapsed_now = time.monotonic() - started
        if (
            elapsed_now < PROXY_STANDARD_MID_REFRESH_AFTER
            or attempts < PROXY_STANDARD_MID_REFRESH_ATTEMPTS
        ):
            return False
        standard_mid_refresh_used = True
        logging.info(
            f"🔄 Длинный discovery: мягко обновляем standard 1500-ms pool "
            f"после {elapsed_now:.0f} сек./{attempts} probe"
        )
        return proxy_manager.refresh_standard_merge(force=True)

    def maybe_load_emergency(force=False):
        nonlocal emergency_loaded
        if emergency_loaded:
            return False
        elapsed_now = time.monotonic() - started
        long_bad_search = (
            elapsed_now >= PROXY_EMERGENCY_TRIGGER_AFTER
            and attempts >= PROXY_EMERGENCY_TRIGGER_ATTEMPTS
        )
        available, total, bad_count, host_bad_count = proxy_manager.pool_stats()
        depleted = total > 0 and proxy_manager.emergency_needed()
        if not (force or long_bad_search or depleted):
            return False

        reason = (
            'исчерпан доступный пул'
            if depleted
            else f'поиск уже {elapsed_now:.0f} сек./{attempts} probe без успеха'
        )
        logging.warning(
            f"🆘 Включаем emergency ProxyScrape timeout={PROXY_EMERGENCY_TIMEOUT_MS} ms: {reason}; "
            f"до расширения доступно {available}/{total}, cooldown={bad_count}, host_cooldown={host_bad_count}"
        )
        # В обычном trigger/depleted режиме используем недавний уже слитый 3000-ms
        # snapshot повторно; force=True применяется только когда кандидаты реально
        # исчерпаны внутри текущего discovery.
        proxy_manager.refresh_proxies(force=force, emergency=True)
        emergency_loaded = True
        return True

    def maybe_load_deep_emergency(force=False):
        nonlocal deep_emergency_loaded
        if deep_emergency_loaded:
            return False
        elapsed_now = time.monotonic() - started
        ready = (
            emergency_loaded
            and elapsed_now >= PROXY_DEEP_EMERGENCY_TRIGGER_AFTER
            and attempts >= PROXY_DEEP_EMERGENCY_TRIGGER_ATTEMPTS
        )
        if not (force or ready):
            return False
        logging.warning(
            f"🆘🆘 Включаем deep-emergency ProxyScrape timeout={PROXY_DEEP_EMERGENCY_TIMEOUT_MS} ms: "
            f"поиск уже {elapsed_now:.0f} сек./{attempts} probe без успеха"
        )
        proxy_manager.refresh_deep_emergency(force=False)
        deep_emergency_loaded = True
        return True

    def submit_more():
        """Поддерживает rolling in-flight probe и один безопасный последний re-probe."""
        nonlocal attempts, refreshed_after_exhaustion, final_reprobe_used

        elapsed_now = time.monotonic() - started
        if elapsed_now >= SEARCH_TIME_BUDGET:
            return False

        desired = desired_concurrency()
        free_slots = max(0, desired - len(future_to_proxy))
        if free_slots <= 0:
            return True

        # В длинном цикле сначала подмешиваем свежий быстрый 1500-ms snapshot: новый
        # рабочий endpoint может появиться уже после старта discovery. Emergency pool
        # при этом не теряется. Затем при необходимости расширяемся до 3000 ms.
        maybe_mid_refresh_standard()
        maybe_load_emergency(force=False)
        maybe_load_deep_emergency(force=False)

        normal_remaining = max(0, MAX_SEARCH_ATTEMPTS - attempts)
        need = min(free_slots, normal_remaining)

        inflight_socks = sum(
            1 for p in future_to_proxy.values()
            if _proxy_scheme(p) == 'socks5'
        )
        preferred_scheme = 'socks5' if desired >= 3 and inflight_socks == 0 else None
        inflight_hosts = {_proxy_host(p) for p in future_to_proxy.values()}
        batch = []
        batch_kinds = {}

        if need > 0:
            # 0) Самый быстрый путь после падения fixed-session: background уже доказал,
            # что эти proxy проводят HTTPS/TLS. Не делаем перед ними новый ProxyScrape refresh.
            while fast_standby_queue and len(batch) < need:
                standby = fast_standby_queue.pop(0)
                standby_host = _proxy_host(standby)
                if (
                    not standby_host
                    or standby_host in tried_hosts
                    or standby_host in inflight_hosts
                    or standby_host in {_proxy_host(p) for p in batch}
                ):
                    continue
                # TTL/cooldown могли закончиться между падением fixed proxy и этим слотом.
                quality_state = proxy_manager.quality_state(standby)
                preflight_state = proxy_manager.preflight_state(standby)
                if quality_state != 'ok' and preflight_state != 'ok' and not proxy_manager.is_recent_good(standby):
                    continue
                batch.append(standby)
                batch_kinds[standby] = (
                    'HTTPS reserve' if quality_state == 'ok' else 'Warm standby'
                )

            # Один re-probe recently-good внутри текущего discovery после окончания cooldown.
            reprobe = proxy_manager.get_due_reprobe_candidate(
                tried_proxies=tried_proxies,
                reprobed_proxies=reprobed_proxies,
                inflight_hosts=inflight_hosts,
            )
            if reprobe is not None and len(batch) < need:
                reprobed_proxies.add(reprobe)
                batch.append(reprobe)
                batch_kinds[reprobe] = 'Re-probe'
                logging.info(f"♻️ Re-probe недавно успешного proxy после cooldown: {_proxy_log_name(reprobe)}")

            # В emergency-mode допускаем максимум 2 вторых шанса только transport-timeout/error.
            while (
                emergency_loaded
                and len(batch) < need
                and len(transient_reprobed_proxies) < PROXY_EMERGENCY_TRANSIENT_REPROBES
            ):
                transient = proxy_manager.get_due_transient_reprobe_candidate(
                    tried_proxies=tried_proxies,
                    reprobed_proxies=transient_reprobed_proxies | reprobed_proxies,
                    inflight_hosts=inflight_hosts | {_proxy_host(p) for p in batch},
                )
                if transient is None:
                    break
                transient_reprobed_proxies.add(transient)
                batch.append(transient)
                batch_kinds[transient] = 'Transient re-probe'
                logging.info(f"♻️ Emergency transient re-probe после cooldown: {_proxy_log_name(transient)}")

            remaining_need = need - len(batch)
            if remaining_need > 0:
                elapsed_for_provider = time.monotonic() - started
                allow_webshare = (
                    elapsed_for_provider >= WEBSHARE_UNLOCK_AFTER
                    or attempts >= WEBSHARE_UNLOCK_ATTEMPTS
                )
                normal_batch = proxy_manager.get_candidate_batch(
                    remaining_need,
                    tried_hosts=tried_hosts | {_proxy_host(p) for p in batch},
                    preferred_scheme=preferred_scheme,
                    allow_webshare=allow_webshare,
                )
                for p in normal_batch:
                    batch.append(p)
                    batch_kinds.setdefault(p, 'Probe')

        # Если уникальные кандидаты кончились раньше лимита, emergency pool важнее
        # бессмысленного повторного стандартного refresh того же 1500-ms списка.
        if not batch and not future_to_proxy and attempts < MAX_SEARCH_ATTEMPTS:
            if not emergency_loaded:
                maybe_load_emergency(force=True)
                batch = proxy_manager.get_candidate_batch(
                    min(free_slots, MAX_SEARCH_ATTEMPTS - attempts),
                    tried_hosts=tried_hosts,
                    preferred_scheme=preferred_scheme,
                    allow_webshare=True,
                )
                for p in batch:
                    batch_kinds[p] = 'Emergency probe'
            elif not refreshed_after_exhaustion:
                logging.info("♻️ Emergency-кандидаты исчерпаны; один раз обновляем расширенный список")
                proxy_manager.refresh_proxies(force=True, emergency=True)
                refreshed_after_exhaustion = True
                batch = proxy_manager.get_candidate_batch(
                    min(free_slots, MAX_SEARCH_ATTEMPTS - attempts),
                    tried_hosts=tried_hosts,
                    preferred_scheme=preferred_scheme,
                    allow_webshare=True,
                )
                for p in batch:
                    batch_kinds[p] = 'Emergency probe'

        # После обычного лимита probe гарантируем максимум один последний шанс known-good,
        # если его cooldown уже закончился. Он не вытесняет обычного кандидата и даёт
        # максимум один дополнительный сетевой запрос сверх обычного лимита.
        if (
            not batch
            and not future_to_proxy
            and attempts >= MAX_SEARCH_ATTEMPTS
            and FINAL_KNOWN_GOOD_REPROBES > 0
            and not final_reprobe_used
        ):
            final_proxy = proxy_manager.get_final_known_good_reprobe(
                tried_proxies=tried_proxies,
                inflight_hosts=inflight_hosts,
            )
            final_reprobe_used = True
            if final_proxy is not None:
                batch = [final_proxy]
                batch_kinds[final_proxy] = 'Final known-good re-probe'
                logging.info(f"♻️ Финальный known-good re-probe поверх обычного лимита: {_proxy_log_name(final_proxy)}")

        if not batch:
            return False

        for proxy in batch:
            is_final_extra = batch_kinds.get(proxy) == 'Final known-good re-probe'
            if not is_final_extra and attempts >= MAX_SEARCH_ATTEMPTS:
                break
            tried_hosts.add(_proxy_host(proxy))
            tried_proxies.add(proxy)
            proxy_manager.remember_outage_attempt(proxy)
            attempts += 1
            kind = batch_kinds.get(proxy, 'Probe')
            source = provider_manager.source_fast(proxy)
            if source != 'free' and kind == 'Probe':
                kind = 'Premium probe' if source == 'proxyscrape_premium' else 'Webshare rescue'
            limit_label = f"{MAX_SEARCH_ATTEMPTS}+1" if is_final_extra else str(MAX_SEARCH_ATTEMPTS)
            logging.info(
                f"🔍 {kind} {attempts}/{limit_label}: proxy {_proxy_log_name(proxy)}, профиль {profile['name']}"
            )
            future = executor.submit(
                _probe_proxy,
                proxy,
                profile,
                (PROBE_CONNECT_TIMEOUT, PROBE_READ_TIMEOUT),
            )
            future_to_proxy[future] = proxy
        return bool(batch)

    submit_more()

    try:
        while True:
            elapsed = time.monotonic() - started
            if elapsed >= SEARCH_TIME_BUDGET:
                logging.warning(
                    f"⏱ Достигнут лимит discovery {SEARCH_TIME_BUDGET} сек.; "
                    "завершаем текущий активный поиск"
                )
                break

            maybe_load_emergency(force=False)
            maybe_load_deep_emergency(force=False)
            submit_more()

            if not future_to_proxy:
                # Нет in-flight. Даём шанс final known-good re-probe; если и его нет — выход.
                if submit_more():
                    continue
                break

            remaining_budget = max(0.05, SEARCH_TIME_BUDGET - (time.monotonic() - started))
            done, _ = wait(
                tuple(future_to_proxy.keys()),
                timeout=min(1.0, remaining_budget),
                return_when=FIRST_COMPLETED,
            )
            if not done:
                continue

            winner = None
            completed_results = []
            for future in done:
                proxy = future_to_proxy.pop(future, None)
                if proxy is None:
                    continue
                try:
                    result, html, session = future.result()
                except Exception as e:
                    logging.error(f"Ошибка probe worker для {_proxy_log_name(proxy)}: {e}")
                    result, html, session = 'proxy_error', None, None

                completed_results.append(result)
                if result == 'success':
                    proxy_manager.mark_success(proxy)
                    if winner is None:
                        winner = (proxy, html, session)
                    else:
                        # Несколько probes могут завершиться одним wait() одновременно.
                        # В V6.6 второй success ошибочно попадал в mark_failure(..., 'success').
                        # Теперь это корректно запомненный запасной рабочий proxy.
                        logging.info(f"🟢 Запомнен запасной успешный proxy {_proxy_log_name(proxy)} (same batch)")
                        close_session(session)
                elif result == 'profile_error':
                    close_session(session)
                    fallback_profile = get_preferred_profile()
                    if fallback_profile is not None:
                        profile = fallback_profile
                else:
                    close_session(session)
                    proxy_manager.mark_failure(proxy, result, reason=result)
                    _auction_proxy_soft_host_penalty(proxy, result)

            if winner is not None:
                for future, proxy in list(future_to_proxy.items()):
                    if future.done():
                        _cleanup_late_probe_future(future, proxy)
                    elif not future.cancel():
                        future.add_done_callback(
                            lambda f, p=proxy: _cleanup_late_probe_future(f, p)
                        )
                future_to_proxy.clear()
                executor.shutdown(wait=False, cancel_futures=True)

                winner_proxy, winner_html, winner_session = winner
                fixed_proxy = winner_proxy
                fixed_profile = profile
                fixed_session = winner_session
                record_ebay_success()
                proxy_preflight_wakeup_event.set()
                logging.info(
                    f"✅ Найдена рабочая пара: proxy {_proxy_log_name(winner_proxy)}, "
                    f"source={provider_manager.source_fast(winner_proxy)}, профиль {profile['name']}; "
                    f"проверено {attempts} proxy за {time.monotonic() - started:.1f} сек."
                )
                return winner_html

            if completed_results:
                time.sleep(_discovery_replacement_pause(completed_results))
            submit_more()

    finally:
        for future, proxy in list(future_to_proxy.items()):
            if future.done():
                _cleanup_late_probe_future(future, proxy)
            elif not future.cancel():
                future.add_done_callback(
                    lambda f, p=proxy: _cleanup_late_probe_future(f, p)
                )
        executor.shutdown(wait=False, cancel_futures=True)

    logging.error(
        f"❌ В этом цикле рабочий proxy не найден: "
        f"проверено {attempts}, время {time.monotonic() - started:.1f} сек."
    )
    # Перед следующим коротким циклом мягко мерджим свежий 1500-ms snapshot, НЕ
    # выбрасывая уже добавленные 3000/4000-ms candidates. Это особенно важно при
    # истощённом пуле: раньше каждый короткий цикл заново скачивал тот же emergency
    # список и терял время, хотя новых endpoint не появлялось.
    proxy_manager.refresh_standard_merge(force=True)
    if proxy_manager.emergency_needed():
        proxy_manager.refresh_proxies(force=False, emergency=True)
        proxy_manager.refresh_deep_emergency(force=False)
    return None

def fetch_ebay_html_with_retry():
    return fetch_ebay_html_with_fixed_pair()

# ============ ПАРСИНГ ============
def extract_item_id(url):
    """Извлекает публичный numeric eBay item id из старого и нового URL.

    Поддерживает оба варианта:
      /itm/123456789012
      /itm/some-title/123456789012
    Не принимает текстовый slug за item_id.
    """
    if not url or '/itm/' not in url:
        return None
    m = re.search(r'/itm/(?:[^/?#]+/)?(\d{8,19})(?:[/?#]|$)', str(url), re.I)
    return m.group(1) if m else None

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

def extract_range_price(card):
    price_spans = card.select('span.s-card__price, span.s-item__price, [class*="price"]')
    for i in range(len(price_spans) - 2):
        first = price_spans[i].get_text(strip=True)
        middle = price_spans[i+1].get_text(strip=True).lower()
        third = price_spans[i+2].get_text(strip=True)
        if 'to' in middle and re.search(r'[£€$]', first) and re.search(r'[£€$]', third):
            return f"{first} до {third}"
    to_elem = card.find(string=re.compile(r'\bto\b', re.I))
    if to_elem:
        parent = to_elem.find_parent()
        if parent:
            prev_price = None
            next_price = None
            for sibling in parent.previous_siblings:
                if hasattr(sibling, 'get_text'):
                    txt = sibling.get_text(strip=True)
                    if re.search(r'[£€$]\s*[\d,]+\.?\d*', txt):
                        prev_price = txt
                        break
            for sibling in parent.next_siblings:
                if hasattr(sibling, 'get_text'):
                    txt = sibling.get_text(strip=True)
                    if re.search(r'[£€$]\s*[\d,]+\.?\d*', txt):
                        next_price = txt
                        break
            if prev_price and next_price:
                return f"{prev_price} до {next_price}"
    return None

def extract_price_jsonld(card, url=None, soup=None):
    range_price = extract_range_price(card)
    if range_price:
        return range_price

    script = card.find('script', type='application/ld+json')
    candidates = []
    if script and script.string:
        try:
            data = json.loads(script.string)
            if isinstance(data, dict):
                offers = data.get('offers')
                if isinstance(offers, dict):
                    price = offers.get('price')
                    currency = offers.get('priceCurrency', '')
                    if price and price != '0':
                        candidates.append((price, currency))
                elif isinstance(offers, list):
                    for off in offers:
                        price = off.get('price')
                        currency = off.get('priceCurrency', '')
                        if price and price != '0':
                            candidates.append((price, currency))
        except:
            pass
    if soup and url:
        for script in soup.find_all('script', type='application/ld+json'):
            if not script.string:
                continue
            try:
                data = json.loads(script.string)
                if isinstance(data, dict) and data.get('url') == url:
                    offers = data.get('offers')
                    if isinstance(offers, dict):
                        price = offers.get('price')
                        currency = offers.get('priceCurrency', '')
                        if price and price != '0':
                            candidates.append((price, currency))
            except:
                continue
    for price, curr in candidates:
        if curr == 'GBP' or (curr == '' and str(price).startswith('£')):
            return f"£{price}"
    for price, curr in candidates:
        if curr:
            return f"{curr} {price}"
        else:
            return str(price)
    return None

def extract_price_css(card):
    range_price = extract_range_price(card)
    if range_price:
        return range_price

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

def extract_shipping(card, item_price=None, range_prices=None):
    for elem in card.select('.su-styled-text.secondary.large'):
        text = elem.get_text(strip=True)
        text_lower = text.lower()
        if 'delivery' in text_lower or 'shipping' in text_lower:
            if 'free' in text_lower:
                return "Бесплатно"
            match = re.search(r'([+]\s*)?([£€$]\s*[\d,]+\.?\d*)', text)
            if match:
                price_candidate = match.group(2)
                if range_prices and any(price_candidate in p or p in price_candidate for p in range_prices):
                    pass
                elif item_price and price_candidate == item_price:
                    pass
                else:
                    return price_candidate
            if len(text) > 3:
                return text

    html_lower = str(card).lower()
    if re.search(r'free\s+delivery', html_lower) or re.search(r'free\s+shipping', html_lower):
        return "Бесплатно"

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
                            if range_prices and any(amount in p or p in amount for p in range_prices):
                                pass
                            elif item_price and str(shipping) == str(item_price) and currency == 'GBP':
                                pass
                            else:
                                return amount
                elif isinstance(offers, list) and len(offers) > 0:
                    first = offers[0]
                    shipping = first.get('shippingCost')
                    if shipping is not None:
                        if shipping == 0 or str(shipping) == '0':
                            return "Бесплатно"
                        if isinstance(shipping, (int, float)):
                            currency = first.get('priceCurrency', '')
                            amount = f"{currency} {shipping}" if currency else str(shipping)
                            if range_prices and any(amount in p or p in amount for p in range_prices):
                                pass
                            elif item_price and str(shipping) == str(item_price) and currency == 'GBP':
                                pass
                            else:
                                return amount
        except:
            pass

    shipping_selectors = [
        'span.s-item__shipping', 'div.s-item__shipping',
        'span.s-item__logisticsCost', 'span.s-item__delivery',
        'span.su-styled-text', '.su-styled-text.secondary.large',
        '[class*="shippingCost"]', '[class*="delivery"]',
        '.s-item__detail--shipping', '.s-item__delivery-costs'
    ]
    for sel in shipping_selectors:
        for elem in card.select(sel):
            text = elem.get_text(strip=True)
            text = re.sub(r'\s+', ' ', text)
            if not text:
                continue
            if re.search(r'(?i)(buy it now|best offer|make offer|watch|add to cart)', text):
                continue
            if re.search(r'\bfree\b', text.lower()):
                return "Бесплатно"
            match = re.search(r'([+]\s*)?([£€$]\s*[\d,]+\.?\d*)', text)
            if match:
                price_candidate = match.group(2)
                if range_prices and any(price_candidate in p or p in price_candidate for p in range_prices):
                    continue
                if item_price and price_candidate == item_price:
                    continue
                return price_candidate
            if re.search(r'(?i)(delivery|shipping)', text) and len(text) > 5:
                if len(text) > 10 or re.search(r'\d', text):
                    return text

    match = re.search(r'\+?\s*([£€$]\s*[\d,]+\.?\d*)\s*(delivery|shipping)', html_lower)
    if match:
        pc = match.group(1)
        if range_prices and any(pc in p or p in pc for p in range_prices):
            pass
        else:
            if not (item_price and pc == item_price):
                return pc
    match = re.search(r'shipping:\s*([£€$]\s*[\d,]+\.?\d*)', html_lower)
    if match:
        pc = match.group(1)
        if range_prices and any(pc in p or p in pc for p in range_prices):
            pass
        else:
            if not (item_price and pc == item_price):
                return pc
    match = re.search(r'(delivery in\s+\d+[-\s]*\d*\s*(days?|weeks?|business days?|working days?))', html_lower)
    if match:
        return match.group(1).strip()
    match = re.search(r'(delivery time\s*:\s*[\w\s\d-]+)', html_lower)
    if match:
        return match.group(1).strip()
    match = re.search(r'(shipping in\s+\d+[-\s]*\d*\s*(days?|weeks?))', html_lower)
    if match:
        return match.group(1).strip()
    return None

def extract_best_offer(card):
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
    if card.select_one('[data-best-offer="true"]'):
        return True
    return False

def extract_auction(card):
    if card.select_one('.su-styled-text.secondary.large'):
        elem = card.select_one('.su-styled-text.secondary.large')
        text = elem.get_text(strip=True).lower()
        if 'bid' in text:
            return True
    auction_selectors = [
        '.s-item__bid-count', '.s-item__bids', '[class*="bidCount"]',
        '[class*="bids"]', '[class*="bid-count"]', '.vi-bidrev',
        '.s-item__detail--bid-count', '[data-testid="bid-count"]', '.bidCount',
        'span.bids', '.s-item__auction', '.auction-badge', '.s-item__bid-count__text',
        '.bid-count', '.bids-count'
    ]
    for sel in auction_selectors:
        found = card.select_one(sel)
        if found:
            txt = found.get_text(strip=True).lower()
            if 'bid' in txt or txt.isdigit():
                return True
    full_text = card.get_text().lower()
    if re.search(r'\d+\s+bids?\b', full_text):
        return True
    if re.search(r'\bplace\s+bid\b', full_text):
        return True
    if 'bids' in full_text and 'buy it now' not in full_text:
        return True
    if card.select_one('a[href*="bid"]'):
        return True
    if card.select_one('[data-auction="true"]'):
        return True
    if card.select_one('[data-testid*="auction"]'):
        return True
    return False

def extract_buy_it_now_info(card):
    buy_it_now_elem = card.find(string=re.compile(r'Buy It Now', re.I))
    if not buy_it_now_elem:
        return False, None
    price_spans = card.select('span.s-card__price, span.s-item__price, [class*="price"]')
    if len(price_spans) >= 2:
        second_price = price_spans[1].get_text(strip=True)
        if re.search(r'[£€$]', second_price):
            return True, second_price
    parent = buy_it_now_elem.find_parent()
    if parent:
        price_elem = parent.find_next('span', class_=re.compile(r'price'))
        if price_elem:
            price_text = price_elem.get_text(strip=True)
            if re.search(r'[£€$]', price_text):
                return True, price_text
    return True, None

def _main_search_result_cards(soup):
    """Возвращает только карточки ОСНОВНОЙ поисковой выдачи eBay.

    eBay сейчас A/B-тестирует несколько DOM-разметок. На разных proxy один и тот же
    поиск может прийти как старый ``li.s-item`` либо как новый ``li.s-card`` /
    ``div.su-card-container``. Поэтому нельзя считать отсутствие ``s-item`` ошибкой.

    При этом мы по-прежнему НЕ сканируем всю страницу по всем ``/itm/`` ссылкам:
    поиск ограничен основным SRP-контейнером, чтобы не захватывать рекомендации,
    карусели и посторонние блоки. MAX_ITEMS остаётся только верхним пределом.
    """
    root = (
        soup.select_one('#srp-river-results')
        or soup.select_one('.srp-river-results')
        or soup.select_one('ul.srp-results')
    )
    if root is None:
        logging.warning(
            "⚠️ Основной контейнер поисковой выдачи eBay не найден; "
            "глобальный fallback по всем /itm/ ссылкам отключён"
        )
        return None

    cards = []
    seen_nodes = set()
    rewrite_boundary_seen = False
    layout_counts = {'s-item': 0, 's-card': 0, 'su-card-container': 0, 'data-viewport': 0}

    def add_card(node, layout):
        key = id(node)
        if key in seen_nodes:
            return
        # Карточка должна содержать реальную ссылку на item. Это отсекает служебные
        # placeholders, заголовки и пустые контейнеры с похожими CSS-классами.
        if not node.select_one('a[href*="/itm/"]'):
            return
        seen_nodes.add(key)
        cards.append(node)
        layout_counts[layout] = layout_counts.get(layout, 0) + 1

    # Идём в DOM-порядке, чтобы остановиться ДО блока расширенных результатов.
    for node in root.descendants:
        if not isinstance(node, Tag):
            continue
        classes = set(node.get('class') or [])
        if 'srp-river-answer--REWRITE_START' in classes:
            rewrite_boundary_seen = True
            break

        if 's-item' in classes:
            add_card(node, 's-item')
        elif 's-card' in classes:
            add_card(node, 's-card')
        elif 'su-card-container' in classes:
            add_card(node, 'su-card-container')
        elif node.name == 'li' and node.has_attr('data-viewport'):
            # Дополнительный безопасный layout-fallback, который встречается в A/B DOM.
            add_card(node, 'data-viewport')

    # Если eBay снова поменял имена card-классов, используем ТОЛЬКО ссылки внутри
    # основного root и поднимаемся к ближайшему небольшому контейнеру с ценой/заголовком.
    # Это намного безопаснее старого fallback по всем /itm/ ссылкам всей страницы.
    if not cards:
        fallback_seen_ids = set()
        for node in root.descendants:
            if not isinstance(node, Tag):
                continue
            classes = set(node.get('class') or [])
            if 'srp-river-answer--REWRITE_START' in classes:
                rewrite_boundary_seen = True
                break
            if node.name != 'a':
                continue
            href = node.get('href') or ''
            item_id = extract_item_id(href)
            if not item_id or item_id in fallback_seen_ids:
                continue

            parent = node
            chosen = None
            for _ in range(8):
                parent = parent.parent
                if parent is None or parent is root:
                    break
                if not isinstance(parent, Tag):
                    continue
                text = parent.get_text(' ', strip=True)
                has_price = bool(
                    parent.select_one('.s-card__price, .s-item__price, [class*="price"]')
                    or re.search(r'£\s*[\d,.]+', text)
                )
                has_title = bool(
                    parent.select_one('.s-card__title, .s-item__title, [role="heading"]')
                    or len(node.get_text(' ', strip=True)) >= 4
                )
                if has_price and has_title:
                    chosen = parent
                    break
            if chosen is not None:
                cards.append(chosen)
                fallback_seen_ids.add(item_id)

        if cards:
            logging.warning(
                f"⚠️ eBay использует неизвестный card-layout; безопасный fallback "
                f"внутри основного контейнера распознал {len(cards)} карточек"
            )

    if not cards:
        logging.warning(
            "⚠️ Основной контейнер eBay найден, но карточки выдачи не распознаны "
            "ни как s-item, ни как s-card/su-card-container; БД не изменяем"
        )
        return None

    if rewrite_boundary_seen:
        logging.info(
            f"🛡 Обнаружена граница расширенных результатов eBay; "
            f"учитываем только {len(cards)} карточек до неё"
        )

    active_layouts = ', '.join(f'{k}={v}' for k, v in layout_counts.items() if v)
    if active_layouts:
        logging.info(f"🧩 Разметка eBay: {active_layouts}; карточек-кандидатов {len(cards)}")

    return cards


def _listing_link(card):
    """Ссылка item для старой и новой SRP-разметки eBay."""
    selectors = (
        'a.s-card__link[href*="/itm/"]',
        'a.s-item__link[href*="/itm/"]',
        'a.su-link[href*="/itm/"]',
        'a[href*="/itm/"]',
    )
    for selector in selectors:
        link = card.select_one(selector)
        if link and extract_item_id(link.get('href') or ''):
            return link
    return None


def parse_ebay_listings(html, max_items=MAX_ITEMS):
    if not html:
        return None
    soup = BeautifulSoup(html, 'html.parser')
    cards = _main_search_result_cards(soup)
    if cards is None:
        return None

    items = {}
    for card in cards:
        if len(items) >= max_items:
            break

        link = _listing_link(card)
        if not link:
            continue
        url = link.get('href')
        if not url:
            continue
        if url.startswith('/'):
            url = 'https://www.ebay.co.uk' + url
        item_id = extract_item_id(url)
        if not item_id or item_id in items:
            continue

        title_elem = (
            card.select_one('div.s-card__title')
            or card.select_one('.s-card__title')
            or card.select_one('div.s-item__title span[role="heading"]')
            or card.select_one('div.s-item__title')
            or card.select_one('[role="heading"]')
            or link
        )
        title = clean_title(title_elem.get_text(' ', strip=True) if title_elem else '')
        if not title:
            title = clean_title(link.get_text(' ', strip=True))
        if not title:
            continue
        if title.strip().lower() in {'shop on ebay', 'opens in a new window or tab'}:
            continue

        price = extract_price_jsonld(card, url, soup) or extract_price_css(card)
        range_prices = []
        if price and ' до ' in price:
            parts = price.split(' до ')
            if len(parts) == 2:
                range_prices = [parts[0].strip(), parts[1].strip()]
        if price and not is_gbp_price(price):
            price = None

        shipping = extract_shipping(card, item_price=price, range_prices=range_prices)
        best_offer = extract_best_offer(card)
        auction = extract_auction(card)
        has_bin, bin_price = extract_buy_it_now_info(card)

        items[item_id] = {
            'url': url,
            'title': title,
            'price': price,
            'shipping': shipping,
            'best_offer': best_offer,
            'auction': auction,
            'has_buy_it_now': has_bin,
            'buy_it_now_price': bin_price,
        }

    logging.info(f"Обработано товаров основной выдачи: {len(items)}")
    return items

def perform_initial_snapshot():
    logging.info("Начальный снимок...")
    html = fetch_ebay_html_with_retry()
    if not html:
        return False
    items = parse_ebay_listings(html, max_items=MAX_ITEMS)
    if not items:
        return False
    add_seen_ids_batch(list(items.keys()))
    logging.info(f"Снимок: {len(items)} товаров")
    return True

def calculate_total_price(price_str, shipping_str, buy_it_now_price_str=None, is_auction=False):
    if not price_str or price_str == "Цена не указана (не GBP)" or "до" in price_str:
        return None

    price_num = None
    if is_auction and buy_it_now_price_str:
        match = re.search(r'([\d,]+\.?\d*)', buy_it_now_price_str.replace(',', ''))
        if match:
            price_num = float(match.group(1))
    if price_num is None:
        match = re.search(r'([\d,]+\.?\d*)', price_str.replace(',', ''))
        if match:
            price_num = float(match.group(1))
    if price_num is None:
        return None

    shipping_num = 0.0
    if shipping_str and shipping_str != "Бесплатно" and shipping_str != "не указана" and shipping_str is not None:
        match = re.search(r'([\d,]+\.?\d*)', shipping_str.replace(',', ''))
        if match:
            shipping_num = float(match.group(1))

    total_gbp = price_num + shipping_num
    total_uah = int(total_gbp * GBP_TO_UAH) + EXTRA_DELIVERY_COST
    return total_uah

def check_and_send_new_items():
    html = fetch_ebay_html_with_retry()
    if not html:
        logging.warning("Не удалось загрузить страницу, проверка пропущена")
        return 'fetch_error'

    current = parse_ebay_listings(html)
    if current is None:
        logging.warning("Структура основной выдачи eBay не распознана; проверка пропущена без изменений БД")
        # Это НЕ означает, что proxy плохой: HTTP 200 уже мог быть успешным.
        # Не запускаем из-за DOM-ошибки ускоренный proxy-discovery каждые 6-10 сек.
        return 'parse_error'
    if not current:
        logging.info("Основная выдача eBay распознана, но подходящих карточек нет")
        return 'ok'

    # ВАЖНО: вместо скачивания всех ~18k seen_items атомарно пробуем вставить только
    # текущие item_id основной выдачи (до 60). PRIMARY KEY + ON CONFLICT гарантирует, что старый товар
    # никогда не станет "новым" повторно, даже при коротком overlap двух Render instances.
    claimed = claim_new_seen_ids(list(current.keys()))
    new = []
    for item_id, data in current.items():
        if item_id in claimed:
            new.append({'id': item_id, **data})
            logging.info(
                f"НОВЫЙ [{item_id}]: {data['title'][:50]}... цена: {data['price']}, "
                f"доставка: {data.get('shipping')}, best_offer: {data.get('best_offer')}, "
                f"auction: {data.get('auction')}, has_buy_it_now: {data.get('has_buy_it_now')}"
            )

    if new:
        for item in new:
            msg = f"🇬🇧 <b>НОВЫЙ ТОВАР Англия</b> 🇬🇧\n\n<b>{item['title']}</b>\n\n"
            if item['price']:
                msg += f"💰 Цена: {item['price']}\n"
            else:
                msg += "💰 Цена не указана (не GBP)\n"
            if item['shipping']:
                msg += f"🚚 Доставка: {item['shipping']}\n"
            else:
                msg += "🚚 Доставка: не указана\n"
            if item.get('best_offer', False):
                msg += "✅ Сделать предложение (Best Offer)\n"
            if item.get('auction', False):
                if item.get('has_buy_it_now', False) and item.get('buy_it_now_price'):
                    msg += f"⏰ Аукцион / Buy It Now цена: {item['buy_it_now_price']}\n"
                elif item.get('has_buy_it_now', False):
                    msg += "⏰ Аукцион / Buy It Now\n"
                else:
                    msg += "⏰ Аукцион\n"
            if not item.get('auction', False) or (
                item.get('auction', False) and item.get('has_buy_it_now', False)
            ):
                total = calculate_total_price(
                    item['price'],
                    item['shipping'],
                    item.get('buy_it_now_price'),
                    is_auction=item.get('auction', False),
                )
                if total is not None:
                    msg += f"\nЗа все (с доставкой в Украину): <b>{total}грн</b>"
            msg += f"\n\n🔗 <a href='{item['url']}'>Ссылка на товар</a>"

            # item_id уже записан в seen_items ДО Telegram. Даже если Render внезапно
            # перезапустится после отправки, этот товар не пойдёт по второму кругу.
            if not send_telegram_message(msg):
                logging.error(
                    f"Telegram не подтвердил отправку нового item {item['id']}. "
                    "ID оставлен в seen_items специально, чтобы не создать повторную отправку."
                )
            time.sleep(1)
    else:
        logging.info("Новых нет")
    return 'ok'


def proxy_preflight_warm_worker():
    """
    Пока fixed proxy исправен, поддерживает свежий резерв БЕЗ eBay HTTP-запросов.

    Основной режим V6.19: TCP proxy -> CONNECT/SOCKS tunnel -> валидный TLS handshake
    к нейтральному endpoint. Это отсекает большую часть CONNECT/SSL/мертвых proxy заранее.
    Если quality-preflight отключён ENV, остаётся безопасный старый TCP-only fallback.
    """
    db_ready_event.wait()
    mode = 'HTTPS/TLS quality' if PROXY_QUALITY_PREFLIGHT_ENABLED else 'TCP-only'
    logging.info(
        f"🧪 Smart reserve worker запущен: mode={mode}, "
        f"batch={PROXY_PREFLIGHT_WARM_BATCH}, target={PROXY_PREFLIGHT_RESERVE_TARGET}, "
        f"workers={PROXY_PREFLIGHT_WARM_CONCURRENCY}, "
        f"quality_timeout={PROXY_QUALITY_TIMEOUT:.1f}s, tcp_timeout={PROXY_PREFLIGHT_CONNECT_TIMEOUT:.1f}s"
    )

    while True:
        try:
            if (
                not PROXY_PREFLIGHT_ENABLED
                or PROXY_PREFLIGHT_WARM_BATCH <= 0
                or is_paused
                or fixed_proxy is None
            ):
                proxy_preflight_wakeup_event.wait(timeout=PROXY_PREFLIGHT_WARM_INTERVAL)
                proxy_preflight_wakeup_event.clear()
                continue

            # Managed provider lists are metadata/API calls only; they do not consume proxy bandwidth.
            provider_manager.refresh_all(force=False)

            # Пока fixed работает, поддерживаем не только health-cache, но и СВЕЖИЙ
            # standard ProxyScrape snapshot. Внутренний refresh_interval ограничивает
            # это примерно одним скачиванием в минуту; warm reserve при refresh сохраняется.
            proxy_manager.refresh_proxies(force=False, emergency=False)

            excluded = {_proxy_host(fixed_proxy)} if fixed_proxy else set()

            if PROXY_QUALITY_PREFLIGHT_ENABLED:
                quality_before, tcp_before = proxy_manager.warm_reserve_stats()
                batch_limit = (
                    PROXY_PREFLIGHT_WARM_BATCH
                    if quality_before < PROXY_PREFLIGHT_RESERVE_TARGET
                    else PROXY_PREFLIGHT_MAINTENANCE_BATCH
                )
                candidates = proxy_manager.get_quality_preflight_candidates(
                    batch_limit,
                    excluded_hosts=excluded,
                )

                checked = 0
                ok_count = 0
                reason_counts = {}
                if candidates:
                    with ThreadPoolExecutor(
                        max_workers=min(PROXY_PREFLIGHT_WARM_CONCURRENCY, len(candidates)),
                        thread_name_prefix='proxy-quality-preflight',
                    ) as executor:
                        future_map = {
                            executor.submit(_quality_https_preflight_proxy, p): p
                            for p in candidates
                        }
                        for future in future_map:
                            checked += 1
                            try:
                                ok, reason = future.result()
                            except Exception:
                                ok, reason = False, 'proxy_error'
                            if ok:
                                ok_count += 1
                            else:
                                reason = reason or 'proxy_error'
                                reason_counts[reason] = reason_counts.get(reason, 0) + 1

                quality_after, tcp_after = proxy_manager.warm_reserve_stats()
                # Логируем и maintenance-проходы: по нему можно реально оценивать качество пула.
                if checked:
                    failure_summary = ', '.join(
                        f"{k}={v}" for k, v in sorted(reason_counts.items())
                    ) or 'нет'
                    logging.info(
                        f"🧪 Smart reserve: HTTPS-ready={quality_after}, TCP-only={tcp_after}; "
                        f"проверено={checked}, quality_ok={ok_count}, failures[{failure_summary}]"
                    )
            else:
                candidates = proxy_manager.get_preflight_candidates(
                    PROXY_PREFLIGHT_WARM_BATCH,
                    excluded_hosts=excluded,
                )
                if candidates:
                    with ThreadPoolExecutor(
                        max_workers=min(PROXY_PREFLIGHT_WARM_CONCURRENCY, len(candidates)),
                        thread_name_prefix='proxy-preflight',
                    ) as executor:
                        futures = [executor.submit(_tcp_preflight_proxy, p, True) for p in candidates]
                        ok_count = 0
                        for future in futures:
                            try:
                                ok, _reason = future.result()
                                if ok:
                                    ok_count += 1
                            except Exception:
                                pass
                    logging.info(
                        f"🧪 TCP reserve fallback: живых {ok_count}/{len(candidates)}"
                    )

            proxy_preflight_wakeup_event.wait(timeout=PROXY_PREFLIGHT_WARM_INTERVAL)
            proxy_preflight_wakeup_event.clear()
        except Exception as e:
            logging.warning(f"Smart reserve worker: {e}")
            proxy_preflight_wakeup_event.wait(timeout=PROXY_PREFLIGHT_WARM_INTERVAL)
            proxy_preflight_wakeup_event.clear()


def bot_worker():
    global is_paused
    logging.info("🤖 Бот-воркер запущен")
    db_ready_event.wait()
    if is_db_empty():
        if not perform_initial_snapshot():
            send_telegram_message("❌ Ошибка инициализации")
            return
        startup_line = "✅ Бот запущен, начальный снимок сделан"
    else:
        startup_line = "✅ Бот запущен / перезапущен"

    seen_total = get_seen_count_cached()
    seen_line = f"\n📚 В базе: {seen_total} товаров." if seen_total is not None else ""
    send_telegram_message(
        startup_line +
        "\n🇬🇧 eBay UK monitor v6.20 SmartReserve работает." +
        seen_line +
        "\nКоманды: /stop /start /list (/auctions) /delauction НОМЕР_ЛОТА"
        "\nМожно отправить ссылку на eBay-аукцион — сохраню точное время и напомню заранее.",
        reply_markup=auction_list_only_keyboard(),
    )
    while True:
        if is_paused:
            time.sleep(2)
            continue
        try:
            result = check_and_send_new_items()
            if result == 'ok':
                wait = random.uniform(CHECK_INTERVAL, CHECK_INTERVAL + 12)
                logging.info(f"✅ Успешная проверка. Следующая через {wait:.0f} секунд.")
            elif result == 'parse_error':
                # Сеть/eBay были доступны, проблема только в DOM-разметке. Нельзя
                # ошибочно объявлять рабочий proxy плохим и запускать частый discovery.
                wait = random.uniform(CHECK_INTERVAL, CHECK_INTERVAL + 12)
                logging.warning(
                    f"⚠️ eBay доступен, но разметка не распознана. "
                    f"Повтор обычной проверки через {wait:.0f} секунд без смены proxy."
                )
            else:
                # Реальная проблема загрузки/proxy: после активного discovery оставляем
                # короткий jitter, чтобы быстро вернуться к поиску, но не крутить busy-loop.
                wait = random.uniform(FAILED_SEARCH_RETRY_MIN, FAILED_SEARCH_RETRY_MAX)
                logging.info(f"⚠️ Рабочий proxy пока не найден. Новый цикл через {wait:.1f} секунд.")
            time.sleep(wait)
        except Exception as e:
            logging.error(f"Ошибка в основном цикле: {e}", exc_info=True)
            time.sleep(5)

def start_leader_workers():
    """Инициализирует БД и запускает фоновые задачи только в leader-instance."""
    init_db()
    initialize_seen_count_cache()
    db_ready_event.set()
    leader_active_event.set()
    logging.info("👑 Эта Render-копия стала leader; запускаем фоновые worker-ы")
    logging.info(
        "🌐 Multi-provider v6.20: "
        f"ProxyScrape Premium={'ON' if PROXYSCRAPE_PREMIUM_API_KEY else 'OFF'}, "
        f"Webshare={'ON (rescue)' if WEBSHARE_API_KEY else 'OFF'}, "
        f"Webshare unlock={WEBSHARE_UNLOCK_AFTER:.0f}s/{WEBSHARE_UNLOCK_ATTEMPTS} probes"
    )

    threading.Thread(target=telegram_listener, daemon=True, name='telegram-listener').start()
    threading.Thread(target=connection_watchdog, daemon=True, name='connection-watchdog').start()
    threading.Thread(target=proxy_preflight_warm_worker, daemon=True, name='proxy-preflight-worker').start()
    threading.Thread(target=auction_link_worker, daemon=True, name='auction-link-worker').start()
    threading.Thread(target=auction_reminder_worker, daemon=True, name='auction-reminder-worker').start()
    threading.Thread(target=auction_status_worker, daemon=True, name='auction-status-worker').start()
    threading.Thread(target=bot_worker, daemon=True, name='main-ebay-worker').start()


def leader_supervisor():
    """Не даёт двум Render instances одновременно запускать Telegram/eBay workers."""
    last_standby_log = 0.0
    while True:
        conn = try_acquire_leader_lock()
        if conn is None:
            now = time.monotonic()
            if now - last_standby_log >= 30:
                logging.info("🟡 Standby: другая Render-копия уже держит bot leader-lock")
                last_standby_log = now
            time.sleep(LEADER_RETRY_INTERVAL)
            continue

        try:
            start_leader_workers()
            # Держим session-level advisory lock отдельным соединением.
            # Если оно умерло, PostgreSQL сам освободит lock. Чтобы старая копия
            # не продолжала workers без lock, завершаем процесс — Render его перезапустит.
            while True:
                time.sleep(LEADER_HEALTH_INTERVAL)
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
        except Exception as e:
            logging.critical(f"Потерян PostgreSQL leader-lock/connection: {e}", exc_info=True)
            try:
                conn.close()
            except Exception:
                pass
            os._exit(1)


@app.route('/')
def index():
    role = "leader" if leader_active_event.is_set() else "standby"
    return f"eBay бот работает (Великобритания, adaptive parallel UK v6.20 SmartReserve, {role})"


@app.route('/health')
def health():
    # /health намеренно не зависит от eBay/proxy/Aiven: UptimeRobot должен держать
    # Render Web Service активным даже во время временного сбоя внешних сервисов.
    return "OK", 200


if __name__ == "__main__":
    # Flask привязывается к PORT сразу, чтобы новый Render instance прошёл health/port check.
    # Фоновые задачи стартуют только после получения PostgreSQL leader-lock.
    threading.Thread(target=leader_supervisor, daemon=True, name='leader-supervisor').start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
