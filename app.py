import os
import sys
import ssl
import time
import random
import re
import json
import gc
import ctypes
import threading
import logging
import html as html_lib
import hashlib
import socket
from collections import deque
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
# V6.23: transient Aiven/network connect failures should not kill a worker on the first
# failed TCP/SSL handshake. Retry ONLY connection establishment; SQL transactions are
# intentionally not auto-replayed because an ambiguous COMMIT must never create hidden
# notification semantics.
DB_CONNECT_ATTEMPTS = max(1, min(int(os.getenv("DB_CONNECT_ATTEMPTS", "3")), 5))
DB_CONNECT_RETRY_BASE = max(0.10, float(os.getenv("DB_CONNECT_RETRY_BASE", "0.35")))
DB_MAIN_RETRY_WAIT = max(3.0, float(os.getenv("DB_MAIN_RETRY_WAIT", "8")))

# V6.30/V6.32: persist only SAFE metadata for the most recently proven main proxy.
# Render zero-downtime deploys start a fresh Python process, so RAM-only reputation is lost.
# A short one-shot restart probe can recover the exact last-good endpoint before broad discovery.
# Credentials are NEVER written to PostgreSQL: managed providers are resolved back to their
# current authenticated URL from the fresh provider API snapshot by host/port.
RESTART_STICKY_PROXY_ENABLED = os.getenv("RESTART_STICKY_PROXY_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on")
RESTART_STICKY_PROXY_MAX_AGE = max(300, int(os.getenv("RESTART_STICKY_PROXY_MAX_AGE", str(12 * 3600))))
RESTART_STICKY_PROXY_REFRESH = max(120, int(os.getenv("RESTART_STICKY_PROXY_REFRESH", "600")))
RESTART_STICKY_CONNECT_TIMEOUT = max(1.5, min(float(os.getenv("RESTART_STICKY_CONNECT_TIMEOUT", "3.5")), 5.0))
RESTART_STICKY_READ_TIMEOUT = max(4.0, min(float(os.getenv("RESTART_STICKY_READ_TIMEOUT", "7.0")), 10.0))
RESTART_STICKY_STATE_KEY = "main_restart_sticky_proxy_v1"

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
# V6.27: реальные логи V6.26 показали median failover ~10 сек., но один длинный outlier 44.7 сек.
# Сохраняем быстрый безопасный разгон: 4 сразу -> 5 после ~8 сек. -> 6 после
# ~18 сек., не дожидаясь deep-emergency. При живом fixed proxy всё равно выполняется ОДИН
# обычный eBay request — частота основного мониторинга не меняется. Memory guard ниже может
# динамически вернуть ширину к 4 только около реального лимита RAM Render.
PROBE_CONCURRENCY = max(4, min(int(os.getenv("PROBE_CONCURRENCY", "4")), 4))
PROBE_ESCALATED_CONCURRENCY = max(
    PROBE_CONCURRENCY,
    min(int(os.getenv("PROBE_ESCALATED_CONCURRENCY", "5")), 5),
)
PROBE_ESCALATE_AFTER = max(5.0, float(os.getenv("PROBE_ESCALATE_AFTER", "8")))
PROBE_DEEP_CONCURRENCY = max(
    PROBE_ESCALATED_CONCURRENCY,
    min(int(os.getenv("PROBE_DEEP_CONCURRENCY", "6")), 6),
)
PROBE_DEEP_ESCALATE_AFTER = max(
    PROBE_ESCALATE_AFTER + 2.0,
    float(os.getenv("PROBE_DEEP_ESCALATE_AFTER", "18")),
)
# V6.33: two late burst stages. They are deliberately unavailable during normal/short
# failovers and are additionally gated by live RSS, so the extra curl responses cannot
# consume the last Render memory reserve.
PROBE_BURST_CONCURRENCY = max(
    PROBE_DEEP_CONCURRENCY,
    min(int(os.getenv("PROBE_BURST_CONCURRENCY", "7")), 7),
)
PROBE_BURST_ESCALATE_AFTER = max(
    PROBE_DEEP_ESCALATE_AFTER + 4.0,
    float(os.getenv("PROBE_BURST_ESCALATE_AFTER", "28")),
)
PROBE_MAX_CONCURRENCY = max(
    PROBE_BURST_CONCURRENCY,
    min(int(os.getenv("PROBE_MAX_CONCURRENCY", "8")), 8),
)
PROBE_MAX_ESCALATE_AFTER = max(
    PROBE_BURST_ESCALATE_AFTER + 5.0,
    float(os.getenv("PROBE_MAX_ESCALATE_AFTER", "38")),
)
PROBE_BURST_MEMORY_CEILING_MB = max(
    340, min(int(os.getenv("PROBE_BURST_MEMORY_CEILING_MB", "390")), 420)
)
PROBE_CONNECT_TIMEOUT = float(os.getenv("PROBE_CONNECT_TIMEOUT", "3.5"))
# В старой версии после 45 сек. timeout искусственно увеличивался до 4.5/12 и один
# полуживой proxy мог держать целый batch 11+ секунд. При сотнях кандидатов выгоднее
# продолжать быстро перебирать пул тем же строгим timeout.
PROBE_READ_TIMEOUT = float(os.getenv("PROBE_READ_TIMEOUT", "8"))
# Current production log: every observed winner was HTTP, while SOCKS5 produced
# many timeout/403/SSL failures. Do not BAN SOCKS5; simply stop forcing it into the
# first seconds of failover when viable HTTP candidates exist.
SOCKS_MIX_DELAY = max(0.0, float(os.getenv("SOCKS_MIX_DELAY", "15")))
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
# V6.27: widen only a genuinely long search sooner; normal 3–13s failovers never pay this cost.
PROXY_EMERGENCY_TRIGGER_AFTER = max(15.0, float(os.getenv("PROXY_EMERGENCY_TRIGGER_AFTER", "22")))
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
# V6.28: log the TOTAL interval between valid eBay HTTP 200 responses after a meaningful
# outage. Per-discovery timings alone hide multi-cycle outages (e.g. 4x75s + retries).
CONNECTION_RECOVERY_LOG_AFTER = max(
    45, int(os.getenv("CONNECTION_RECOVERY_LOG_AFTER", "60"))
)
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

# v6.20 за ~53 минуты сделал ~1684 neutral quality-checks; при этом для быстрого failover
# реально достаточно короткой первой wave из 4 verified-free. Поэтому держим ~8 свежих
# HTTPS-capable резервов, а не 12+, и уменьшаем тяжёлый background batch без потери fallback.
PROXY_PREFLIGHT_WARM_BATCH = max(4, min(int(os.getenv("PROXY_PREFLIGHT_WARM_BATCH", "12")), 24))
# V6.26 AdaptiveFast: keep the same reserve depth, but limit simultaneous TLS handshakes.
# This does NOT slow the 15-27 second main eBay checks; it only lowers background peak RAM.
PROXY_PREFLIGHT_WARM_CONCURRENCY = max(
    2, min(int(os.getenv("PROXY_PREFLIGHT_WARM_CONCURRENCY", "4")), 6)
)
PROXY_PREFLIGHT_WARM_INTERVAL = max(10.0, float(os.getenv("PROXY_PREFLIGHT_WARM_INTERVAL", "20")))
PROXY_PREFLIGHT_RESERVE_TARGET = max(
    4, min(int(os.getenv("PROXY_PREFLIGHT_RESERVE_TARGET", "8")), 24)
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

# V6.26 AdaptiveFast: Render Free is capped at 512 MB. The observed V6.25 RSS stayed
# around 198–290 MB, so 400 MB was unnecessarily conservative as a HIGH-pressure point.
# Keep early GC/trim as cheap prevention, but do not pause background work/cap discovery until
# ~450 MB. A second emergency ceiling near 485 MB preserves headroom below Render's 512 MB kill.
MEMORY_GUARD_ENABLED = (os.getenv("MEMORY_GUARD_ENABLED", "true").strip().lower() not in ("0", "false", "no", "off"))
MEMORY_GUARD_INTERVAL = max(5.0, float(os.getenv("MEMORY_GUARD_INTERVAL", "10")))
MEMORY_SOFT_MB = max(220, int(os.getenv("MEMORY_SOFT_MB", "330")))
MEMORY_HIGH_MB = max(MEMORY_SOFT_MB + 40, int(os.getenv("MEMORY_HIGH_MB", "450")))
MEMORY_EMERGENCY_MB = max(MEMORY_HIGH_MB + 20, min(int(os.getenv("MEMORY_EMERGENCY_MB", "485")), 500))
MEMORY_CLEAR_MB = min(MEMORY_HIGH_MB - 30, max(180, int(os.getenv("MEMORY_CLEAR_MB", "360"))))
PROXY_REPUTATION_TTL = max(1800, int(os.getenv("PROXY_REPUTATION_TTL", "21600")))
PROXY_REPUTATION_MAX = max(1000, int(os.getenv("PROXY_REPUTATION_MAX", "7000")))
PROVIDER_UNIQUE_STATS_CAP = max(500, int(os.getenv("PROVIDER_UNIQUE_STATS_CAP", "4096")))


# ============ V6.22 MULTI-WEBSHARE / MAKE-BEFORE-BREAK BRIDGE ============
# Secrets are read ONLY from Render Environment Variables. Never hard-code API keys.
def _collect_webshare_api_keys():
    """Backwards-compatible Webshare key list without ever logging secret values."""
    values = []
    legacy = (os.getenv("WEBSHARE_API_KEY") or "").strip()
    if legacy:
        values.append(legacy)
    raw = (os.getenv("WEBSHARE_API_KEYS") or "").strip()
    if raw:
        for value in re.split(r"[\s,;]+", raw):
            value = value.strip()
            if value:
                values.append(value)
    for idx in range(2, 9):
        value = (os.getenv(f"WEBSHARE_API_KEY_{idx}") or "").strip()
        if value:
            values.append(value)
    # Preserve order and silently remove duplicates.
    return tuple(dict.fromkeys(values))


def _collect_proxio_api_keys():
    """Collect Proxio keys from Render without ever logging the secret values.

    Supported forms:
      * PROXIO_API_KEYS="key1,key2,..."
      * PROXIO_API_KEY / PROXIO_API_KEY_1 ... PROXIO_API_KEY_8

    A single API request uses exactly one key. Keys are rotated so five 100-call/day
    accounts act as one quota pool instead of five parallel API calls.
    """
    values = []
    legacy = (os.getenv("PROXIO_API_KEY") or "").strip()
    if legacy:
        values.append(legacy)
    raw = (os.getenv("PROXIO_API_KEYS") or "").strip()
    if raw:
        for value in re.split(r"[\s,;]+", raw):
            value = value.strip()
            if value:
                values.append(value)
    for idx in range(1, 9):
        value = (os.getenv(f"PROXIO_API_KEY_{idx}") or "").strip()
        if value:
            values.append(value)
    return tuple(dict.fromkeys(values))


WEBSHARE_API_KEY = (os.getenv("WEBSHARE_API_KEY") or "").strip()
WEBSHARE_API_KEYS = _collect_webshare_api_keys()
PROXIO_API_KEYS = _collect_proxio_api_keys()
PROXYSCRAPE_PREMIUM_API_KEY = (os.getenv("PROXYSCRAPE_PREMIUM_API_KEY") or "").strip()
PROXYSCRAPE_PREMIUM_SUBACCOUNT_ID = (os.getenv("PROXYSCRAPE_PREMIUM_SUBACCOUNT_ID") or "").strip()
PROVIDER_API_TIMEOUT = max(3.0, min(float(os.getenv("PROVIDER_API_TIMEOUT", "10")), 20.0))
PROXYSCRAPE_PREMIUM_REFRESH = max(30, int(os.getenv("PROXYSCRAPE_PREMIUM_REFRESH", "60")))
WEBSHARE_REFRESH = max(60, int(os.getenv("WEBSHARE_REFRESH", "300")))
WEBSHARE_STATS_REFRESH = max(300, int(os.getenv("WEBSHARE_STATS_REFRESH", "600")))

# V6.22: one metered Webshare slot is allowed immediately. The real provider billing
# screenshot showed ~21 MB / 112 requests, so discovery probes are cheap; the expensive
# scenario is leaving Webshare as fixed for hours. We solve that with make-before-break handoff.
WEBSHARE_FIRST_BATCH = (os.getenv("WEBSHARE_FIRST_BATCH", "true").strip().lower() not in ("0", "false", "no"))
WEBSHARE_UNLOCK_AFTER = max(0.0, float(os.getenv("WEBSHARE_UNLOCK_AFTER", "4")))
WEBSHARE_UNLOCK_ATTEMPTS = max(0, int(os.getenv("WEBSHARE_UNLOCK_ATTEMPTS", "6")))
WEBSHARE_USAGE_SOFT_LIMIT = min(0.95, max(0.50, float(os.getenv("WEBSHARE_USAGE_SOFT_LIMIT", "0.85"))))
WEBSHARE_USAGE_HARD_LIMIT = min(0.995, max(WEBSHARE_USAGE_SOFT_LIMIT + 0.02, float(os.getenv("WEBSHARE_USAGE_HARD_LIMIT", "0.98"))))
WEBSHARE_HANDOFF_AFTER = max(30.0, float(os.getenv("WEBSHARE_HANDOFF_AFTER", "90")))
WEBSHARE_HANDOFF_HIGH_USAGE_AFTER = max(15.0, float(os.getenv("WEBSHARE_HANDOFF_HIGH_USAGE_AFTER", "30")))
WEBSHARE_HANDOFF_INTERVAL = max(10.0, float(os.getenv("WEBSHARE_HANDOFF_INTERVAL", "20")))
WEBSHARE_HANDOFF_BATCH = max(2, min(int(os.getenv("WEBSHARE_HANDOFF_BATCH", "4")), 8))
WEBSHARE_HANDOFF_CONCURRENCY = max(1, min(int(os.getenv("WEBSHARE_HANDOFF_CONCURRENCY", "2")), 3))
# V6.27: production showed a FREE handoff candidate that passed 2x HTTP 200 but returned 403
# immediately after adoption. Three confirmations separated by 10s provide a much stronger
# stability signal while the metered Webshare fixed session remains online in parallel.
WEBSHARE_HANDOFF_CONFIRMATIONS = max(1, min(int(os.getenv("WEBSHARE_HANDOFF_CONFIRMATIONS", "3")), 3))
WEBSHARE_HANDOFF_CONFIRM_DELAY = max(1.0, float(os.getenv("WEBSHARE_HANDOFF_CONFIRM_DELAY", "10")))
WEBSHARE_HANDOFF_READY_TTL = max(30.0, float(os.getenv("WEBSHARE_HANDOFF_READY_TTL", "90")))
# V6.35: a background Webshare->FREE scout is opportunistic. A transient timeout/error
# on a FREE proxy that was eBay-good recently must not poison the PRIMARY failover path.
# Keep a separate scout-only cooldown so the handoff worker does not hammer it, while
# main discovery remains free to make its own decision if the current fixed proxy dies.
WEBSHARE_HANDOFF_TRANSIENT_COOLDOWN = max(20.0, min(
    float(os.getenv("WEBSHARE_HANDOFF_TRANSIENT_COOLDOWN", "45")), 120.0
))
MANAGED_PROVIDER_CIRCUIT_SECONDS = max(120, int(os.getenv("MANAGED_PROVIDER_CIRCUIT_SECONDS", "300")))
# V6.28: keep the full provider circuit as a safety net, but do not make a healthy
# Webshare pool disappear for the whole 5 minutes. During a shared-pool circuit we allow
# one controlled half-open probe after 60s and then at most once per 60s. A success closes
# the circuit immediately; another 403 simply leaves the circuit in place.
WEBSHARE_CIRCUIT_HALF_OPEN_AFTER = max(
    30.0, float(os.getenv("WEBSHARE_CIRCUIT_HALF_OPEN_AFTER", "60"))
)
WEBSHARE_CIRCUIT_HALF_OPEN_INTERVAL = max(
    30.0, float(os.getenv("WEBSHARE_CIRCUIT_HALF_OPEN_INTERVAL", "60"))
)
# V6.23: current production log showed a correlated 403 wall: all Webshare addresses
# from one account were consumed almost back-to-back, then dozens of Premium IPs were
# probed before its 32-block circuit opened. Account-level Webshare circuit protects
# metered traffic; Premium is first THROTTLED (one slot/batch), then circuit-broken only
# after a longer wall so a late good Premium endpoint can still be discovered.
WEBSHARE_BLOCK_CIRCUIT_STREAK = max(3, min(int(os.getenv("WEBSHARE_BLOCK_CIRCUIT_STREAK", "4")), 10))
PREMIUM_BLOCK_THROTTLE_STREAK = max(4, min(int(os.getenv("PREMIUM_BLOCK_THROTTLE_STREAK", "8")), 20))
PREMIUM_BLOCK_CIRCUIT_STREAK = max(
    PREMIUM_BLOCK_THROTTLE_STREAK + 4,
    min(int(os.getenv("PREMIUM_BLOCK_CIRCUIT_STREAK", "20")), 40),
)
# V6.31: Premium gets the same conservative recovery semantics as Webshare.
# A 5-minute circuit remains the safety net, but after 60s exactly one half-open
# Premium endpoint may be tested per minute. Late in-flight 403s never extend the circuit.
PREMIUM_CIRCUIT_HALF_OPEN_AFTER = max(
    30.0, float(os.getenv("PREMIUM_CIRCUIT_HALF_OPEN_AFTER", "60"))
)
PREMIUM_CIRCUIT_HALF_OPEN_INTERVAL = max(
    30.0, float(os.getenv("PREMIUM_CIRCUIT_HALF_OPEN_INTERVAL", "60"))
)

# V6.32/V6.33: HProxy + Databay are UNMETERED public sources.
# V6.36 adds Proxio as a quota-limited quality source. Proxio is deliberately handled
# as a small snapshot/reserve, NOT merged into the large ProxyScrape pool and NOT allowed
# to increase the global discovery ceiling. One API call fetches up to 200 Elite HTTPS
# candidates; actual eBay probes are only made when normal failover needs them.
HPROXY_FREE_ENABLED = os.getenv("HPROXY_FREE_ENABLED", "true").strip().lower() not in ("0", "false", "no", "off")
DATABAY_FREE_ENABLED = os.getenv("DATABAY_FREE_ENABLED", "true").strip().lower() not in ("0", "false", "no", "off")
PROXIO_FREE_ENABLED = bool(PROXIO_API_KEYS) and os.getenv("PROXIO_FREE_ENABLED", "true").strip().lower() not in ("0", "false", "no", "off")
EXTERNAL_FREE_API_TIMEOUT = max(3.0, min(float(os.getenv("EXTERNAL_FREE_API_TIMEOUT", "8")), 15.0))
HPROXY_FREE_REFRESH = max(60, int(os.getenv("HPROXY_FREE_REFRESH", "60")))
DATABAY_FREE_REFRESH = max(120, int(os.getenv("DATABAY_FREE_REFRESH", "300")))

# Proxio says its live list is re-tested roughly every five minutes, so refreshing faster
# only burns API quota without adding useful freshness. The user currently has 100 calls/day
# on each key; keep 20% headroom and automatically lengthen the refresh interval if fewer
# keys are configured. With 5 keys and defaults: <=288 calls/day total (~58/key/day).
PROXIO_CALLS_PER_KEY_PER_DAY = max(10, int(os.getenv("PROXIO_CALLS_PER_KEY_PER_DAY", "100")))
PROXIO_QUOTA_HEADROOM = min(0.95, max(0.50, float(os.getenv("PROXIO_QUOTA_HEADROOM", "0.80"))))
PROXIO_REQUESTED_REFRESH = max(300, int(os.getenv("PROXIO_FREE_REFRESH", "300")))
_proxio_safe_calls_day = max(
    1,
    int(max(1, len(PROXIO_API_KEYS)) * PROXIO_CALLS_PER_KEY_PER_DAY * PROXIO_QUOTA_HEADROOM),
)
_proxio_quota_safe_interval = max(1, (86400 + _proxio_safe_calls_day - 1) // _proxio_safe_calls_day)
PROXIO_FREE_REFRESH = max(PROXIO_REQUESTED_REFRESH, _proxio_quota_safe_interval)
PROXIO_API_LIMIT = max(50, min(int(os.getenv("PROXIO_API_LIMIT", "200")), 200))
PROXIO_SNAPSHOT_LIMIT = max(50, min(int(os.getenv("PROXIO_SNAPSHOT_LIMIT", "200")), PROXIO_API_LIMIT))
PROXIO_API_RETRY_KEYS = max(1, min(int(os.getenv("PROXIO_API_RETRY_KEYS", "2")), 2))
PROXIO_PRIMARY_MIN_RELIABILITY = max(0.0, min(float(os.getenv("PROXIO_PRIMARY_MIN_RELIABILITY", "80")), 100.0))
PROXIO_PRIMARY_MAX_LATENCY_S = max(0.2, min(float(os.getenv("PROXIO_PRIMARY_MAX_LATENCY_S", "2.5")), 10.0))
PROXIO_FALLBACK_MIN_RELIABILITY = max(0.0, min(float(os.getenv("PROXIO_FALLBACK_MIN_RELIABILITY", "60")), PROXIO_PRIMARY_MIN_RELIABILITY))
PROXIO_FALLBACK_MAX_LATENCY_S = max(PROXIO_PRIMARY_MAX_LATENCY_S, min(float(os.getenv("PROXIO_FALLBACK_MAX_LATENCY_S", "5.0")), 15.0))

HPROXY_FREE_ELITE_LIMIT = max(50, min(int(os.getenv("HPROXY_FREE_ELITE_LIMIT", "160")), 400))
HPROXY_FREE_ANON_LIMIT = max(25, min(int(os.getenv("HPROXY_FREE_ANON_LIMIT", "90")), 250))
HPROXY_FREE_MAX_LATENCY_MS = max(500, min(int(os.getenv("HPROXY_FREE_MAX_LATENCY_MS", "2000")), 5000))
HPROXY_FREE_ELITE_MIN_UPTIME = max(50, min(int(os.getenv("HPROXY_FREE_ELITE_MIN_UPTIME", "85")), 100))
HPROXY_FREE_ANON_MIN_UPTIME = max(50, min(int(os.getenv("HPROXY_FREE_ANON_MIN_UPTIME", "70")), 100))
DATABAY_FREE_ELITE_LIMIT = max(50, min(int(os.getenv("DATABAY_FREE_ELITE_LIMIT", "160")), 400))
DATABAY_FREE_ANON_LIMIT = max(25, min(int(os.getenv("DATABAY_FREE_ANON_LIMIT", "90")), 250))
EXTERNAL_FREE_EARLY_SLOTS = max(0, min(int(os.getenv("EXTERNAL_FREE_EARLY_SLOTS", "1")), 1))
EXTERNAL_FREE_ESCALATED_SLOTS = max(EXTERNAL_FREE_EARLY_SLOTS, min(int(os.getenv("EXTERNAL_FREE_ESCALATED_SLOTS", "2")), 2))
# Small neutral TLS/CONNECT warm-check budget for HProxy+Databay+Proxio. It never calls eBay
# and shares the existing SmartReserve worker pool, so it does not add concurrent
# background threads beyond the already bounded preflight executor.
EXTERNAL_FREE_PREFLIGHT_SLOTS = max(0, min(int(os.getenv("EXTERNAL_FREE_PREFLIGHT_SLOTS", "4")), 4))
EXTERNAL_FREE_STATS_INTERVAL = max(60, int(os.getenv("EXTERNAL_FREE_STATS_INTERVAL", "300")))

# Background SmartReserve may hold 12+ verified free proxies, but the real log showed that
# draining 10-12 of them before managed providers delayed recovery. Keep a deeper reserve in
# memory, while using only a short first wave before normal Premium/Webshare discovery.
WARM_STANDBY_TOTAL_LIMIT = max(4, min(int(os.getenv("WARM_STANDBY_TOTAL_LIMIT", "6")), 10))
WARM_STANDBY_FREE_QUALITY_LIMIT = max(
    2, min(int(os.getenv("WARM_STANDBY_FREE_QUALITY_LIMIT", "4")), WARM_STANDBY_TOTAL_LIMIT)
)

# eBay 403 on managed datacenter IPs usually outlived the old 5-minute cooldown in the real log.
# Longer provider-specific cooldown prevents wasting scarce Webshare and repeated Premium probes.
PREMIUM_BLOCK_COOLDOWNS = (1800, 3600, 3600, 3600)
WEBSHARE_BLOCK_COOLDOWNS = (1200, 1200, 1800, 2700)

# Approximate process-local warning threshold only. It never breaks an already working fixed session.
WEBSHARE_ESTIMATED_MB_WARN = max(10.0, float(os.getenv("WEBSHARE_ESTIMATED_MB_WARN", "250")))
PROVIDER_STATS_INTERVAL = max(60, int(os.getenv("PROVIDER_STATS_INTERVAL", "300")))

# Память outage живёт между соседними 75-секундными discovery. Ранее проверенные неизвестные
# IP не исчезают из пула, но новые IP идут раньше. Known-good всё ещё может получить controlled retry.
OUTAGE_HOST_MEMORY = max(180, int(os.getenv("OUTAGE_HOST_MEMORY", "600")))
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
# New user-submitted auction links have priority over background re-checks of already saved
# auctions. The normal item monitor remains independent and continues in parallel.
auction_user_job_active_event = threading.Event()
# Новый fixed proxy будит Smart Reserve немедленно, а не ждёт до 15 сек. polling interval.
proxy_preflight_wakeup_event = threading.Event()
# V6.25: background TLS reserve/handoff pauses while the main worker is already doing
# an expensive failover. This prevents two independent socket/thread bursts from stacking.
main_discovery_active_event = threading.Event()
memory_pressure_event = threading.Event()
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

# ============ MEMORY SAFETY (Render 512 MB) ============
_malloc_trim = None
try:
    _libc = ctypes.CDLL(None)
    _malloc_trim = getattr(_libc, 'malloc_trim', None)
    if _malloc_trim is not None:
        _malloc_trim.argtypes = [ctypes.c_size_t]
        _malloc_trim.restype = ctypes.c_int
except Exception:
    _malloc_trim = None


def _memory_rss_mb():
    """Current resident set size on Linux, or None if unavailable."""
    try:
        with open('/proc/self/status', 'r', encoding='utf-8') as fh:
            for line in fh:
                if line.startswith('VmRSS:'):
                    parts = line.split()
                    if len(parts) >= 2:
                        return float(parts[1]) / 1024.0
    except Exception:
        return None
    return None


def _release_python_memory(force_trim=False):
    """Collect cyclic DOM objects and, on glibc, return free arenas to the OS."""
    try:
        gc.collect()
    except Exception:
        pass
    if force_trim and _malloc_trim is not None:
        try:
            _malloc_trim(0)
        except Exception:
            pass


def _memory_maintenance(reason='', force=False):
    rss = _memory_rss_mb()
    if rss is None:
        return None
    if force or rss >= MEMORY_SOFT_MB:
        before = rss
        _release_python_memory(force_trim=True)
        after = _memory_rss_mb()
        if after is not None and (force or before >= MEMORY_HIGH_MB):
            logging.info(
                f"🧠 Memory cleanup{(' (' + reason + ')') if reason else ''}: "
                f"RSS {before:.0f}→{after:.0f} MB"
            )
        rss = after if after is not None else before
    if rss is not None:
        if rss >= MEMORY_HIGH_MB:
            memory_pressure_event.set()
        elif rss <= MEMORY_CLEAR_MB:
            memory_pressure_event.clear()
    return rss


def memory_guard_worker():
    db_ready_event.wait()
    if not MEMORY_GUARD_ENABLED:
        logging.info('🧠 Memory guard disabled by ENV')
        return
    last_state = None
    last_report = 0.0
    while True:
        try:
            rss = _memory_maintenance('guard', force=False)
            high = memory_pressure_event.is_set()
            now_mono = time.monotonic()
            if rss is not None and now_mono - last_report >= 300:
                logging.info(
                    f"🧠 RSS≈{rss:.0f} MB; discovery={'ON' if main_discovery_active_event.is_set() else 'OFF'}, "
                    f"pressure={'HIGH' if high else 'normal'}"
                )
                last_report = now_mono
            if high != last_state and rss is not None:
                if high:
                    logging.warning(
                        f"🧠 High memory pressure: RSS≈{rss:.0f} MB; "
                        f"pause background reserve and cap discovery until memory falls (emergency≈{MEMORY_EMERGENCY_MB} MB)"
                    )
                elif last_state is True:
                    logging.info(f"🧠 Memory pressure cleared: RSS≈{rss:.0f} MB")
                last_state = high
        except Exception as e:
            logging.debug(f"Memory guard skipped: {e}")
        time.sleep(MEMORY_GUARD_INTERVAL)

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
        previous_success_at = last_ebay_success_at
        first_success = previous_success_at is None
        was_alerted = connection_alert_sent
        recovery_gap = (now - previous_success_at) if previous_success_at is not None else None
        last_ebay_success_at = now
        connection_alert_sent = False
    if recovery_gap is not None and recovery_gap >= CONNECTION_RECOVERY_LOG_AFTER:
        logging.info(
            f"🔄 eBay связь восстановлена: между валидными HTTP 200 прошло "
            f"{recovery_gap:.1f} сек. ({recovery_gap / 60.0:.1f} мин.)"
        )
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
        source = provider_manager.source_label_fast(proxy) if 'provider_manager' in globals() else None
        if source == 'free' and 'external_free_manager' in globals():
            external_label = external_free_manager.label_fast(proxy)
            if external_label:
                source = external_label
        suffix = f" [{source}]" if source else ''
        return f"{scheme}://{host}{port}{suffix}"
    except Exception:
        return '<proxy>'


class ProviderManager:
    """Fail-open managed proxy layer with multi-account Webshare balancing.

    Important invariants:
      * API failures never break the legacy free pool.
      * API keys / proxy credentials are never logged.
      * Webshare accounts are balanced by REAL provider bandwidth usage when the
        stats API is available, not by decompressed HTML bytes seen by Python.
      * Expired/unauthorized Premium or Webshare accounts are temporarily removed
        instead of leaving stale authenticated endpoints in the hot path.
    """
    def __init__(self):
        self.lock = threading.Lock()
        self.provider_by_proxy = {}
        self.webshare_account_by_proxy = {}
        self.proxy_sets = {'proxyscrape_premium': set(), 'webshare': set()}
        self.last_refresh = {'proxyscrape_premium': 0.0, 'webshare': 0.0}
        self.ps_subaccount_id = PROXYSCRAPE_PREMIUM_SUBACCOUNT_ID
        self.provider_circuit_until = {'proxyscrape_premium': 0.0}
        self.provider_recent = {'proxyscrape_premium': deque(maxlen=32)}
        self.premium_circuit_opened_at = 0.0
        self.premium_half_open_last = 0.0
        # V6.25: Webshare accounts may expose the SAME exit IP pool. Additional keys then
        # increase bandwidth allowance but not IP diversity. A shared circuit prevents key #2
        # from immediately retrying the same blocked provider pool after key #1 hits a 403 wall.
        self.webshare_pool_recent = deque(maxlen=12)
        self.webshare_pool_circuit_until = 0.0
        self.webshare_pool_circuit_opened_at = 0.0
        self.webshare_pool_half_open_last = 0.0

        self.webshare_accounts = {}
        for idx, key in enumerate(WEBSHARE_API_KEYS, 1):
            self.webshare_accounts[idx] = {
                'key': key,
                'proxies': set(),
                'last_proxy_refresh': 0.0,
                'last_stats_refresh': 0.0,
                'plan_id': None,
                'billing_start': None,
                'billing_end': None,
                'bandwidth_limit_gb': None,
                'bandwidth_used_bytes': None,
                'bandwidth_projected_bytes': None,
                'circuit_until': 0.0,
                'recent': deque(maxlen=10),
                'api_disabled_until': 0.0,
                'app_body_bytes': 0,
                # Per-account eBay evidence: useful when multiple Webshare billing
                # accounts are connected, even when they share the same exit IPs. Sets stay tiny (10 proxies/account) and let
                # logs show which account/IP pool actually helps instead of only aggregate stats.
                'ebay_requests': 0,
                'ebay_success': 0,
                'discovery_requests': 0,
                'discovery_success': 0,
                'blocked': 0,
                'winners': 0,
                'unique_discovery': set(),
                'unique_success': set(),
            }

        self.stats = {
            name: {
                'requests': 0, 'success': 0, 'blocked': 0, 'rate_limited': 0,
                'proxy_timeout': 0, 'proxy_error': 0, 'proxy_rejected': 0,
                'proxy_ssl': 0, 'http_error': 0, 'bytes': 0,
                'discovery_requests': 0, 'discovery_success': 0,
                'fixed_requests': 0, 'fixed_success': 0,
                'recovery_requests': 0, 'recovery_success': 0,
                'winners': 0, 'winner_seconds_sum': 0.0, 'winner_attempts_sum': 0,
            }
            for name in ('proxyscrape_premium', 'webshare', 'free')
        }
        self.discovery_proxy_seen = {
            name: set() for name in ('proxyscrape_premium', 'webshare', 'free')
        }
        self.success_proxy_seen = {
            name: set() for name in ('proxyscrape_premium', 'webshare', 'free')
        }
        self.last_stats_log = 0.0
        self._webshare_warned = False

    @staticmethod
    def _bounded_seen_add(bucket, proxy):
        # Diagnostic uniqueness must never become an unbounded archive of public
        # proxy credential strings. A stable hash is enough for approximate unique counts.
        if len(bucket) >= PROVIDER_UNIQUE_STATS_CAP:
            return
        try:
            bucket.add(hash(proxy))
        except Exception:
            pass

    def _webshare_unique_hosts_locked(self):
        hosts = set()
        for state in self.webshare_accounts.values():
            for proxy in state.get('proxies', ()):
                host = _proxy_host(proxy)
                if host:
                    hosts.add(host)
        return hosts

    def _webshare_pool_is_shared_locked(self):
        active_sets = []
        for state in self.webshare_accounts.values():
            hosts = {_proxy_host(p) for p in state.get('proxies', ()) if _proxy_host(p)}
            if hosts:
                active_sets.append(hosts)
        if len(active_sets) <= 1:
            return True
        base = active_sets[0]
        for other in active_sets[1:]:
            union = base | other
            if not union:
                continue
            if len(base & other) / len(union) < 0.80:
                return False
        return True

    def webshare_unique_host_count(self):
        with self.lock:
            return len(self._webshare_unique_hosts_locked())

    def source(self, proxy):
        with self.lock:
            return self.provider_by_proxy.get(proxy, 'free')

    def source_fast(self, proxy):
        return self.provider_by_proxy.get(proxy, 'free')

    def source_label_fast(self, proxy):
        src = self.provider_by_proxy.get(proxy, 'free')
        if src == 'webshare':
            idx = self.webshare_account_by_proxy.get(proxy)
            return f'webshare#{idx}' if idx else 'webshare'
        return src

    def managed_proxy_available(self, proxy):
        """True for active managed endpoints only; historical source labels are not enough."""
        src = self.provider_by_proxy.get(proxy, 'free')
        if src == 'free':
            return True
        now = time.time()
        with self.lock:
            if src == 'proxyscrape_premium':
                return (
                    self.provider_circuit_until['proxyscrape_premium'] <= now
                    and proxy in self.proxy_sets['proxyscrape_premium']
                )
            if src == 'webshare':
                if self.webshare_pool_circuit_until > now:
                    return False
                idx = self.webshare_account_by_proxy.get(proxy)
                state = self.webshare_accounts.get(idx)
                if not state:
                    return False
                ratio = self._webshare_usage_ratio_locked(idx)
                if state.get('api_disabled_until', 0.0) > now or state.get('circuit_until', 0.0) > now:
                    return False
                if ratio is not None and ratio >= WEBSHARE_USAGE_HARD_LIMIT:
                    return False
                return proxy in state.get('proxies', set())
        return False

    def _set_provider_snapshot(self, name, proxies):
        proxies = set(proxies or ())
        with self.lock:
            previous = set(self.proxy_sets.get(name, ()))
            # Remove stale source labels when a managed snapshot shrinks or expires.
            # Otherwise an endpoint that later reappears in a free feed can remain
            # misclassified as an unavailable managed provider.
            for p in previous - proxies:
                if self.provider_by_proxy.get(p) == name:
                    self.provider_by_proxy.pop(p, None)
            self.proxy_sets[name] = proxies
            for p in proxies:
                self.provider_by_proxy[p] = name
            self.last_refresh[name] = time.time()
        return len(proxies)

    def _disable_premium_temporarily(self, seconds=1800, clear_subaccount=False):
        """Drop stale Premium endpoints immediately while keeping all other workers usable."""
        now = time.time()
        with self.lock:
            previous = set(self.proxy_sets.get('proxyscrape_premium', ()))
            for p in previous:
                if self.provider_by_proxy.get(p) == 'proxyscrape_premium':
                    self.provider_by_proxy.pop(p, None)
            self.proxy_sets['proxyscrape_premium'] = set()
            self.provider_circuit_until['proxyscrape_premium'] = max(
                self.provider_circuit_until.get('proxyscrape_premium', 0.0),
                now + max(60.0, float(seconds)),
            )
            self.premium_circuit_opened_at = now
            self.premium_half_open_last = 0.0
            self.last_refresh['proxyscrape_premium'] = now
            if clear_subaccount:
                self.ps_subaccount_id = ''

    def _rebuild_webshare_snapshot_locked(self):
        combined = set()
        for idx, state in self.webshare_accounts.items():
            for proxy in state['proxies']:
                combined.add(proxy)
                self.provider_by_proxy[proxy] = 'webshare'
                self.webshare_account_by_proxy[proxy] = idx
        self.proxy_sets['webshare'] = combined
        self.last_refresh['webshare'] = time.time()
        return len(combined)

    def _webshare_usage_ratio_locked(self, idx):
        state = self.webshare_accounts.get(idx)
        if not state:
            return None
        limit_gb = state.get('bandwidth_limit_gb')
        used = state.get('bandwidth_used_bytes')
        if limit_gb is None or used is None:
            return None
        try:
            limit_gb = float(limit_gb)
            if limit_gb <= 0:
                return 0.0  # Webshare uses 0 for unlimited on paid plans.
            limit_bytes = limit_gb * 1_000_000_000  # Webshare dashboard/API reports decimal GB
            return min(10.0, max(0.0, float(used) / limit_bytes))
        except Exception:
            return None

    def _webshare_projected_ratio_locked(self, idx):
        state = self.webshare_accounts.get(idx)
        if not state:
            return None
        limit_gb = state.get('bandwidth_limit_gb')
        projected = state.get('bandwidth_projected_bytes')
        if limit_gb is None or projected is None:
            return None
        try:
            limit_gb = float(limit_gb)
            if limit_gb <= 0:
                return 0.0
            return max(0.0, float(projected) / (limit_gb * 1_000_000_000))  # decimal GB
        except Exception:
            return None

    def webshare_usage_ratio_for_proxy(self, proxy):
        with self.lock:
            idx = self.webshare_account_by_proxy.get(proxy)
            return self._webshare_usage_ratio_locked(idx) if idx else None

    def webshare_pressure_ratio_for_proxy(self, proxy):
        """Bandwidth pressure for bridge timing only.

        Hard-disable always uses REAL consumed bytes, never a projection. For deciding
        whether a working Webshare should hand off sooner we also look at Webshare's
        projected end-of-cycle usage: a bursty account can then be protected before
        it actually reaches 85-98% of the monthly allowance.
        """
        with self.lock:
            idx = self.webshare_account_by_proxy.get(proxy)
            if not idx:
                return None
            actual = self._webshare_usage_ratio_locked(idx)
            projected = self._webshare_projected_ratio_locked(idx)
        values = [x for x in (actual, projected) if x is not None]
        return max(values) if values else None

    def webshare_handoff_after_for_proxy(self, proxy):
        pressure = self.webshare_pressure_ratio_for_proxy(proxy)
        if pressure is not None and pressure >= WEBSHARE_USAGE_SOFT_LIMIT:
            return WEBSHARE_HANDOFF_HIGH_USAGE_AFTER
        return WEBSHARE_HANDOFF_AFTER

    def _fetch_webshare_proxy_list(self, idx, state):
        key = state['key']
        url = 'https://proxy.webshare.io/api/v2/proxy/list/'
        headers = {'Authorization': f'Token {key}'}
        params = {'mode': 'direct', 'valid': 'true', 'page': 1, 'page_size': 100}
        out = []
        try:
            while url and len(out) < 500:
                resp = requests.get(
                    url, headers=headers, params=params if '?' not in url else None,
                    timeout=PROVIDER_API_TIMEOUT,
                )
                if resp.status_code in (401, 403):
                    logging.warning(f"Webshare#{idx} API: HTTP {resp.status_code}; account paused for 30 min")
                    with self.lock:
                        state['api_disabled_until'] = time.time() + 1800
                        state['proxies'] = set()
                        self._rebuild_webshare_snapshot_locked()
                    return None
                if resp.status_code == 429:
                    logging.warning(f"Webshare#{idx} API: HTTP 429; keep previous snapshot")
                    return None
                if resp.status_code != 200:
                    logging.warning(f"Webshare#{idx} API: HTTP {resp.status_code}; keep previous snapshot")
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
                        out.append(
                            f"http://{quote(user, safe='')}:{quote(password, safe='')}@{host}:{int(port)}"
                        )
                url = data.get('next')
                params = None
            return list(dict.fromkeys(out))
        except Exception as e:
            logging.warning(f"Webshare#{idx} API недоступен: {e}; keep previous snapshot")
            return None

    def _refresh_webshare_usage(self, idx, state, force=False):
        now = time.time()
        if not force and (now - state.get('last_stats_refresh', 0.0)) < WEBSHARE_STATS_REFRESH:
            return
        key = state['key']
        headers = {'Authorization': f'Token {key}'}
        try:
            sub = requests.get(
                'https://proxy.webshare.io/api/v2/subscription/',
                headers=headers, timeout=PROVIDER_API_TIMEOUT,
            )
            if sub.status_code != 200:
                return
            sub_data = sub.json() or {}
            plan_id = sub_data.get('plan')
            billing_start = sub_data.get('start_date')
            billing_end = sub_data.get('end_date')

            bandwidth_limit_gb = None
            if plan_id:
                plan = requests.get(
                    f'https://proxy.webshare.io/api/v2/subscription/plan/{plan_id}/',
                    headers=headers, timeout=PROVIDER_API_TIMEOUT,
                )
                if plan.status_code == 200:
                    bandwidth_limit_gb = (plan.json() or {}).get('bandwidth_limit')

            stats_params = {}
            if plan_id:
                stats_params['plan_id'] = plan_id
            if billing_start:
                stats_params['timestamp__gte'] = billing_start
            # Request through the current billing end (never beyond it). Webshare's
            # aggregate endpoint then returns BOTH real bandwidth_total and the provider's
            # end-of-cycle bandwidth_projected. With only 'now' as LTE the projection is
            # not useful for proactive traffic protection.
            now_dt = datetime.now(timezone.utc)
            lte_dt = now_dt
            if billing_end:
                try:
                    end_dt = datetime.fromisoformat(str(billing_end).replace('Z', '+00:00'))
                    if end_dt.tzinfo is None:
                        end_dt = end_dt.replace(tzinfo=timezone.utc)
                    lte_dt = end_dt
                except Exception:
                    pass
            stats_params['timestamp__lte'] = lte_dt.isoformat()
            stats = requests.get(
                'https://proxy.webshare.io/api/v2/stats/aggregate/',
                headers=headers, params=stats_params, timeout=PROVIDER_API_TIMEOUT,
            )
            bandwidth_used = None
            bandwidth_projected = None
            if stats.status_code == 200:
                stat_data = stats.json() or {}
                bandwidth_used = stat_data.get('bandwidth_total')
                bandwidth_projected = stat_data.get('bandwidth_projected')

            with self.lock:
                state['plan_id'] = plan_id
                state['billing_start'] = billing_start
                state['billing_end'] = billing_end
                if bandwidth_limit_gb is not None:
                    state['bandwidth_limit_gb'] = bandwidth_limit_gb
                if bandwidth_used is not None:
                    state['bandwidth_used_bytes'] = int(bandwidth_used)
                if bandwidth_projected is not None:
                    state['bandwidth_projected_bytes'] = int(bandwidth_projected)
                state['last_stats_refresh'] = now
        except Exception as e:
            logging.debug(f"Webshare#{idx} stats refresh skipped: {e}")

    def _refresh_webshare_all(self, force=False, include_stats=True):
        """Refresh Webshare billing accounts in parallel.

        Failover needs the proxy LIST, not three billing/statistics HTTP calls. Discovery
        therefore calls this with include_stats=False; the background reserve worker keeps
        real usage fresh while a fixed proxy is online. With multiple accounts, list refreshes
        run concurrently so one slow API does not serialize 4 x provider timeouts.
        """
        if not self.webshare_accounts:
            return
        now = time.time()
        jobs = []
        for idx, state in list(self.webshare_accounts.items()):
            if state.get('api_disabled_until', 0.0) > now:
                continue
            due = force or (now - state.get('last_proxy_refresh', 0.0)) >= WEBSHARE_REFRESH
            stats_due = bool(
                include_stats
                and (force or (now - state.get('last_stats_refresh', 0.0)) >= WEBSHARE_STATS_REFRESH)
            )
            if due or stats_due:
                jobs.append((idx, state, due, stats_due))
        if not jobs:
            return

        def _one(job):
            idx, state, due, stats_due = job
            rows = self._fetch_webshare_proxy_list(idx, state) if due else '__not_due__'
            if stats_due:
                self._refresh_webshare_usage(idx, state, force=force)
            return idx, state, due, stats_due, rows

        results = []
        max_workers = min(4, len(jobs))
        if max_workers <= 1:
            for job in jobs:
                results.append(_one(job))
        else:
            with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix='webshare-api') as executor:
                futures = [executor.submit(_one, job) for job in jobs]
                for future in futures:
                    try:
                        results.append(future.result())
                    except Exception as e:
                        logging.warning(f"Webshare API refresh worker skipped: {e}")

        did_refresh = False
        with self.lock:
            for idx, state, due, stats_due, rows in results:
                if due and rows != '__not_due__' and rows is not None:
                    state['proxies'] = set(rows)
                    state['last_proxy_refresh'] = now
                    did_refresh = True
                if stats_due:
                    did_refresh = True
            if did_refresh:
                self._rebuild_webshare_snapshot_locked()
                total = len(self.proxy_sets['webshare'])
                pieces = []
                for idx, state in self.webshare_accounts.items():
                    ratio = self._webshare_usage_ratio_locked(idx)
                    projected = self._webshare_projected_ratio_locked(idx)
                    count = len(state['proxies'])
                    if ratio is None:
                        pieces.append(f"#{idx}:{count} proxy")
                    else:
                        tail = f"/{projected*100:.0f}% projected" if projected is not None else ''
                        pieces.append(f"#{idx}:{count} proxy/{ratio*100:.1f}% used{tail}")
            else:
                total = 0
                pieces = []
        if total:
            with self.lock:
                unique_hosts = len(self._webshare_unique_hosts_locked())
                shared_pool = self._webshare_pool_is_shared_locked()
            logging.info(
                f"🟩 Webshare: {unique_hosts} unique exit IP(s), {total} credential endpoint(s) "
                f"across {len(self.webshare_accounts)} account(s), "
                f"pool={'shared/overlapping' if shared_pool else 'partly distinct'} "
                f"[{', '.join(pieces)}]"
            )

    def _discover_ps_subaccount(self):
        if self.ps_subaccount_id or not PROXYSCRAPE_PREMIUM_API_KEY:
            return self.ps_subaccount_id
        try:
            resp = requests.get(
                'https://api.proxyscrape.com/v4/account/subaccounts',
                headers={'api-token': PROXYSCRAPE_PREMIUM_API_KEY},
                timeout=PROVIDER_API_TIMEOUT,
            )
            if resp.status_code in (401, 402, 403, 404, 410):
                logging.warning(
                    f"ProxyScrape Premium: subaccount API HTTP {resp.status_code}; "
                    "Premium removed for 30 min, all discovery slots continue with other sources"
                )
                self._disable_premium_temporarily(1800, clear_subaccount=True)
                return ''
            if resp.status_code == 429:
                logging.warning(
                    "ProxyScrape Premium: subaccount API rate-limited; Premium paused 5 min, other sources continue"
                )
                self._disable_premium_temporarily(300, clear_subaccount=False)
                return ''
            if resp.status_code != 200:
                logging.warning(
                    f"ProxyScrape Premium: subaccount auto-discovery HTTP {resp.status_code}; "
                    "keep old state and continue with other sources"
                )
                return ''
            data = resp.json().get('data', {}).get('subaccounts', [])
            rows = [r for r in data if str(r.get('AccountType', '')).lower() == 'datacenter_shared']
            if not rows:
                logging.warning(
                    'ProxyScrape Premium: datacenter_shared subaccount not found; '
                    'Premium removed for 30 min, other sources continue'
                )
                self._disable_premium_temporarily(1800, clear_subaccount=True)
                return ''
            rows.sort(key=lambda r: (
                'premium' in str(r.get('label', '')).lower() or 'trial' in str(r.get('label', '')).lower(),
                not bool(r.get('is_hidden')),
            ), reverse=True)
            self.ps_subaccount_id = str(rows[0].get('AccountID') or '').strip()
            return self.ps_subaccount_id
        except Exception as e:
            logging.warning(f"ProxyScrape Premium: subaccount auto-discovery error: {e}; other sources continue")
            return ''

    def _fetch_proxyscrape_premium(self):
        if not PROXYSCRAPE_PREMIUM_API_KEY:
            return []
        now = time.time()
        with self.lock:
            if self.provider_circuit_until['proxyscrape_premium'] > now:
                return None
        sid = self._discover_ps_subaccount()
        if not sid:
            return None
        url = f'https://api.proxyscrape.com/v4/account/{sid}/datacenter_shared/proxy-list'
        params = {
            'type': 'displayproxies', 'protocol': 'http', 'format': 'credentials',
            'credential_format': 3, 'status': 'online', 'limit': 500,
        }
        try:
            resp = requests.get(
                url, headers={'api-token': PROXYSCRAPE_PREMIUM_API_KEY},
                params=params, timeout=PROVIDER_API_TIMEOUT,
            )
            if resp.status_code in (401, 402, 403, 404, 410):
                logging.warning(
                    f"ProxyScrape Premium API HTTP {resp.status_code}; Premium removed for 30 min, "
                    "all discovery slots continue with Proxio/free/Webshare"
                )
                self._disable_premium_temporarily(1800, clear_subaccount=True)
                return []
            if resp.status_code == 429:
                with self.lock:
                    self.provider_circuit_until['proxyscrape_premium'] = time.time() + 300
                return None
            if resp.status_code != 200:
                msg = (resp.text or '')[:180].replace('\n', ' ')
                logging.warning(f"ProxyScrape Premium API: HTTP {resp.status_code}: {msg}; keep old snapshot")
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
            logging.warning(f"ProxyScrape Premium API unavailable: {e}; keep old snapshot")
            return None

    def refresh_all(self, force=False, include_stats=True):
        now = time.time()
        with self.lock:
            ps_due = force or (now - self.last_refresh['proxyscrape_premium']) >= PROXYSCRAPE_PREMIUM_REFRESH
        if ps_due and PROXYSCRAPE_PREMIUM_API_KEY:
            rows = self._fetch_proxyscrape_premium()
            if rows is not None:
                n = self._set_provider_snapshot('proxyscrape_premium', rows)
                if n:
                    logging.info(f"🔷 ProxyScrape Premium: {n} online HTTP proxy loaded")
        self._refresh_webshare_all(force=force, include_stats=include_stats)

    def _webshare_candidates_locked(self):
        now = time.time()
        if self.webshare_pool_circuit_until > now:
            return []
        accounts = []
        for idx, state in self.webshare_accounts.items():
            if state.get('api_disabled_until', 0.0) > now or state.get('circuit_until', 0.0) > now:
                continue
            ratio = self._webshare_usage_ratio_locked(idx)
            if ratio is not None and ratio >= WEBSHARE_USAGE_HARD_LIMIT:
                continue
            if state['proxies']:
                accounts.append((idx, ratio if ratio is not None else 0.50, state))
        accounts.sort(key=lambda row: (row[1], row[0]))

        # Interleave accounts instead of exhausting #1 before #2/#3/#4.
        buckets = []
        for idx, ratio, state in accounts:
            rows = list(state['proxies'])
            random.shuffle(rows)
            buckets.append((idx, ratio, rows))
        out = []
        while buckets:
            next_buckets = []
            for idx, ratio, rows in buckets:
                if rows:
                    out.append(rows.pop())
                if rows:
                    next_buckets.append((idx, ratio, rows))
            buckets = next_buckets
        return out

    def premium_half_open_snapshot(self):
        """Return Premium endpoints only for one controlled half-open recovery probe."""
        now = time.time()
        with self.lock:
            until = self.provider_circuit_until.get('proxyscrape_premium', 0.0)
            if until <= now:
                return []
            opened = self.premium_circuit_opened_at or (until - MANAGED_PROVIDER_CIRCUIT_SECONDS)
            if (now - opened) < PREMIUM_CIRCUIT_HALF_OPEN_AFTER:
                return []
            if self.premium_half_open_last and (now - self.premium_half_open_last) < PREMIUM_CIRCUIT_HALF_OPEN_INTERVAL:
                return []
            rows = list(self.proxy_sets.get('proxyscrape_premium', ()))
            random.shuffle(rows)
            return rows

    def claim_premium_half_open(self):
        """Atomically reserve the current Premium half-open opportunity."""
        now = time.time()
        with self.lock:
            until = self.provider_circuit_until.get('proxyscrape_premium', 0.0)
            if until <= now:
                return False
            opened = self.premium_circuit_opened_at or (until - MANAGED_PROVIDER_CIRCUIT_SECONDS)
            if (now - opened) < PREMIUM_CIRCUIT_HALF_OPEN_AFTER:
                return False
            if self.premium_half_open_last and (now - self.premium_half_open_last) < PREMIUM_CIRCUIT_HALF_OPEN_INTERVAL:
                return False
            self.premium_half_open_last = now
            return True

    def _close_premium_circuit_locked(self):
        now = time.time()
        if self.provider_circuit_until.get('proxyscrape_premium', 0.0) <= now:
            return False
        self.provider_circuit_until['proxyscrape_premium'] = 0.0
        self.premium_circuit_opened_at = 0.0
        self.premium_half_open_last = 0.0
        dq = self.provider_recent.get('proxyscrape_premium')
        if dq is not None:
            dq.clear()
            dq.append('success')
        return True

    def webshare_half_open_snapshot(self):
        """Return Webshare endpoints while the shared circuit is open, without consuming a probe slot.

        This is used only by the main discovery half-open path. Normal candidate APIs still
        respect the shared circuit and return no Webshare endpoints during it.
        """
        now = time.time()
        with self.lock:
            if self.webshare_pool_circuit_until <= now:
                return []
            opened = self.webshare_pool_circuit_opened_at or (
                self.webshare_pool_circuit_until - MANAGED_PROVIDER_CIRCUIT_SECONDS
            )
            if (now - opened) < WEBSHARE_CIRCUIT_HALF_OPEN_AFTER:
                return []
            if self.webshare_pool_half_open_last and (
                now - self.webshare_pool_half_open_last
            ) < WEBSHARE_CIRCUIT_HALF_OPEN_INTERVAL:
                return []

            accounts = []
            for idx, state in self.webshare_accounts.items():
                if state.get('api_disabled_until', 0.0) > now or state.get('circuit_until', 0.0) > now:
                    continue
                ratio = self._webshare_usage_ratio_locked(idx)
                if ratio is not None and ratio >= WEBSHARE_USAGE_HARD_LIMIT:
                    continue
                if state.get('proxies'):
                    accounts.append((idx, ratio if ratio is not None else 0.50, list(state['proxies'])))
            accounts.sort(key=lambda row: (row[1], row[0]))

            out = []
            for idx, _ratio, rows in accounts:
                random.shuffle(rows)
                out.extend(rows)
            return out

    def claim_webshare_half_open(self):
        """Atomically reserve the current half-open opportunity for one discovery probe."""
        now = time.time()
        with self.lock:
            if self.webshare_pool_circuit_until <= now:
                return False
            opened = self.webshare_pool_circuit_opened_at or (
                self.webshare_pool_circuit_until - MANAGED_PROVIDER_CIRCUIT_SECONDS
            )
            if (now - opened) < WEBSHARE_CIRCUIT_HALF_OPEN_AFTER:
                return False
            if self.webshare_pool_half_open_last and (
                now - self.webshare_pool_half_open_last
            ) < WEBSHARE_CIRCUIT_HALF_OPEN_INTERVAL:
                return False
            self.webshare_pool_half_open_last = now
            return True

    def _close_webshare_shared_circuit_locked(self):
        now = time.time()
        if self.webshare_pool_circuit_until <= now:
            return False
        self.webshare_pool_circuit_until = 0.0
        self.webshare_pool_circuit_opened_at = 0.0
        self.webshare_pool_half_open_last = 0.0
        self.webshare_pool_recent.clear()
        self.webshare_pool_recent.append('success')
        return True

    def candidates(self, include_webshare=False):
        now = time.time()
        with self.lock:
            premium = []
            if self.provider_circuit_until['proxyscrape_premium'] <= now:
                premium = list(self.proxy_sets['proxyscrape_premium'])
            webshare = self._webshare_candidates_locked() if include_webshare else []
        return premium + webshare

    def has_usable_premium(self):
        """True only when Premium can fill a discovery slot right now.

        Expired/unauthorized Premium clears its snapshot and opens a circuit, so callers
        immediately reassign the same bounded worker slot to Proxio/FREE/Webshare.
        """
        now = time.time()
        with self.lock:
            return bool(
                self.provider_circuit_until.get('proxyscrape_premium', 0.0) <= now
                and self.proxy_sets.get('proxyscrape_premium')
            )

    def has_usable_webshare(self):
        with self.lock:
            return bool(self._webshare_candidates_locked())

    def usable_webshare_account_count(self):
        """Count usable billing accounts, NOT unique exit-IP pools.

        V6.25 knows several Webshare keys may expose the same 10 exits. Extra accounts
        increase traffic budget only; host-level cooldown and the shared pool circuit
        still prevent duplicate-IP retries from masquerading as diversity.
        """
        now = time.time()
        with self.lock:
            # A shared/overlapping Webshare pool circuit means the exits themselves are
            # temporarily bad for eBay. Multiple API keys must not be mistaken for
            # additional usable providers during that period.
            if self.webshare_pool_circuit_until > now:
                return 0
            count = 0
            for idx, state in self.webshare_accounts.items():
                if state.get('api_disabled_until', 0.0) > now or state.get('circuit_until', 0.0) > now:
                    continue
                ratio = self._webshare_usage_ratio_locked(idx)
                if ratio is not None and ratio >= WEBSHARE_USAGE_HARD_LIMIT:
                    continue
                if state.get('proxies'):
                    count += 1
            return count

    def webshare_account_index_fast(self, proxy):
        return self.webshare_account_by_proxy.get(proxy)

    def webshare_balance_bonus(self, proxy):
        with self.lock:
            idx = self.webshare_account_by_proxy.get(proxy)
            ratio = self._webshare_usage_ratio_locked(idx) if idx else None
        if ratio is None:
            return 0.0
        # Lower-used accounts get a small deterministic preference.
        return max(-50.0, min(35.0, (0.80 - ratio) * 50.0))

    def _snapshot_stats_locked(self):
        snapshot = {k: dict(v) for k, v in self.stats.items()}
        for name in snapshot:
            snapshot[name]['unique_discovery'] = len(self.discovery_proxy_seen[name])
            snapshot[name]['unique_success'] = len(self.success_proxy_seen[name])
        ws_accounts = []
        for idx, state in self.webshare_accounts.items():
            ratio = self._webshare_usage_ratio_locked(idx)
            ws_accounts.append({
                'idx': idx,
                'proxy_count': len(state['proxies']),
                'ratio': ratio,
                'projected_ratio': self._webshare_projected_ratio_locked(idx),
                'used': state.get('bandwidth_used_bytes'),
                'limit_gb': state.get('bandwidth_limit_gb'),
                'ebay_requests': state.get('ebay_requests', 0),
                'ebay_success': state.get('ebay_success', 0),
                'discovery_requests': state.get('discovery_requests', 0),
                'discovery_success': state.get('discovery_success', 0),
                'blocked': state.get('blocked', 0),
                'winners': state.get('winners', 0),
                'unique_discovery': len(state.get('unique_discovery') or ()),
                'unique_success': len(state.get('unique_success') or ()),
            })
        return snapshot, ws_accounts

    def _format_stats(self, snapshot, ws_accounts=None):
        bits = []
        for name in ('proxyscrape_premium', 'webshare', 'free'):
            st = snapshot[name]
            d = st['discovery_requests']
            ds = st['discovery_success']
            unique_d = st.get('unique_discovery', 0)
            unique_s = st.get('unique_success', 0)
            endpoint_rate = (100.0 * unique_s / unique_d) if unique_d else 0.0
            fixed = st['fixed_requests'] + st['recovery_requests']
            fixed_ok = st['fixed_success'] + st['recovery_success']
            winners = st['winners']
            avg_find = (st['winner_seconds_sum'] / winners) if winners else 0.0
            bits.append(
                f"{name}: eBay={st['requests']}, discovery={d}/{ds}, "
                f"unique={unique_d}/{unique_s} ({endpoint_rate:.1f}% endpoints), "
                f"fixed={fixed}/{fixed_ok}, winners={winners}, avg_find={avg_find:.1f}s, "
                f"403={st['blocked']}, timeout={st['proxy_timeout']}, "
                f"reject={st['proxy_rejected']}, ssl={st['proxy_ssl']}, "
                f"body≈{st['bytes']/1024/1024:.1f}MB"
            )
        if ws_accounts:
            acct_bits = []
            for row in ws_accounts:
                if row['ratio'] is None:
                    usage = 'usage?n/a'
                else:
                    usage = f"used={row['ratio']*100:.1f}%"
                if row.get('projected_ratio') is not None:
                    usage += f"/projected={row['projected_ratio']*100:.0f}%"
                ud = row.get('unique_discovery', 0)
                us = row.get('unique_success', 0)
                q = (100.0 * us / ud) if ud else 0.0
                acct_bits.append(
                    f"ws#{row['idx']}:{row['proxy_count']}proxy/{usage}/"
                    f"unique={ud}/{us}({q:.0f}%)/"
                    f"eBay={row.get('ebay_requests', 0)}/{row.get('ebay_success', 0)}/"
                    f"winners={row.get('winners', 0)}/403={row.get('blocked', 0)}"
                )
            bits.append('Webshare accounts=' + ','.join(acct_bits))
        return '📊 Provider stats | ' + ' | '.join(bits)

    def maybe_log_stats(self, force=False):
        now = time.time()
        with self.lock:
            if not force and (now - self.last_stats_log) < PROVIDER_STATS_INTERVAL:
                return
            self.last_stats_log = now
            snapshot, ws_accounts = self._snapshot_stats_locked()
        logging.info(self._format_stats(snapshot, ws_accounts))

    def premium_block_streak(self):
        """Consecutive discovery 403s at the tail of the Premium provider history."""
        with self.lock:
            dq = self.provider_recent.get('proxyscrape_premium') or ()
            streak = 0
            for result in reversed(dq):
                if result != 'blocked':
                    break
                streak += 1
            return streak

    def _update_managed_circuit_locked(self, proxy, result, request_kind):
        if request_kind != 'discovery':
            return None
        now = time.time()
        src = self.provider_by_proxy.get(proxy, 'free')
        if src == 'proxyscrape_premium':
            dq = self.provider_recent['proxyscrape_premium']
            dq.append(result)
            # Keep a late-good Premium chance, but do not burn most of the 100-IP pool
            # during one correlated eBay block wall. Candidate batching starts throttling
            # after PREMIUM_BLOCK_THROTTLE_STREAK; a longer all-403 tail opens the circuit.
            if len(dq) >= PREMIUM_BLOCK_CIRCUIT_STREAK and all(
                r == 'blocked' for r in list(dq)[-PREMIUM_BLOCK_CIRCUIT_STREAK:]
            ):
                until = now + MANAGED_PROVIDER_CIRCUIT_SECONDS
                # Open once. Late in-flight 403s from the same wave must not push the
                # recovery window forward again. Controlled half-open probes handle recovery.
                if self.provider_circuit_until['proxyscrape_premium'] <= now:
                    self.provider_circuit_until['proxyscrape_premium'] = until
                    self.premium_circuit_opened_at = now
                    self.premium_half_open_last = 0.0
                    return 'ProxyScrape Premium'
        elif src == 'webshare':
            idx = self.webshare_account_by_proxy.get(proxy)
            state = self.webshare_accounts.get(idx)
            if state is not None:
                dq = state['recent']
                dq.append(result)
                self.webshare_pool_recent.append(result)
                # Accounts often expose the same exit IPs. If snapshots overlap heavily,
                # opening only an account-local circuit would simply burn the second key
                # against the same blocked exits. Use one shared circuit for the common pool.
                shared_pool = self._webshare_pool_is_shared_locked()
                target_dq = self.webshare_pool_recent if shared_pool else dq
                if len(target_dq) >= WEBSHARE_BLOCK_CIRCUIT_STREAK and all(
                    r == 'blocked' for r in list(target_dq)[-WEBSHARE_BLOCK_CIRCUIT_STREAK:]
                ):
                    until = now + MANAGED_PROVIDER_CIRCUIT_SECONDS
                    if shared_pool:
                        # Do not keep extending an already-open shared circuit because
                        # of late in-flight 403s from the same wave. Open it once, then
                        # let controlled half-open probes test whether the pool recovered.
                        if self.webshare_pool_circuit_until <= now:
                            self.webshare_pool_circuit_until = until
                            self.webshare_pool_circuit_opened_at = now
                            self.webshare_pool_half_open_last = 0.0
                            return 'Webshare shared exit pool'
                    elif state['circuit_until'] < until - 1:
                        state['circuit_until'] = until
                        return f'Webshare#{idx}'
        return None

    def record_result(self, proxy, result, body_bytes=0, request_kind='unknown'):
        src = self.source_fast(proxy)
        if src not in self.stats:
            src = 'free'
        circuit_label = None
        webshare_circuit_closed = False
        premium_circuit_closed = False
        with self.lock:
            st = self.stats[src]
            st['requests'] += 1
            if result in st:
                st[result] += 1
            st['bytes'] += max(0, int(body_bytes or 0))

            if request_kind == 'discovery':
                st['discovery_requests'] += 1
                self._bounded_seen_add(self.discovery_proxy_seen[src], proxy)
                if result == 'success':
                    st['discovery_success'] += 1
            elif request_kind == 'fixed':
                st['fixed_requests'] += 1
                if result == 'success':
                    st['fixed_success'] += 1
            elif request_kind == 'recovery':
                st['recovery_requests'] += 1
                if result == 'success':
                    st['recovery_success'] += 1

            if result == 'success':
                self._bounded_seen_add(self.success_proxy_seen[src], proxy)
                if src == 'proxyscrape_premium':
                    premium_circuit_closed = self._close_premium_circuit_locked()

            if src == 'webshare':
                idx = self.webshare_account_by_proxy.get(proxy)
                state = self.webshare_accounts.get(idx)
                if state is not None:
                    state['app_body_bytes'] += max(0, int(body_bytes or 0))
                    state['ebay_requests'] += 1
                    if result == 'success':
                        state['ebay_success'] += 1
                    if result == 'blocked':
                        state['blocked'] += 1
                    if request_kind == 'discovery':
                        state['discovery_requests'] += 1
                        self._bounded_seen_add(state['unique_discovery'], proxy)
                        if result == 'success':
                            state['discovery_success'] += 1
                    if result == 'success':
                        self._bounded_seen_add(state['unique_success'], proxy)
                        # A successful controlled half-open (or any discovery success that
                        # somehow arrives while the shared circuit is still active) proves
                        # the exit pool recovered. Re-open Webshare immediately instead of
                        # waiting for the original 5-minute timer.
                        webshare_circuit_closed = self._close_webshare_shared_circuit_locked()

            circuit_label = self._update_managed_circuit_locked(proxy, result, request_kind)

        if webshare_circuit_closed:
            logging.info("🟢 Webshare shared exit pool recovered; circuit closed early after HTTP 200")
        if premium_circuit_closed:
            logging.info("🟢 ProxyScrape Premium recovered; circuit closed early after HTTP 200")
        if circuit_label:
            logging.warning(
                f"🛑 {circuit_label}: temporary 403 circuit opened; "
                "other providers continue immediately"
            )
        self.maybe_log_stats(force=False)

    def record_winner(self, proxy, elapsed_seconds, attempts):
        src = self.source_fast(proxy)
        if src not in self.stats:
            src = 'free'
        with self.lock:
            st = self.stats[src]
            st['winners'] += 1
            st['winner_seconds_sum'] += max(0.0, float(elapsed_seconds or 0.0))
            st['winner_attempts_sum'] += max(0, int(attempts or 0))
            if src == 'webshare':
                idx = self.webshare_account_by_proxy.get(proxy)
                state = self.webshare_accounts.get(idx)
                if state is not None:
                    state['winners'] += 1
        self.maybe_log_stats(force=True)

    def webshare_estimated_mb(self):
        with self.lock:
            return self.stats['webshare']['bytes'] / 1024 / 1024

    def maybe_warn_webshare_usage(self):
        # Keep the old HTML-body warning as a fallback, but real billing ratios are logged
        # from Webshare's own stats API and are used for balancing.
        mb = self.webshare_estimated_mb()
        if mb >= WEBSHARE_ESTIMATED_MB_WARN and not self._webshare_warned:
            self._webshare_warned = True
            logging.warning(
                f"⚠️ Webshare decompressed HTML body in this process ≈{mb:.1f}MB; "
                "provider billing is tracked separately through Webshare Stats API."
            )


class ExternalFreeSourceManager:
    """Small, quality-filtered HProxy + Databay + Proxio snapshots for discovery.

    HProxy/Databay are unmetered public feeds. Proxio is intentionally different: its API
    is quota-limited, so exactly ONE key is used per due refresh and the returned Elite
    HTTPS snapshot is reused for real probes until the next refresh. None of these feeds is
    merged into the large ProxyScrape list, and they never increase global eBay concurrency.
    """
    SOURCES = ('hproxy', 'databay', 'proxio')

    def __init__(self):
        self.lock = threading.Lock()
        self.refresh_gate = threading.Lock()
        self.snapshots = {name: [] for name in self.SOURCES}
        self.last_refresh = {name: 0.0 for name in self.SOURCES}
        self.credit_source_by_proxy = {}
        self.credit_source_at = {}
        self.turn = 0
        self.preflight_turn = 0
        self.last_stats_log = 0.0

        # Proxio API quota state. Secrets themselves are never logged or persisted.
        utc_day = datetime.now(timezone.utc).date().isoformat()
        self.proxio_key_state = [
            {'day': utc_day, 'calls': 0, 'disabled_until': 0.0}
            for _ in PROXIO_API_KEYS
        ]
        self.proxio_key_turn = 0
        self.proxio_next_retry_at = 0.0
        self.proxio_feed_rank = {}
        self.proxio_last_quota_log = 0.0

        self.stats = {
            name: {
                'discovery': 0, 'discovery_success': 0, 'fixed': 0, 'fixed_success': 0,
                'blocked': 0, 'rate_limited': 0, 'proxy_timeout': 0, 'proxy_error': 0,
                'proxy_rejected': 0, 'proxy_ssl': 0, 'http_error': 0,
                'winners': 0, 'unique_tested': set(), 'unique_success': set(),
            }
            for name in ('proxyscrape', 'hproxy', 'databay', 'proxio')
        }

    @staticmethod
    def _normalize_http_proxy(ip, port):
        try:
            ip = str(ip or '').strip()
            port = int(port)
            if not ip or port < 1 or port > 65535:
                return None
            # HTTPS-capable public lists describe CONNECT capability. curl_cffi connects
            # to the proxy via HTTP and establishes eBay TLS through CONNECT.
            return f"http://{ip}:{port}"
        except Exception:
            return None

    @staticmethod
    def _rows_from_json(data):
        if isinstance(data, list):
            return data
        if not isinstance(data, dict):
            return []
        for key in ('data', 'results', 'proxies', 'items'):
            rows = data.get(key)
            if isinstance(rows, list):
                return rows
            # Be tolerant of APIs that wrap rows as {data:{proxies:[...]}}.
            if isinstance(rows, dict):
                for nested_key in ('results', 'proxies', 'items', 'data'):
                    nested = rows.get(nested_key)
                    if isinstance(nested, list):
                        return nested
        return []

    def _source_enabled(self, source):
        if source == 'hproxy':
            return HPROXY_FREE_ENABLED
        if source == 'databay':
            return DATABAY_FREE_ENABLED
        if source == 'proxio':
            return PROXIO_FREE_ENABLED and bool(PROXIO_API_KEYS)
        return False

    @staticmethod
    def _source_interval(source):
        if source == 'hproxy':
            return HPROXY_FREE_REFRESH
        if source == 'databay':
            return DATABAY_FREE_REFRESH
        return PROXIO_FREE_REFRESH

    def _fetch_hproxy_group(self, anonymity, limit, min_uptime):
        params = {
            'format': 'json', 'protocol': 'https', 'anonymity': anonymity,
            'min_uptime_pct': int(min_uptime), 'max_latency_ms': int(HPROXY_FREE_MAX_LATENCY_MS),
            'sort': 'uptime', 'limit': int(limit),
        }
        resp = requests.get('https://hproxy.com/api/proxy-list', params=params, timeout=EXTERNAL_FREE_API_TIMEOUT)
        if resp.status_code != 200:
            logging.warning(f"HProxy free API HTTP {resp.status_code}; keep previous snapshot")
            return None
        rows = self._rows_from_json(resp.json())
        out = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            proxy = self._normalize_http_proxy(row.get('ip') or row.get('host'), row.get('port'))
            if proxy:
                out.append(proxy)
        return out

    def _fetch_hproxy(self):
        if not HPROXY_FREE_ENABLED:
            return []
        elite = self._fetch_hproxy_group('elite', HPROXY_FREE_ELITE_LIMIT, HPROXY_FREE_ELITE_MIN_UPTIME)
        anon = self._fetch_hproxy_group('anonymous', HPROXY_FREE_ANON_LIMIT, HPROXY_FREE_ANON_MIN_UPTIME)
        if elite is None and anon is None:
            return None
        return list(dict.fromkeys((elite or []) + (anon or [])))

    def _fetch_databay_group(self, anonymity, limit):
        params = {
            'protocol': 'https', 'ssl': 'strict', 'speed': 'fast',
            'anonymity': anonymity, 'format': 'json', 'limit': int(limit), 'page': 1,
        }
        resp = requests.get('https://databay.com/api/v1/proxy-list', params=params, timeout=EXTERNAL_FREE_API_TIMEOUT)
        if resp.status_code != 200:
            logging.warning(f"Databay free API HTTP {resp.status_code}; keep previous snapshot")
            return None
        rows = self._rows_from_json(resp.json())
        out = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            proxy = self._normalize_http_proxy(row.get('ip') or row.get('host'), row.get('port'))
            if proxy:
                out.append(proxy)
        return out

    def _fetch_databay(self):
        if not DATABAY_FREE_ENABLED:
            return []
        elite = self._fetch_databay_group('elite', DATABAY_FREE_ELITE_LIMIT)
        anon = self._fetch_databay_group('anonymous', DATABAY_FREE_ANON_LIMIT)
        if elite is None and anon is None:
            return None
        return list(dict.fromkeys((elite or []) + (anon or [])))

    @staticmethod
    def _proxio_number(value, default=None):
        try:
            if value is None or value == '':
                return default
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _seconds_until_next_utc_day(now=None):
        now_dt = datetime.now(timezone.utc) if now is None else datetime.fromtimestamp(now, tz=timezone.utc)
        tomorrow = (now_dt + timedelta(days=1)).date()
        reset = datetime.combine(tomorrow, datetime.min.time(), tzinfo=timezone.utc)
        return max(60.0, (reset - now_dt).total_seconds())

    def _proxio_reset_days_locked(self):
        day = datetime.now(timezone.utc).date().isoformat()
        for state in self.proxio_key_state:
            if state.get('day') != day:
                state['day'] = day
                state['calls'] = 0
                # A quota/429 lock naturally expires at day rollover. Auth failures are
                # also retried once next day; a permanently invalid key then gets parked again.
                state['disabled_until'] = 0.0

    @staticmethod
    def _proxio_daily_budget():
        return max(1, int(PROXIO_CALLS_PER_KEY_PER_DAY * PROXIO_QUOTA_HEADROOM))

    def _proxio_next_key(self, skip_indexes=None):
        """Reserve one eligible API call and return (index, secret_key)."""
        if not PROXIO_API_KEYS:
            return None
        skip_indexes = set(skip_indexes or ())
        now = time.time()
        with self.lock:
            self._proxio_reset_days_locked()
            budget = self._proxio_daily_budget()
            eligible = []
            for idx, state in enumerate(self.proxio_key_state):
                if idx in skip_indexes:
                    continue
                if state['calls'] >= budget or state['disabled_until'] > now:
                    continue
                eligible.append(idx)
            if not eligible:
                self.proxio_next_retry_at = max(
                    self.proxio_next_retry_at,
                    now + self._seconds_until_next_utc_day(now),
                )
                if now - self.proxio_last_quota_log >= 900:
                    self.proxio_last_quota_log = now
                    logging.warning(
                        f"🧭 Proxio: no API key currently available inside safe quota "
                        f"({budget}/{PROXIO_CALLS_PER_KEY_PER_DAY} calls/key/day); "
                        "keep previous snapshot and other proxy sources continue"
                    )
                return None

            # Prefer the least-used keys, then rotate ties so all accounts share the load.
            min_calls = min(self.proxio_key_state[i]['calls'] for i in eligible)
            least = [i for i in eligible if self.proxio_key_state[i]['calls'] == min_calls]
            start = self.proxio_key_turn % max(1, len(PROXIO_API_KEYS))
            chosen = min(least, key=lambda i: (i - start) % len(PROXIO_API_KEYS))
            self.proxio_key_turn = (chosen + 1) % len(PROXIO_API_KEYS)
            self.proxio_key_state[chosen]['calls'] += 1
            return chosen, PROXIO_API_KEYS[chosen]

    def _proxio_disable_key(self, idx, seconds):
        now = time.time()
        with self.lock:
            if 0 <= idx < len(self.proxio_key_state):
                self.proxio_key_state[idx]['disabled_until'] = max(
                    self.proxio_key_state[idx]['disabled_until'], now + max(60.0, float(seconds))
                )

    @staticmethod
    def _proxio_retry_after_seconds(resp, default=900.0):
        try:
            value = (resp.headers.get('Retry-After') or '').strip()
            if value:
                return max(60.0, min(float(value), 86400.0))
        except Exception:
            pass
        return float(default)

    def _fetch_proxio(self):
        """Quota-safe Proxio refresh with at most one key-specific failover.

        Normal refresh uses one key. A second key is tried immediately only when the first
        key itself is rejected/quota-limited (401/402/403/404/410/429). Network/5xx/JSON
        failures do not fan out across keys because they likely affect the shared provider.
        """
        if not PROXIO_FREE_ENABLED or not PROXIO_API_KEYS:
            return {'rows': [], 'ranks': {}, 'attempted': False}

        attempted_any = False
        skipped_indexes = set()
        for key_attempt in range(PROXIO_API_RETRY_KEYS):
            selected = self._proxio_next_key(skip_indexes=skipped_indexes)
            if selected is None:
                return {'rows': None, 'ranks': None, 'attempted': attempted_any}
            idx, api_key = selected
            skipped_indexes.add(idx)
            attempted_any = True
            params = {
                'type': 'https',
                'anonymity': 'Elite',
                'limit': int(PROXIO_API_LIMIT),
            }
            try:
                resp = requests.get(
                    'https://proxio.io/api/list',
                    params=params,
                    headers={'X-Key': api_key},
                    timeout=EXTERNAL_FREE_API_TIMEOUT,
                )
            except Exception as exc:
                logging.warning(f"Proxio API unavailable: {exc}; keep previous snapshot")
                return {'rows': None, 'ranks': None, 'attempted': True}

            if resp.status_code == 429:
                wait_s = max(
                    self._proxio_retry_after_seconds(resp, default=900.0),
                    self._seconds_until_next_utc_day(),
                )
                self._proxio_disable_key(idx, wait_s)
                logging.warning(
                    "🧭 Proxio API key reached quota/rate limit (HTTP 429); key parked until reset"
                )
                if key_attempt + 1 < PROXIO_API_RETRY_KEYS:
                    continue
                return {'rows': None, 'ranks': None, 'attempted': True}

            if resp.status_code in (401, 402, 403, 404, 410):
                self._proxio_disable_key(idx, self._seconds_until_next_utc_day())
                logging.warning(
                    f"🧭 Proxio API key rejected (HTTP {resp.status_code}); key parked until next UTC day"
                )
                if key_attempt + 1 < PROXIO_API_RETRY_KEYS:
                    continue
                return {'rows': None, 'ranks': None, 'attempted': True}

            if resp.status_code != 200:
                logging.warning(f"Proxio API HTTP {resp.status_code}; keep previous snapshot")
                return {'rows': None, 'ranks': None, 'attempted': True}

            try:
                rows = self._rows_from_json(resp.json())
            except Exception as exc:
                logging.warning(f"Proxio API JSON error: {exc}; keep previous snapshot")
                return {'rows': None, 'ranks': None, 'attempted': True}

            candidates = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                proxy = self._normalize_http_proxy(row.get('ip') or row.get('host'), row.get('port'))
                host = _proxy_host(proxy) if proxy else ''
                if not proxy or not host:
                    continue

                # Defensive metadata validation even though the API query already requests it.
                anonymity = str(row.get('anonymity') or '').strip().lower()
                if anonymity and anonymity != 'elite':
                    continue
                protocols = row.get('protocols')
                if isinstance(protocols, str):
                    protocol_set = {x.strip().lower() for x in re.split(r'[,;/\s]+', protocols) if x.strip()}
                elif isinstance(protocols, (list, tuple, set)):
                    protocol_set = {str(x).strip().lower() for x in protocols if str(x).strip()}
                else:
                    protocol_set = set()
                if protocol_set and 'https' not in protocol_set:
                    continue

                reliability = self._proxio_number(row.get('reliability'), None)
                latency = self._proxio_number(row.get('latency_s', row.get('latency')), None)
                uptime = self._proxio_number(row.get('uptime'), None)
                if uptime is not None and 1.0 < uptime <= 100.0:
                    uptime /= 100.0

                primary = (
                    reliability is not None
                    and reliability >= PROXIO_PRIMARY_MIN_RELIABILITY
                    and (latency is None or latency <= PROXIO_PRIMARY_MAX_LATENCY_S)
                    and (uptime is None or uptime >= 0.70)
                )
                fallback = (
                    reliability is not None
                    and reliability >= PROXIO_FALLBACK_MIN_RELIABILITY
                    and (latency is None or latency <= PROXIO_FALLBACK_MAX_LATENCY_S)
                    and (uptime is None or uptime >= 0.60)
                )
                # New endpoints can have reliability=null until enough checks exist.
                new_strong = (
                    reliability is None
                    and latency is not None and latency <= min(PROXIO_PRIMARY_MAX_LATENCY_S, 1.5)
                    and uptime is not None and uptime >= 0.80
                )
                if primary:
                    tier = 0
                elif fallback:
                    tier = 1
                elif new_strong:
                    tier = 2
                else:
                    continue

                rel_sort = reliability if reliability is not None else 50.0
                lat_sort = latency if latency is not None else PROXIO_FALLBACK_MAX_LATENCY_S
                uptime_sort = uptime if uptime is not None else 0.0
                candidates.append((tier, -rel_sort, lat_sort, -uptime_sort, host, proxy))

            # Rank first, then collapse duplicate exit IPs so the best port wins per host.
            candidates.sort()
            proxies = []
            seen_hosts = set()
            for row in candidates:
                host, proxy = row[-2], row[-1]
                if host in seen_hosts:
                    continue
                seen_hosts.add(host)
                proxies.append(proxy)
                if len(proxies) >= PROXIO_SNAPSHOT_LIMIT:
                    break
            ranks = {proxy: rank for rank, proxy in enumerate(proxies)}
            if not proxies:
                logging.warning(
                    "🧭 Proxio API returned no Elite HTTPS proxy passing local quality filter; "
                    "keep previous snapshot"
                )
                return {'rows': None, 'ranks': None, 'attempted': True}
            return {'rows': proxies, 'ranks': ranks, 'attempted': True}

        return {'rows': None, 'ranks': None, 'attempted': attempted_any}

    def _source_due(self, source, now, force=False):
        if not self._source_enabled(source):
            return False
        if source == 'proxio' and now < self.proxio_next_retry_at:
            return False
        if force:
            return True
        return (now - self.last_refresh.get(source, 0.0)) >= self._source_interval(source)

    def refresh_all(self, force=False):
        if not self.refresh_gate.acquire(blocking=False):
            return False
        try:
            now = time.time()
            due = []
            with self.lock:
                for source in self.SOURCES:
                    if self._source_due(source, now, force=force):
                        due.append(source)
            if not due:
                return False

            fetchers = {
                'hproxy': self._fetch_hproxy,
                'databay': self._fetch_databay,
                'proxio': self._fetch_proxio,
            }
            results = {}
            with ThreadPoolExecutor(max_workers=min(3, len(due)), thread_name_prefix='external-free-api') as executor:
                futs = {executor.submit(fetchers[source]): source for source in due}
                for fut, source in list((f, src) for f, src in futs.items()):
                    try:
                        results[source] = fut.result()
                    except Exception as e:
                        logging.warning(f"{source} free API unavailable: {e}; keep previous snapshot")
                        results[source] = None

            changed = False
            with self.lock:
                stamp = time.time()
                for source, result in results.items():
                    if source == 'proxio':
                        result = result or {'rows': None, 'ranks': None, 'attempted': False}
                        rows = result.get('rows')
                        # A failed API call still counts as this scheduled refresh attempt;
                        # do not immediately burn another key in the same minute.
                        if result.get('attempted'):
                            self.last_refresh[source] = stamp
                        if rows is None:
                            continue
                        self.snapshots[source] = list(rows)
                        self.proxio_feed_rank = dict(result.get('ranks') or {})
                        self.last_refresh[source] = stamp
                        changed = True
                        continue

                    rows = result
                    if rows is None:
                        continue
                    self.snapshots[source] = list(rows)
                    self.last_refresh[source] = stamp
                    changed = True

                host_sets = {
                    source: {_proxy_host(p) for p in self.snapshots[source] if _proxy_host(p)}
                    for source in self.SOURCES
                }
                h_count = len(self.snapshots['hproxy'])
                d_count = len(self.snapshots['databay'])
                p_count = len(self.snapshots['proxio'])
                overlap_hosts = len(
                    (host_sets['hproxy'] & host_sets['databay']) |
                    (host_sets['hproxy'] & host_sets['proxio']) |
                    (host_sets['databay'] & host_sets['proxio'])
                )
                proxio_budget = self._proxio_daily_budget()
                proxio_calls_today = sum(st.get('calls', 0) for st in self.proxio_key_state)
                proxio_max_key_calls = max((st.get('calls', 0) for st in self.proxio_key_state), default=0)
                proxio_safe_total = proxio_budget * len(self.proxio_key_state)
            if changed:
                logging.info(
                    f"🧭 External snapshots: HProxy={h_count}, Databay={d_count}, Proxio={p_count}, "
                    f"overlap_hosts={overlap_hosts}; Proxio=Elite HTTPS quality-filtered, "
                    f"api_calls_today={proxio_calls_today}/{proxio_safe_total or 0} safe-budget "
                    f"(max_key={proxio_max_key_calls}/{proxio_budget})"
                )
            return changed
        finally:
            self.refresh_gate.release()

    def refresh_all_async(self, force=False):
        if self.refresh_gate.locked():
            return
        threading.Thread(
            target=self.refresh_all,
            kwargs={'force': force},
            name='external-free-refresh', daemon=True,
        ).start()

    def _selection_snapshot(self, preflight=False):
        with self.lock:
            enabled = [
                source for source in self.SOURCES
                if self._source_enabled(source) and self.snapshots.get(source)
            ]
            if not enabled:
                return [], {}
            if len(enabled) > 1:
                turn = self.preflight_turn if preflight else self.turn
                shift = turn % len(enabled)
                enabled = enabled[shift:] + enabled[:shift]
                if preflight:
                    self.preflight_turn += 1
                else:
                    self.turn += 1
            return enabled, {name: list(self.snapshots[name]) for name in enabled}

    def selection_snapshot(self, include_proxio=True, prefer_proxio=False):
        """Discovery snapshot with Proxio acting as a controlled reserve."""
        with self.lock:
            enabled = []
            for source in self.SOURCES:
                if source == 'proxio' and not include_proxio:
                    continue
                if self._source_enabled(source) and self.snapshots.get(source):
                    enabled.append(source)
            if not enabled:
                return [], {}
            if len(enabled) > 1:
                shift = self.turn % len(enabled)
                enabled = enabled[shift:] + enabled[:shift]
                self.turn += 1
            if prefer_proxio and 'proxio' in enabled:
                enabled = ['proxio'] + [name for name in enabled if name != 'proxio']
            return enabled, {name: list(self.snapshots[name]) for name in enabled}

    def preflight_selection_snapshot(self):
        # Independent round-robin: neutral warming does not consume direct eBay slots.
        return self._selection_snapshot(preflight=True)

    def feed_rank_fast(self, proxy, source):
        if source == 'proxio':
            return self.proxio_feed_rank.get(proxy, 10**9)
        return 10**9

    def claim_credit(self, proxy, source):
        if source not in self.stats:
            return
        now = time.time()
        with self.lock:
            self.credit_source_by_proxy[proxy] = source
            self.credit_source_at[proxy] = now
            # Keep credit history bounded; it is diagnostic only.
            if len(self.credit_source_by_proxy) > 3000:
                cutoff = now - 6 * 3600
                stale = [p for p, ts in self.credit_source_at.items() if ts < cutoff]
                for p in stale[:1000]:
                    self.credit_source_at.pop(p, None)
                    self.credit_source_by_proxy.pop(p, None)

    def credit_source_fast(self, proxy):
        return self.credit_source_by_proxy.get(proxy)

    def label_fast(self, proxy):
        source = self.credit_source_by_proxy.get(proxy)
        if source in ('hproxy', 'databay', 'proxio'):
            return source
        return None

    def all_snapshot_proxies(self):
        with self.lock:
            out = set()
            for source in self.SOURCES:
                out.update(self.snapshots[source])
            return out

    def record_result(self, proxy, result, request_kind='unknown'):
        source = self.credit_source_by_proxy.get(proxy)
        if source not in self.stats:
            return
        with self.lock:
            st = self.stats[source]
            if request_kind == 'discovery':
                st['discovery'] += 1
                self._bounded_hash_add(st['unique_tested'], proxy)
                if result == 'success':
                    st['discovery_success'] += 1
            elif request_kind in ('fixed', 'recovery'):
                st['fixed'] += 1
                if result == 'success':
                    st['fixed_success'] += 1
            if result in st:
                st[result] += 1
            if result == 'success':
                self._bounded_hash_add(st['unique_success'], proxy)
        self.maybe_log_stats(False)

    @staticmethod
    def _bounded_hash_add(bucket, proxy):
        if len(bucket) < PROVIDER_UNIQUE_STATS_CAP:
            bucket.add(hash(proxy))

    def record_winner(self, proxy):
        source = self.credit_source_by_proxy.get(proxy)
        if source not in self.stats:
            return
        with self.lock:
            self.stats[source]['winners'] += 1
        self.maybe_log_stats(True)

    def maybe_log_stats(self, force=False):
        now = time.time()
        with self.lock:
            if not force and (now - self.last_stats_log) < EXTERNAL_FREE_STATS_INTERVAL:
                return
            self.last_stats_log = now
            parts = []
            for source in ('proxyscrape', 'hproxy', 'databay', 'proxio'):
                st = self.stats[source]
                d = st['discovery']
                ds = st['discovery_success']
                ud, us = len(st['unique_tested']), len(st['unique_success'])
                rate = 100.0 * us / ud if ud else 0.0
                pool = '-'
                if source in self.snapshots:
                    pool = str(len(self.snapshots[source]))
                parts.append(
                    f"{source}: pool={pool}, discovery={d}/{ds}, unique={ud}/{us}({rate:.1f}%), "
                    f"fixed={st['fixed']}/{st['fixed_success']}, winners={st['winners']}, "
                    f"403={st['blocked']}, timeout={st['proxy_timeout']}, reject={st['proxy_rejected']}, ssl={st['proxy_ssl']}"
                )
        logging.info("📊 Free-source stats | " + " | ".join(parts))


provider_manager = ProviderManager()
external_free_manager = ExternalFreeSourceManager()

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
        # V6.35: separate cooldown used ONLY by background Webshare handoff scout.
        # It intentionally does not participate in main discovery/candidate scoring.
        self.handoff_scout_bad_until = {}
        self.last_reputation_prune = 0.0

    def _prune_reputation_locked(self, now):
        """Bound stale reputation from rotating public proxy feeds.

        Free ProxyScrape endpoints change continuously. V6.23 kept fail_streak/last_failure/
        last_used forever, which is harmless for correctness but can grow the process RSS
        over days. Keep active/current endpoints and recent history; discard old dead ones.
        """
        if now - self.last_reputation_prune < 300:
            return
        self.last_reputation_prune = now
        active = set(self.all_proxies) | set(self.proxies) | set(self.standard_current)
        try:
            active.update(provider_manager.proxy_sets.get('proxyscrape_premium', ()))
            active.update(provider_manager.proxy_sets.get('webshare', ()))
            if 'external_free_manager' in globals():
                active.update(external_free_manager.all_snapshot_proxies())
        except Exception:
            pass
        keys = set()
        for d in (self.last_used, self.success_score, self.last_success_at, self.fail_streak,
                  self.last_failure_result, self.last_failure_at, self.standard_first_seen_at):
            keys.update(d.keys())
        cutoff = now - PROXY_REPUTATION_TTL
        removable = []
        for proxy in keys:
            if proxy in active:
                continue
            activity = max(
                self.last_used.get(proxy, 0.0),
                self.last_success_at.get(proxy, 0.0),
                self.last_failure_at.get(proxy, 0.0),
                self.standard_first_seen_at.get(proxy, 0.0),
            )
            if activity < cutoff:
                removable.append((activity, proxy))
        # Hard cap is only for inactive history; never evict the currently usable pool.
        inactive_count = max(0, len(keys) - len(active & keys))
        if inactive_count > PROXY_REPUTATION_MAX:
            extra = inactive_count - PROXY_REPUTATION_MAX
            already = {p for _a, p in removable}
            older = []
            for proxy in keys:
                if proxy in active or proxy in already:
                    continue
                activity = max(
                    self.last_used.get(proxy, 0.0), self.last_success_at.get(proxy, 0.0),
                    self.last_failure_at.get(proxy, 0.0), self.standard_first_seen_at.get(proxy, 0.0),
                )
                older.append((activity, proxy))
            older.sort(key=lambda row: row[0])
            removable.extend(older[:extra])
        if not removable:
            return
        doomed = {p for _a, p in removable}
        for proxy in doomed:
            self.last_used.pop(proxy, None)
            self.success_score.pop(proxy, None)
            self.last_success_at.pop(proxy, None)
            self.fail_streak.pop(proxy, None)
            self.last_failure_result.pop(proxy, None)
            self.last_failure_at.pop(proxy, None)
            self.standard_first_seen_at.pop(proxy, None)
            self.handoff_scout_bad_until.pop(proxy, None)
            self.preflight_ok_until.pop(proxy, None)
            self.preflight_bad_until.pop(proxy, None)
            self.preflight_last_result.pop(proxy, None)
            self.preflight_latency_ms.pop(proxy, None)
            self.quality_ok_until.pop(proxy, None)
            self.quality_bad_until.pop(proxy, None)
            self.quality_last_result.pop(proxy, None)
            self.quality_latency_ms.pop(proxy, None)
            self.outage_proxy_last_probe.pop(proxy, None)

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
        for p in [p for p, until in self.handoff_scout_bad_until.items() if until <= now]:
            self.handoff_scout_bad_until.pop(p, None)
        outage_cutoff = now - OUTAGE_HOST_MEMORY
        for host in [h for h, ts in self.outage_host_last_probe.items() if ts < outage_cutoff]:
            self.outage_host_last_probe.pop(host, None)
        for p in [p for p, ts in self.outage_proxy_last_probe.items() if ts < outage_cutoff]:
            self.outage_proxy_last_probe.pop(p, None)

        self._prune_reputation_locked(now)

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
                if provider_manager.source_fast(p) != 'free':
                    continue
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
                # Managed providers already publish online/valid state and may require
                # proxy authentication. The neutral raw-socket quality preflight is for
                # the legacy FREE pool only; testing managed credentials here can create
                # false proxy_rejected and wastes metered Webshare handshakes.
                if provider_manager.source_fast(p) != 'free':
                    continue
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
                if provider_manager.source_fast(p) != 'free' and not provider_manager.managed_proxy_available(p):
                    continue
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
            # Реальный v6.20 log показал, что 10-12 neutral HTTPS-ready FREE подряд
            # задерживали managed providers. Держим глубокий reserve в памяти, но первая
            # failover-wave использует только несколько таких адресов.
            free_quality = [p for p in quality if provider_manager.source_fast(p) == 'free']
            add_group(free_quality, max_from_group=WARM_STANDBY_FREE_QUALITY_LIMIT)
            # TCP-only — слабый сигнал. Не заполняем им весь первый batch.
            add_group(tcp, max_from_group=1)
            return result

    def get_free_handoff_candidates(self, limit, excluded_hosts=None):
        """Free-only candidates for make-before-break Webshare handoff.

        No managed/provider endpoint is returned here: the whole point is to move a
        working metered Webshare fixed session back to the unmetered pool without any gap.
        Recently-good and HTTPS/TLS-ready free proxies are preferred, then fresh unknowns.
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
            rows = []
            for proxy in universe:
                if provider_manager.source_fast(proxy) != 'free':
                    continue
                host = _proxy_host(proxy)
                if not host or host in excluded_hosts:
                    continue
                if self.bad_until.get(proxy, 0) > now or self.host_bad_until.get(host, 0) > now:
                    continue
                if self.handoff_scout_bad_until.get(proxy, 0) > now:
                    continue
                if self.soft_host_penalty_until.get(host, 0) > now:
                    continue
                if self._preflight_state_locked(proxy, now) == 'bad':
                    continue
                recent_good = self._is_recent_good_locked(proxy, now)
                quality_ok = self._quality_state_locked(proxy, now) == 'ok'
                tcp_ok = self._preflight_state_locked(proxy, now) == 'ok'
                freshness = 3 if recent_good else (2 if quality_ok else (1 if tcp_ok else 0))
                rows.append((freshness, self._candidate_score_locked(proxy, now), proxy))
            rows.sort(key=lambda row: (row[0], row[1]), reverse=True)
            result = []
            used_hosts = set(excluded_hosts)
            for _freshness, _score, proxy in rows:
                host = _proxy_host(proxy)
                if host in used_hosts:
                    continue
                result.append(proxy)
                used_hosts.add(host)
                self.last_used[proxy] = now
                if len(result) >= limit:
                    break
            return result

    def mark_handoff_transient_failure(self, proxy, result):
        """Isolate weak background-scout transport failures from main reputation.

        Returns True only when the proxy was genuinely eBay-good in the recent-good
        window and the scout failure is transient (timeout/error). In that case we
        create a short HANDOFF-ONLY cooldown and leave main fail_streak/bad_until/
        last_failure untouched. Hard evidence (403/429, SSL, CONNECT reject) must still
        go through mark_failure() and protect the primary monitor.
        """
        if not proxy or result not in ('proxy_timeout', 'proxy_error'):
            return False
        now = time.time()
        with self.lock:
            if not self._is_recent_good_locked(proxy, now):
                return False
            until = now + WEBSHARE_HANDOFF_TRANSIENT_COOLDOWN
            self.handoff_scout_bad_until[proxy] = max(
                self.handoff_scout_bad_until.get(proxy, 0), until
            )
        logging.info(
            f"🌉 Handoff-only cooldown {int(WEBSHARE_HANDOFF_TRANSIENT_COOLDOWN)} сек. "
            f"для {_proxy_log_name(proxy)} после {result}; "
            "main reputation/cooldown не изменены"
        )
        return True

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
        provider_bonus = 150.0 if provider == 'proxyscrape_premium' else ((210.0 + provider_manager.webshare_balance_bonus(proxy)) if provider == 'webshare' else 0.0)

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

    def get_webshare_rescue_candidates(self, limit=1, excluded_hosts=None):
        """Return fast Webshare rescue candidates, preferring different accounts.

        With one account this deliberately behaves like the old single rescue slot.
        With multiple billing accounts, the failover wave may use different exit hosts
        while balancing traffic between credentials at once. This improves time-to-first-success without
        draining many IPs from one correlated Webshare pool.
        """
        limit = max(0, int(limit or 0))
        if limit <= 0:
            return []
        excluded_hosts = set(excluded_hosts or ())
        now = time.time()
        with self.lock:
            self._cleanup_bad_locked()
            rows = []
            for proxy in provider_manager.candidates(include_webshare=True):
                if provider_manager.source_fast(proxy) != 'webshare':
                    continue
                host = _proxy_host(proxy)
                if not host or host in excluded_hosts:
                    continue
                if self.bad_until.get(proxy, 0) > now or self.host_bad_until.get(host, 0) > now:
                    continue
                if self.soft_host_penalty_until.get(host, 0) > now:
                    continue
                idx = provider_manager.webshare_account_index_fast(proxy)
                rows.append((proxy, idx, self._candidate_score_locked(proxy, now)))
            if not rows:
                return []

            rows.sort(key=lambda row: row[2], reverse=True)
            chosen = []
            chosen_accounts = set()
            chosen_hosts = set()

            # First pass: balance endpoints across billing accounts; host de-duplication still wins.
            for proxy, idx, _score in rows:
                host = _proxy_host(proxy)
                if idx in chosen_accounts or host in chosen_hosts:
                    continue
                chosen.append(proxy)
                chosen_accounts.add(idx)
                chosen_hosts.add(host)
                self.last_used[proxy] = now
                if len(chosen) >= limit:
                    return chosen

            # Fallback only when limit exceeds usable account count.
            for proxy, _idx, _score in rows:
                host = _proxy_host(proxy)
                if proxy in chosen or host in chosen_hosts:
                    continue
                chosen.append(proxy)
                chosen_hosts.add(host)
                self.last_used[proxy] = now
                if len(chosen) >= limit:
                    break
            return chosen

    def get_webshare_rescue_candidate(self, excluded_hosts=None):
        rows = self.get_webshare_rescue_candidates(1, excluded_hosts=excluded_hosts)
        return rows[0] if rows else None

    def get_webshare_half_open_candidate(self, excluded_hosts=None):
        """One controlled Webshare retry while the shared 403 circuit is still open.

        The provider circuit remains the default. This path is intentionally tiny: at most
        one endpoint per half-open interval, and normal proxy/host cooldowns still apply.
        """
        excluded_hosts = set(excluded_hosts or ())
        raw = provider_manager.webshare_half_open_snapshot()
        if not raw:
            return None
        now = time.time()
        with self.lock:
            self._cleanup_bad_locked()
            rows = []
            for proxy in raw:
                host = _proxy_host(proxy)
                if not host or host in excluded_hosts:
                    continue
                if self.bad_until.get(proxy, 0) > now or self.host_bad_until.get(host, 0) > now:
                    continue
                if self.soft_host_penalty_until.get(host, 0) > now:
                    continue
                rows.append(proxy)
            if not rows:
                return None
            rows.sort(key=lambda p: self._candidate_score_locked(p, now), reverse=True)
            chosen = rows[0]

        if not provider_manager.claim_webshare_half_open():
            return None
        with self.lock:
            self.last_used[chosen] = now
        return chosen

    def get_premium_half_open_candidate(self, excluded_hosts=None):
        """One controlled Premium retry while its 403 circuit is still active."""
        excluded_hosts = set(excluded_hosts or ())
        raw = provider_manager.premium_half_open_snapshot()
        if not raw:
            return None
        now = time.time()
        with self.lock:
            self._cleanup_bad_locked()
            rows = []
            for proxy in raw:
                host = _proxy_host(proxy)
                if not host or host in excluded_hosts:
                    continue
                if self.bad_until.get(proxy, 0) > now or self.host_bad_until.get(host, 0) > now:
                    continue
                if self.soft_host_penalty_until.get(host, 0) > now:
                    continue
                rows.append(proxy)
            if not rows:
                return None
            rows.sort(key=lambda p: self._candidate_score_locked(p, now), reverse=True)
            chosen = rows[0]
        if not provider_manager.claim_premium_half_open():
            return None
        with self.lock:
            self.last_used[chosen] = now
        return chosen

    def get_external_quality_preflight_candidates(self, limit=4, excluded_hosts=None):
        """Neutral TLS/CONNECT candidates from HProxy/Databay/Proxio.

        At most four existing SmartReserve slots are used; this never increases thread or
        eBay-request concurrency. Fairness is source-first (one unknown candidate from each
        enabled feed before any feed receives a second slot), so Proxio is sampled without
        crowding out the proven HProxy/Databay paths. Successful preflight keeps source credit
        when the proxy later moves through warm reserve.
        """
        limit = max(0, min(int(limit or 0), 4))
        if limit <= 0 or not PROXY_QUALITY_PREFLIGHT_ENABLED:
            return []
        excluded_hosts = set(excluded_hosts or ())
        order, snapshots = external_free_manager.preflight_selection_snapshot()
        if not order:
            return []
        now = time.time()
        chosen = []
        chosen_hosts = set()
        chosen_source = {}
        rows_by_source = {}

        with self.lock:
            self._cleanup_bad_locked()
            for source in order:
                rows = []
                for proxy in snapshots.get(source, ()):
                    host = _proxy_host(proxy)
                    if not host or host in excluded_hosts:
                        continue
                    if self.bad_until.get(proxy, 0) > now or self.host_bad_until.get(host, 0) > now:
                        continue
                    if self.soft_host_penalty_until.get(host, 0) > now:
                        continue
                    if self._quality_state_locked(proxy, now) != 'unknown':
                        continue
                    if self._preflight_state_locked(proxy, now) == 'bad':
                        continue
                    rows.append(proxy)
                rows.sort(
                    key=lambda p, src=source: (
                        external_free_manager.feed_rank_fast(p, src),
                        -self._candidate_score_locked(p, now),
                    )
                )
                rows_by_source[source] = rows

            # Round 1: one per source. Round 2: at most one extra per source.
            for round_no in range(2):
                for source in order:
                    if len(chosen) >= limit:
                        break
                    taken_this_source = 0
                    for proxy in rows_by_source.get(source, ()):
                        host = _proxy_host(proxy)
                        if not host or host in chosen_hosts:
                            continue
                        # During the second round skip a source unless it already supplied
                        # exactly one candidate in the first round.
                        already = sum(1 for p in chosen if chosen_source.get(p) == source)
                        if round_no == 0 and already:
                            break
                        if round_no == 1 and already != 1:
                            break
                        chosen.append(proxy)
                        chosen_source[proxy] = source
                        chosen_hosts.add(host)
                        self.last_used[proxy] = now
                        taken_this_source = 1
                        break
                    if len(chosen) >= limit:
                        break
                if len(chosen) >= limit:
                    break

        # Claim outside ProxyManager.lock to keep lock ordering simple.
        for proxy in chosen:
            external_free_manager.claim_credit(proxy, chosen_source[proxy])
        return chosen

    def get_external_free_candidates(
        self, max_total=1, excluded_hosts=None, include_proxio=True, prefer_proxio=False
    ):
        """Return bounded external candidates without increasing global discovery concurrency."""
        max_total = max(0, min(int(max_total or 0), 2))
        if max_total <= 0:
            return []
        excluded_hosts = set(excluded_hosts or ())
        order, snapshots = external_free_manager.selection_snapshot(
            include_proxio=include_proxio,
            prefer_proxio=prefer_proxio,
        )
        if not order:
            return []
        now = time.time()
        chosen = []
        chosen_hosts = set()
        with self.lock:
            self._cleanup_bad_locked()
            for source in order:
                rows = []
                for proxy in snapshots.get(source, ()):
                    host = _proxy_host(proxy)
                    if not host or host in excluded_hosts or host in chosen_hosts:
                        continue
                    if self.bad_until.get(proxy, 0) > now or self.host_bad_until.get(host, 0) > now:
                        continue
                    if self.soft_host_penalty_until.get(host, 0) > now:
                        continue
                    if self._preflight_state_locked(proxy, now) == 'bad' or self._quality_state_locked(proxy, now) == 'bad':
                        continue
                    if self._host_recent_outage_locked(host, now) and not self._is_recent_good_locked(proxy, now):
                        continue
                    rows.append(proxy)
                if not rows:
                    continue
                rows.sort(
                    key=lambda p, src=source: (
                        external_free_manager.feed_rank_fast(p, src),
                        -self._candidate_score_locked(p, now),
                    )
                )
                proxy = rows[0]
                chosen.append((proxy, source))
                chosen_hosts.add(_proxy_host(proxy))
                self.last_used[proxy] = now
                if len(chosen) >= max_total:
                    break
        return chosen

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
                if provider_manager.source_fast(p) != 'free' and not provider_manager.managed_proxy_available(p):
                    continue
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

                # V6.28: a proxy that already failed eBay during the SAME outage must not
                # jump back ahead merely because TCP/TLS preflight still says "ready".
                # Preflight proves transport only; the real eBay result is stronger evidence.
                # Keep recently-good proxies exempt, and keep recycled endpoints available as
                # a fallback after fresh candidates are exhausted.
                if soft_penalized or recent_outage_unknown or quality_state == 'bad':
                    recycled_usable.append(p)
                elif quality_state == 'ok':
                    quality_usable.append(p)
                elif tcp_state == 'ok':
                    tcp_usable.append(p)
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
                # Scale rescue help with INDEPENDENT accounts, not with raw proxy count.
                # 1 account -> one ordinary slot. 2-4 accounts -> up to two Webshare
                # slots in ordinary rolling batches, still leaving room for Premium/FREE.
                active_ws_accounts = provider_manager.usable_webshare_account_count()
                ws_limit = min(active_ws_accounts, max(1, batch_size // 2), 2)
                ws_accounts_added = {
                    provider_manager.webshare_account_index_fast(p)
                    for p in batch if provider_manager.source_fast(p) == 'webshare'
                }
                ws_added = len([p for p in batch if provider_manager.source_fast(p) == 'webshare'])
                if ws_added < ws_limit:
                    for p in usable:
                        if provider_manager.source_fast(p) != 'webshare':
                            continue
                        idx = provider_manager.webshare_account_index_fast(p)
                        if idx in ws_accounts_added:
                            continue
                        if add_candidate(p):
                            ws_added += 1
                            ws_accounts_added.add(idx)
                            if ws_added >= ws_limit or len(batch) >= batch_size:
                                break

            # Hard boundary: later generic protocol/quality fillers must not silently
            # add extra metered Webshare endpoints beyond the account-aware quota above.
            non_webshare_usable = [
                p for p in usable if provider_manager.source_fast(p) != 'webshare'
            ]
            non_webshare_quality = [
                p for p in quality_usable if provider_manager.source_fast(p) != 'webshare'
            ]
            non_webshare_tcp = [
                p for p in tcp_usable if provider_manager.source_fast(p) != 'webshare'
            ]

            premium_block_streak = provider_manager.premium_block_streak()
            premium_limit = (
                1
                if premium_block_streak >= PREMIUM_BLOCK_THROTTLE_STREAK
                else max(1, batch_size - 1)
            )
            premium_added = 0
            for p in non_webshare_usable:
                if provider_manager.source_fast(p) != 'proxyscrape_premium':
                    continue
                if add_candidate(p):
                    premium_added += 1
                    if premium_added >= premium_limit or len(batch) >= batch_size:
                        break

            # Hard Premium boundary: every generic filler below must exclude Premium,
            # otherwise the old fallback path silently defeated the throttle.
            ordinary_usable = [
                p for p in non_webshare_usable
                if provider_manager.source_fast(p) != 'proxyscrape_premium'
            ]
            ordinary_quality = [
                p for p in non_webshare_quality
                if provider_manager.source_fast(p) != 'proxyscrape_premium'
            ]
            ordinary_tcp = [
                p for p in non_webshare_tcp
                if provider_manager.source_fast(p) != 'proxyscrape_premium'
            ]

            # Один слот по возможности оставляем уже доказанному HTTPS/TLS free-reserve.
            for p in ordinary_quality:
                if provider_manager.source_fast(p) == 'free' and add_candidate(p):
                    break

            # Если verified-free не было, Premium can fill only up to premium_limit.
            if len(batch) < batch_size and premium_added < premium_limit:
                for p in non_webshare_usable:
                    if provider_manager.source_fast(p) != 'proxyscrape_premium':
                        continue
                    if add_candidate(p):
                        premium_added += 1
                    if premium_added >= premium_limit or len(batch) >= batch_size:
                        break

            # Остальные quality-ready free endpoints. Premium is intentionally excluded.
            if len(batch) < batch_size:
                for p in ordinary_quality:
                    add_candidate(p)
                    if len(batch) >= batch_size:
                        return batch

            # TCP-only — только один дополнительный слот: лог v6.18 показал, что сам
            # открытый порт слишком слабый признак и не должен забивать весь batch.
            tcp_added = 0
            for p in ordinary_tcp:
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
                for p in ordinary_usable:
                    if _proxy_scheme(p) == preferred_scheme and add_candidate(p):
                        break

            # 4) SOCKS5 is not forced during the first SOCKS_MIX_DELAY seconds.
            # It remains available as fallback and receives one reserved slot later,
            # so we keep protocol diversity without paying early timeout/SSL cost.
            remaining = batch_size - len(batch)
            if remaining > 0:
                selected_socks = sum(1 for p in batch if _proxy_scheme(p) == 'socks5')
                want_socks_total = 1 if (batch_size >= 3 and preferred_scheme == 'socks5') else 0
                need_socks = max(0, want_socks_total - selected_socks)

                if need_socks:
                    for p in ordinary_usable:
                        if _proxy_scheme(p) == 'socks5' and add_candidate(p):
                            need_socks -= 1
                            if need_socks <= 0 or len(batch) >= batch_size:
                                break

            # 5) Остальные слоты в первую очередь HTTP/HTTPS, затем любой protocol.
            if len(batch) < batch_size:
                for p in ordinary_usable:
                    if _proxy_scheme(p) in ('http', 'https'):
                        add_candidate(p)
                        if len(batch) >= batch_size:
                            break

            if len(batch) < batch_size:
                for p in ordinary_usable:
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
            # Managed provider endpoints remain in ProviderManager/current snapshot and
            # last_success memory. Do not mix authenticated Premium/Webshare URLs into the
            # legacy free pool or background raw-socket SmartReserve.
            if provider_manager.source_fast(proxy) == 'free' and proxy not in self.proxies:
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
                # 403 / Pardon — eBay signal by exit IP. Managed datacenter IPs in the
                # v6.20 log often remained blocked well beyond 5 minutes, so do not burn
                # Webshare/Premium by recycling them too early. Free pool keeps old policy.
                provider = provider_manager.source_fast(proxy)
                if provider == 'webshare':
                    cooldown = WEBSHARE_BLOCK_COOLDOWNS[min(streak - 1, 3)]
                elif provider == 'proxyscrape_premium':
                    cooldown = PREMIUM_BLOCK_COOLDOWNS[min(streak - 1, 3)]
                elif known_good:
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
fixed_pair_since_monotonic = None

# Durable restart-sticky proxy state. Only one candidate is tried at cold leader start;
# a stale/dead endpoint can therefore cost only one short request before normal discovery.
restart_sticky_lock = threading.Lock()
restart_sticky_candidate = None
restart_sticky_consumed = False
restart_sticky_last_queued_identity = None
restart_sticky_last_queued_at = 0.0
restart_sticky_pending_record = None
restart_sticky_persist_event = threading.Event()

# V6.22 make-before-break: while a metered Webshare fixed session keeps the monitor
# continuously online, a separate worker scouts FREE proxies. A replacement is adopted
# only after it already returned a valid eBay HTTP 200 in its own Session.
webshare_handoff_wakeup_event = threading.Event()
webshare_handoff_state_lock = threading.Lock()
webshare_handoff_ready = None  # (proxy, profile, session, monotonic_created)
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
    """Open a PostgreSQL connection with bounded connect-only retry.

    We deliberately do NOT auto-retry SQL/COMMIT here: after an ambiguous network loss
    the server may already have committed a transaction. Replaying INSERT/UPDATE blindly
    could alter notification semantics. Retrying the connection handshake itself is safe.
    TCP keepalive makes the two long-lived connections (leader lock and reminder scheduler)
    notice a dead Aiven path promptly instead of remaining half-open for minutes.
    """
    last_error = None
    for attempt in range(1, DB_CONNECT_ATTEMPTS + 1):
        try:
            return psycopg2.connect(
                DATABASE_URL,
                connect_timeout=DB_CONNECT_TIMEOUT,
                application_name=application_name,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=3,
                options=(
                    f"-c statement_timeout={DB_STATEMENT_TIMEOUT_MS} "
                    f"-c lock_timeout={DB_LOCK_TIMEOUT_MS}"
                ),
            )
        except psycopg2.OperationalError as e:
            last_error = e
            if attempt >= DB_CONNECT_ATTEMPTS:
                raise
            delay = DB_CONNECT_RETRY_BASE * (2 ** (attempt - 1)) + random.uniform(0.0, 0.15)
            logging.warning(
                f"⚠️ Aiven connect failed ({application_name}), "
                f"retry {attempt}/{DB_CONNECT_ATTEMPTS - 1} in {delay:.2f}s: {e}"
            )
            time.sleep(delay)
    raise last_error


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


def _restart_sticky_safe_record(proxy):
    """Build credential-free metadata for PostgreSQL. Never persist user:password."""
    if not proxy:
        return None
    try:
        parts = urlsplit(proxy)
        host = (parts.hostname or '').strip().lower()
        port = parts.port
        scheme = (parts.scheme or 'http').lower()
        if not host or port is None or scheme not in ('http', 'https', 'socks5'):
            return None
        source = provider_manager.source_fast(proxy) if 'provider_manager' in globals() else 'free'
        if source not in ('free', 'proxyscrape_premium', 'webshare'):
            source = 'free'
        return {
            'v': 1,
            'scheme': scheme,
            'host': host,
            'port': int(port),
            'source': source,
            'saved_at': time.time(),
        }
    except Exception:
        return None


def _restart_sticky_identity(record):
    if not record:
        return None
    return (
        str(record.get('source') or 'free'),
        str(record.get('scheme') or 'http'),
        str(record.get('host') or '').lower(),
        int(record.get('port') or 0),
    )


def _queue_restart_sticky_persist(proxy, force=False):
    """Non-blocking persistence: main eBay cycle never waits on Aiven for this optimization."""
    global restart_sticky_last_queued_identity, restart_sticky_last_queued_at, restart_sticky_pending_record
    if not RESTART_STICKY_PROXY_ENABLED:
        return
    record = _restart_sticky_safe_record(proxy)
    if not record:
        return
    ident = _restart_sticky_identity(record)
    now_mono = time.monotonic()
    with restart_sticky_lock:
        if (
            not force
            and ident == restart_sticky_last_queued_identity
            and (now_mono - restart_sticky_last_queued_at) < RESTART_STICKY_PROXY_REFRESH
        ):
            return
        restart_sticky_last_queued_identity = ident
        restart_sticky_last_queued_at = now_mono
        restart_sticky_pending_record = record
    restart_sticky_persist_event.set()


def restart_sticky_persist_worker():
    """Persist latest safe proxy metadata without ever blocking the main monitor."""
    global restart_sticky_pending_record
    db_ready_event.wait()
    while True:
        restart_sticky_persist_event.wait(timeout=30)
        restart_sticky_persist_event.clear()
        with restart_sticky_lock:
            record = restart_sticky_pending_record
            restart_sticky_pending_record = None
        if not record:
            continue
        try:
            set_bot_state(RESTART_STICKY_STATE_KEY, json.dumps(record, separators=(',', ':')))
            logging.info(
                f"💾 Restart-sticky saved: {record['scheme']}://{record['host']}:{record['port']} "
                f"[{record['source']}]"
            )
        except Exception as e:
            logging.warning(f"⚠️ Не удалось сохранить restart-sticky proxy в Aiven: {e}")
            # Keep the newest pending value. If another success arrived meanwhile, do not overwrite it.
            with restart_sticky_lock:
                if restart_sticky_pending_record is None:
                    restart_sticky_pending_record = record
            restart_sticky_persist_event.wait(timeout=10)
            restart_sticky_persist_event.set()


def _load_restart_sticky_candidate():
    """Load one credential-free candidate from PostgreSQL for the next cold-start probe."""
    global restart_sticky_candidate, restart_sticky_consumed
    restart_sticky_candidate = None
    restart_sticky_consumed = False
    if not RESTART_STICKY_PROXY_ENABLED:
        return None
    try:
        raw = get_bot_state(RESTART_STICKY_STATE_KEY)
        if not raw:
            logging.info("♻️ Restart-sticky: сохранённого last-good proxy пока нет")
            return None
        record = json.loads(raw)
        if not isinstance(record, dict) or int(record.get('v', 0)) != 1:
            return None
        ident = _restart_sticky_identity(record)
        if not ident or not ident[2] or ident[3] <= 0:
            return None
        age = max(0.0, time.time() - float(record.get('saved_at') or 0.0))
        if age > RESTART_STICKY_PROXY_MAX_AGE:
            logging.info(
                f"♻️ Restart-sticky: запись устарела ({age/3600:.1f} ч.); обычный discovery"
            )
            return None
        restart_sticky_candidate = record
        logging.info(
            f"♻️ Restart-sticky loaded: {record.get('scheme','http')}://{record.get('host')}:{record.get('port')} "
            f"[{record.get('source','free')}], age={age:.0f}s; будет проверен первым"
        )
        return record
    except Exception as e:
        logging.warning(f"⚠️ Не удалось загрузить restart-sticky proxy: {e}; обычный discovery")
        return None


def _resolve_restart_sticky_proxy(record):
    """Resolve safe DB metadata to a current proxy URL; credentials stay only in provider memory."""
    if not record:
        return None
    try:
        source = str(record.get('source') or 'free')
        scheme = str(record.get('scheme') or 'http').lower()
        host = str(record.get('host') or '').lower()
        port = int(record.get('port') or 0)
        if not host or port <= 0:
            return None
        if source == 'free':
            return f"{scheme}://{host}:{port}"

        # Managed endpoints need fresh credentials. Never reconstruct or persist them ourselves.
        rows = provider_manager.candidates(include_webshare=True)
        for proxy in rows:
            if provider_manager.source_fast(proxy) != source:
                continue
            try:
                p = urlsplit(proxy)
                if (p.hostname or '').lower() == host and int(p.port or 0) == port:
                    return proxy
            except Exception:
                continue
    except Exception:
        pass
    return None


def _consume_restart_sticky_candidate():
    global restart_sticky_consumed
    with restart_sticky_lock:
        if restart_sticky_consumed or restart_sticky_candidate is None:
            return None
        restart_sticky_consumed = True
        return dict(restart_sticky_candidate)


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


def _read_existing_auction_state(cur, item_id, include_queue=True):
    """Read durable auction state in priority order: exact -> pending -> queue."""
    item_id = str(item_id or '').strip()
    if not item_id:
        return None
    cur.execute(
        "SELECT title, end_time_utc FROM auction_reminders WHERE item_id=%s",
        (item_id,),
    )
    row = cur.fetchone()
    if row:
        return {'state': 'exact', 'title': row[0], 'end_time_utc': row[1]}

    cur.execute(
        """
        SELECT title, remaining_text, end_earliest_utc, end_latest_utc, status_message_id
        FROM auction_pending WHERE item_id=%s
        """,
        (item_id,),
    )
    row = cur.fetchone()
    if row:
        return {
            'state': 'pending', 'title': row[0], 'remaining_text': row[1],
            'end_earliest_utc': row[2], 'end_latest_utc': row[3],
            'status_message_id': row[4],
        }

    if include_queue:
        cur.execute(
            """
            SELECT status_message_id, attempt_count, next_attempt, created_at
            FROM auction_link_queue WHERE item_id=%s
            ORDER BY created_at ASC LIMIT 1
            """,
            (item_id,),
        )
        row = cur.fetchone()
        if row:
            return {
                'state': 'queued', 'status_message_id': row[0],
                'attempt_count': row[1], 'next_attempt': row[2], 'created_at': row[3],
            }
    return None


def get_saved_auction_state(item_id):
    """Exact/pending state only; used to reconcile a queue row after crash/restart."""
    if not item_id:
        return None
    with get_db_connection('ebay_uk_auction_saved_state') as conn:
        with conn.cursor() as cur:
            return _read_existing_auction_state(cur, item_id, include_queue=False)


def enqueue_auction_link(url):
    """Durably enqueue one user auction link with cross-table duplicate protection.

    The INSERT/duplicate check is committed before Telegram update acknowledgement.
    Exact/pending/queued duplicates never create another network job. A queue row stays
    in PostgreSQL until processing reaches a durable exact/pending/terminal result, so a
    Render restart at any point simply resumes the existing job.
    """
    item_id = extract_ebay_item_id_any(url or '')
    canonical = f"https://www.ebay.co.uk/itm/{item_id}" if item_id else str(url or '')
    key = _auction_queue_key(canonical, item_id)
    duplicate_state = None
    existing_message_id = None
    is_new = False

    with get_db_connection('ebay_uk_auction_link_enqueue') as conn:
        with conn.cursor() as cur:
            if item_id:
                duplicate_state = _read_existing_auction_state(cur, item_id, include_queue=True)

            if duplicate_state is None:
                # URL-only links without an ItemID still dedupe by deterministic queue_key.
                cur.execute(
                    "SELECT status_message_id, attempt_count, next_attempt, created_at "
                    "FROM auction_link_queue WHERE queue_key=%s FOR UPDATE",
                    (key,),
                )
                row = cur.fetchone()
                if row:
                    existing_message_id = row[0]
                    duplicate_state = {
                        'state': 'queued', 'status_message_id': row[0],
                        'attempt_count': row[1], 'next_attempt': row[2], 'created_at': row[3],
                    }
                else:
                    cur.execute(
                        """
                        INSERT INTO auction_link_queue (queue_key, item_id, url, next_attempt)
                        VALUES (%s,%s,%s,NOW())
                        """,
                        (key, item_id, canonical),
                    )
                    is_new = True
            elif duplicate_state.get('state') == 'queued':
                existing_message_id = duplicate_state.get('status_message_id')
        conn.commit()

    if is_new:
        logging.info(f"📥 Auction link durably queued: key={key}, item={item_id or 'unknown'}")
        auction_link_wakeup_event.set()
    elif duplicate_state and duplicate_state.get('state') == 'queued':
        # Do not reset attempt_count/backoff on accidental duplicate messages, but wake the
        # worker in case next_attempt is already due.
        logging.info(
            f"♻️ Duplicate auction link already queued: key={key}, item={item_id or 'unknown'}, "
            f"attempt={int(duplicate_state.get('attempt_count') or 0)}"
        )
        auction_link_wakeup_event.set()
    elif duplicate_state is not None:
        logging.info(
            f"♻️ Duplicate auction already durable: item={item_id or 'unknown'}, "
            f"state={duplicate_state.get('state')}"
        )
    return key, item_id, canonical, existing_message_id, is_new, duplicate_state


def send_duplicate_auction_notice(item_id, canonical_url, state):
    """Tell the user immediately that this ItemID is already durable; no re-scrape."""
    state = state or {}
    kind = state.get('state')
    if kind == 'exact':
        title = html_lib.escape(_clean_auction_title(state.get('title'), item_id))
        end_dt = state.get('end_time_utc')
        end_line = f"\n🕒 Окончание по Киеву: {format_kyiv_datetime(end_dt)}" if end_dt else ''
        msg = (
            "ℹ️ <b>Этот аукцион уже есть в списке</b> 🇬🇧\n\n"
            f"📦 <b>{title}</b>{end_line}\n\n"
            f"🔗 <a href='{html_lib.escape(canonical_url, quote=True)}'>Открыть аукцион на eBay</a>"
        )
    elif kind == 'pending':
        title = html_lib.escape(_clean_auction_title(state.get('title'), item_id))
        remaining = _format_pending_remaining_ru(state.get('remaining_text'))
        msg = (
            "ℹ️ <b>Этот аукцион уже сохранён и уточняется</b> 🇬🇧\n\n"
            f"📦 <b>{title}</b>\n"
            f"⏳ Сейчас по eBay: <b>{html_lib.escape(remaining)}</b>\n\n"
            "🔄 Точное время будет уточнено автоматически.\n\n"
            f"🔗 <a href='{html_lib.escape(canonical_url, quote=True)}'>Открыть аукцион на eBay</a>"
        )
    else:
        attempt = int(state.get('attempt_count') or 0)
        msg = (
            "ℹ️ <b>Этот аукцион уже находится в надёжной очереди</b> 🇬🇧\n\n"
            "⏳ Повторно не добавляю. Проверка продолжится автоматически даже после перезапуска Render."
            + (f"\nПопыток: {attempt}" if attempt else '')
            + f"\n\n🔗 <a href='{html_lib.escape(canonical_url, quote=True)}'>Открыть аукцион на eBay</a>"
        )
    duplicate_keyboard = (
        auction_message_keyboard(item_id, canonical_url)
        if kind in ('exact', 'pending') and item_id
        else auction_queue_keyboard(canonical_url)
    )
    send_telegram_message(
        msg,
        reply_markup=duplicate_keyboard,
        preview_url=canonical_url,
    )


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
    # Durable adaptive retry. With one/no usable Webshare account preserve the proven
    # 15 -> 30 -> 60 cadence. Extra billing accounts add traffic budget,
    # but may expose the same exit IPs, so waiting a full minute is unnecessary:
    # 2 accounts ~= 8/16/30s, 3 ~= 5/10/20s, 4+ ~= 5/8/15s. If accounts hit
    # circuit/bandwidth limits, usable count falls and backoff automatically relaxes.
    ws_accounts = provider_manager.usable_webshare_account_count()
    if ws_accounts <= 0:
        effective_base = AUCTION_LINK_RETRY_BASE
        effective_max = min(AUCTION_LINK_RETRY_MAX, 45)
    elif ws_accounts == 1:
        # One 1GB account: rescue traffic is cheap compared with missing an auction.
        effective_base = min(AUCTION_LINK_RETRY_BASE, 10)
        effective_max = min(AUCTION_LINK_RETRY_MAX, 30)
    else:
        # V6.25: key #2 usually adds bandwidth, not new exit IPs. Use that extra budget
        # for moderately faster retries, but do NOT scale 3-4x as if the IP pool grew.
        effective_base, effective_max = 7, 20
    raw_delay = effective_base * (2 ** min(attempt_count - 1, 2))
    delay = min(effective_max, raw_delay)
    delay = max(effective_base, int(delay + random.uniform(0, min(3, delay * 0.12))))
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
        f"повтор примерно через {delay} сек.; reason={reason}; usable_webshare_accounts={ws_accounts}"
    )
    return delay


def delete_auction_link_job(queue_key):
    with get_db_connection('ebay_uk_auction_link_delete') as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM auction_link_queue WHERE queue_key=%s RETURNING queue_key", (queue_key,))
            deleted = cur.fetchone() is not None
        conn.commit()
    return deleted


def get_auction_durable_counts():
    """Return exact/pending/queued counts from PostgreSQL for startup diagnostics."""
    with get_db_connection('ebay_uk_auction_durable_counts') as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM auction_reminders),
                    (SELECT COUNT(*) FROM auction_pending),
                    (SELECT COUNT(*) FROM auction_link_queue)
                """
            )
            row = cur.fetchone() or (0, 0, 0)
            return tuple(int(x or 0) for x in row)


def recover_auction_queue_after_startup():
    """Keep every unfinished job and make old backoff retry soon after a deploy/restart."""
    with get_db_connection('ebay_uk_auction_queue_recover') as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE auction_link_queue
                SET next_attempt = LEAST(next_attempt, NOW() + INTERVAL '3 seconds'),
                    updated_at = NOW()
                WHERE next_attempt > NOW() + INTERVAL '3 seconds'
                """
            )
            accelerated = cur.rowcount
        conn.commit()
    exact, pending, queued = get_auction_durable_counts()
    logging.info(
        f"♻️ Durable auction state restored: exact={exact}, pending={pending}, queued={queued}; "
        f"restart-accelerated={accelerated}"
    )
    if queued:
        auction_link_wakeup_event.set()
    return exact, pending, queued


def _advance_queued_auctions_for_new_fixed(limit=10):
    """A newly proven main Session is a fresh auction opportunity: do not wait 60s."""
    try:
        with get_db_connection('ebay_uk_auction_queue_new_fixed') as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH picked AS (
                        SELECT queue_key
                        FROM auction_link_queue
                        ORDER BY created_at ASC
                        LIMIT %s
                    )
                    UPDATE auction_link_queue AS q
                    SET next_attempt = LEAST(q.next_attempt, NOW()),
                        updated_at = NOW()
                    FROM picked
                    WHERE q.queue_key = picked.queue_key
                    RETURNING q.queue_key
                    """,
                    (max(1, int(limit)),),
                )
                changed = len(cur.fetchall())
            conn.commit()
        if changed:
            logging.info(
                f"⚡ Новый рабочий fixed proxy: {changed} queued auction job(s) получили немедленный retry"
            )
            auction_link_wakeup_event.set()
    except Exception as e:
        # This optimization must never affect the main monitor/fixed proxy.
        logging.warning(f"Не удалось ускорить auction queue после нового fixed proxy: {e}")


def wake_queued_auctions_for_new_fixed(limit=10):
    auction_link_wakeup_event.set()
    threading.Thread(
        target=_advance_queued_auctions_for_new_fixed,
        args=(limit,),
        daemon=True,
        name='auction-new-fixed-wakeup',
    ).start()


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
                  AND end_time_utc <= NOW() + INTERVAL '24 hours'
                ORDER BY end_time_utc ASC,
                         COALESCE(last_status_check, TIMESTAMPTZ '1970-01-01') ASC
                LIMIT %s
                """,
                (limit,),
            )
            return cur.fetchall()


def _status_check_interval_seconds(remaining):
    """Network re-check cadence for already exact auctions.

    Reminders themselves are DB/time driven and do not require an eBay fetch.  We therefore
    never poll exact auctions while they are >24h away, and keep only a progressively tighter
    verification cadence inside the final 24h.  Pending/coarse auctions have their own
    refinement scheduler and are intentionally unchanged.
    """
    if remaining <= 5 * 60:
        return None
    if remaining <= 10 * 60:
        return 120          # final validation window
    if remaining <= 30 * 60:
        return 300
    if remaining <= 60 * 60:
        return 600
    if remaining <= 2 * 3600:
        return 1200         # every 20 min
    if remaining <= 6 * 3600:
        return 3600         # every hour
    if remaining <= 12 * 3600:
        return 7200         # every 2 hours
    if remaining <= 24 * 3600:
        return 10800        # every 3 hours
    return None


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


def bot_main_reply_keyboard():
    """Persistent Telegram keyboard next to the message input.

    Telegram reply-keyboard buttons send their visible text as a normal message, so the
    listener maps "📋 Аукционы" to the exact same handler as /auctions.  Inline keyboards
    on individual auction messages can coexist with this persistent chat keyboard.
    """
    return {
        'keyboard': [[{'text': '📋 Аукционы'}]],
        'resize_keyboard': True,
        'one_time_keyboard': False,
        'is_persistent': True,
        'input_field_placeholder': 'Отправьте ссылку eBay или откройте аукционы',
    }


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


def _normalize_ebay_url_candidate(raw):
    """Normalize a Telegram/eBay URL candidate without following it."""
    candidate = str(raw or '').strip().strip('<>')
    candidate = candidate.rstrip(').,;]>}\"\'')
    if not candidate:
        return None
    if not re.match(r'(?i)^https?://', candidate):
        candidate = 'https://' + candidate.lstrip('/')
    return candidate if _is_allowed_ebay_url(candidate) else None


def extract_ebay_urls(text):
    """Extract literal and scheme-less eBay URLs from plain text.

    Telegram/iOS may paste ``www.ebay.co.uk/...`` without ``https://``.  The old
    extractor silently ignored such a message and then acknowledged its update_id.
    """
    urls = []
    seen = set()
    source = str(text or '')
    patterns = (
        r'https?://[^\s<>]+',
        r'(?<![\w@])(?:www\.)?(?:[A-Za-z0-9-]+\.)*ebay\.(?:co\.uk|com|us)(?:/[^\s<>]*)?',
    )
    for pattern in patterns:
        for raw in re.findall(pattern, source, flags=re.I):
            url = _normalize_ebay_url_candidate(raw)
            if url and url not in seen:
                seen.add(url)
                urls.append(url)
    return urls


def _telegram_utf16_slice(text, offset, length):
    """Slice Telegram entity text using Telegram's UTF-16 code-unit offsets."""
    try:
        raw = str(text or '').encode('utf-16-le')
        start = max(0, int(offset or 0)) * 2
        end = start + max(0, int(length or 0)) * 2
        return raw[start:end].decode('utf-16-le', errors='ignore')
    except Exception:
        return ''


def _telegram_message_raw_url_candidates(message):
    """Return URL candidates from text/caption and Telegram URL entities.

    A ``text_link`` entity can hide the real URL behind visible product-title text, so
    looking at ``message['text']`` alone is not sufficient.  Caption entities are handled
    too because forwarded/shared listings can arrive as media captions.
    """
    message = message or {}
    candidates = []
    for text_key, entities_key in (('text', 'entities'), ('caption', 'caption_entities')):
        body = str(message.get(text_key) or '')
        if body:
            candidates.extend(extract_ebay_urls(body))
        for entity in message.get(entities_key) or []:
            etype = str(entity.get('type') or '')
            raw_url = None
            if etype == 'text_link':
                raw_url = entity.get('url')
            elif etype == 'url':
                raw_url = _telegram_utf16_slice(
                    body, entity.get('offset', 0), entity.get('length', 0)
                )
            if raw_url:
                candidates.append(str(raw_url))

    # Defensive coverage for less common Telegram representations.  Incoming link-preview
    # metadata and inline-keyboard buttons can carry a URL that is not literally present in
    # text/caption.  They are cheap to inspect and eliminate another possible silent-drop path.
    preview = message.get('link_preview_options') or {}
    if preview.get('url'):
        candidates.append(str(preview.get('url')))
    keyboard = (message.get('reply_markup') or {}).get('inline_keyboard') or []
    for row in keyboard:
        for button in row or []:
            if isinstance(button, dict) and button.get('url'):
                candidates.append(str(button.get('url')))
    return candidates


def extract_ebay_urls_from_telegram_message(message):
    """Robust eBay URL extraction for Telegram messages/captions/entities."""
    urls = []
    seen = set()
    for raw in _telegram_message_raw_url_candidates(message):
        # ``raw`` may already be normalized by extract_ebay_urls() or may come directly
        # from a Telegram text_link entity.
        normalized = _normalize_ebay_url_candidate(raw)
        if normalized and normalized not in seen:
            seen.add(normalized)
            urls.append(normalized)
    return urls


def telegram_message_has_ebay_signal(message):
    """Detect an eBay-looking message so it can never be silently acknowledged."""
    message = message or {}
    for key in ('text', 'caption'):
        if 'ebay.' in str(message.get(key) or '').lower():
            return True
    for key in ('entities', 'caption_entities'):
        for entity in message.get(key) or []:
            if 'ebay.' in str(entity.get('url') or '').lower():
                return True
    if 'ebay.' in str((message.get('link_preview_options') or {}).get('url') or '').lower():
        return True
    for row in (message.get('reply_markup') or {}).get('inline_keyboard') or []:
        for button in row or []:
            if isinstance(button, dict) and 'ebay.' in str(button.get('url') or '').lower():
                return True
    return False


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
        if p not in auction_proxy_success_at:
            auction_proxy_fail_streak.pop(p, None)
    stale_hosts = [h for h, until in auction_proxy_host_bad_until.items() if until <= now]
    for h in stale_hosts:
        auction_proxy_host_bad_until.pop(h, None)
    stale_success = [p for p, ts in auction_proxy_success_at.items() if now - ts > AUCTION_PROXY_GOOD_MEMORY]
    for p in stale_success:
        auction_proxy_success_at.pop(p, None)
        if auction_proxy_bad_until.get(p, 0) <= now:
            auction_proxy_fail_streak.pop(p, None)


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
    # Decode once. Auction pages can be hundreds of KB; repeated response.text calls
    # create avoidable temporary Unicode copies near Render's 512 MB memory ceiling.
    body_text = response.text or ''
    final_url = str(getattr(response, 'url', '') or '')
    status = int(getattr(response, 'status_code', 0) or 0)
    if status not in (200, 404, 410):
        logging.info(f"Auction page через {_proxy_log_name(proxy)}: HTTP {status}, url={final_url[:160]}")
        if status == 403:
            return None, final_url, 'blocked'
        if status == 429:
            return None, final_url, 'rate_limited'
        return None, final_url, 'http_error'

    blocked, reason = _is_ebay_block_page(response, body_text)
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
    return body_text, final_url, 'success'


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


def _request_auction_via_main_session(url, connect_timeout=None, read_timeout=None, wait_timeout=None, penalize_failure=True):
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
                f"Auction: main fixed Session {_proxy_log_name(current)} занята; ждали {wait_timeout:.1f} сек., "
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
        elif result not in ('main_busy', 'main_unavailable') and penalize_failure:
            _auction_proxy_mark_failure(proxy, result)
        elif result not in ('main_busy', 'main_unavailable') and not penalize_failure:
            logging.info(
                f"🧭 Auction route-only failure via {_proxy_log_name(proxy)}: {result}; "
                "не ставим whole-proxy auction cooldown до проверки canonical item page"
            )
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
    penalize_main_failure=True,
    allow_webshare_reserve=True,
    penalize_reserve_blocked=True,
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
                penalize_failure=penalize_main_failure,
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
            allow_webshare=allow_webshare_reserve,
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
                allow_webshare=allow_webshare_reserve,
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
            if result == 'blocked' and not penalize_reserve_blocked:
                logging.info(
                    f"🧭 Auction reserve search-route 403 via {_proxy_log_name(proxy)}; "
                    "оставляем proxy доступным для canonical item-page fallback"
                )
            else:
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
                    penalize_failure=penalize_main_failure,
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


def _html_visible_and_h1(raw_html):
    """Return visible text and H1 text without retaining a full BeautifulSoup tree."""
    if not raw_html:
        return '', ''
    soup = BeautifulSoup(raw_html, 'html.parser')
    try:
        visible = re.sub(r'\s+', ' ', soup.get_text(' ', strip=True))
        h1 = soup.find('h1')
        h1_text = re.sub(r'\s+', ' ', h1.get_text(' ', strip=True)).strip() if h1 else ''
        return visible, h1_text
    finally:
        try:
            soup.decompose()
        except Exception:
            pass


def _html_visible_text(raw_html):
    return _html_visible_and_h1(raw_html)[0]


def _extract_exact_start_time(raw_html, item_id=None):
    if not raw_html:
        return None, 'none'
    visible = _html_visible_text(raw_html)
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

    visible = _html_visible_text(raw_html)
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
    try:
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

    finally:
        try:
            soup.decompose()
        except Exception:
            pass

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
    visible = _html_visible_text(raw_html)
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
        visible = _html_visible_text(blob)
        minute_end = _extract_visible_end_minute(visible)
        if minute_end and not creation_only_start:
            candidate = minute_end.replace(second=start.second, microsecond=start.microsecond)
            if candidate > datetime.now(timezone.utc) - timedelta(hours=1):
                return candidate, f'derived:{start_source}+visible_end_minute'

    now = _ensure_aware_utc(observed_at or datetime.now(timezone.utc))
    standard_days = (1, 3, 5, 7, 10, 30)
    for blob in blobs:
        visible = _html_visible_text(blob)
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
    try:
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

    finally:
        try:
            soup.decompose()
        except Exception:
            pass

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
    try:
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
    finally:
        try:
            soup.decompose()
        except Exception:
            pass
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

    visible, title = _html_visible_and_h1(html)
    item_id = extract_ebay_item_id_any(final_url, html)

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
    try:
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
    finally:
        try:
            soup.decompose()
        except Exception:
            pass


def _search_target_card_tag(html, item_id):
    """Return a small detached target-card tree instead of retaining the full SRP DOM."""
    card_html = _search_target_card_html(html, item_id)
    if not card_html:
        return None
    fragment = BeautifulSoup(card_html, 'html.parser')
    # The fragment is small (one card); returning its root no longer pins the full
    # 1.6 MB search-page BeautifulSoup graph in memory.
    return fragment.find(['li', 'div']) or fragment


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
    visible, title = _html_visible_and_h1(html)
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
                f"🕒 Окончание по Киеву: {format_kyiv_datetime(end_time_utc)}\n\n"
                f"🔗 <a href='{html_lib.escape(canonical_url, quote=True)}'>Открыть аукцион на eBay</a>",
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
            "🔔 Напоминания: <b>60 / 30 / 10 / 5 мин.</b>\n\n"
            f"🔗 <a href='{html_lib.escape(canonical_url, quote=True)}'>Открыть аукцион на eBay</a>",
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


def _fetch_exact_item_search_pages(item_id, reserve_if_needed=True, prefer_current_fixed=True):
    """Fetch exact-item search card with route-aware fixed-session handling.

    A 403 on /sch/i.html for an ItemID is not enough to condemn a proxy: the same
    persistent Session may still serve /itm/<id> and the normal monitor. Therefore the
    fixed search-route probe is non-penalizing. Reserve search proxies are used only
    after the caller has had a chance to try the canonical item page via the same fixed.
    """
    search_url = _auction_exact_search_url(item_id)
    pages = []

    if prefer_current_fixed:
        pages = fetch_auction_pages(
            search_url,
            max_reserve_proxies=0,
            connect_timeout=min(AUCTION_FETCH_CONNECT_TIMEOUT, 4.0),
            read_timeout=min(AUCTION_FETCH_READ_TIMEOUT, 10.0),
            max_pages=1,
            canonicalize_item=False,
            prefer_current_fixed=True,
            penalize_main_failure=False,
        )
        exact, coarse, had_target = _evaluate_search_pages_for_auction(pages, item_id)
        if exact or coarse or not reserve_if_needed:
            return exact, coarse, had_target, pages
    else:
        exact, coarse, had_target = None, None, False
        if not reserve_if_needed:
            return exact, coarse, had_target, pages

    # Reserve search is bounded and may use metered Webshare rescue if available.
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
        allow_webshare_reserve=True,
        penalize_reserve_blocked=False,
    )
    exact2, coarse2, had_target2 = _evaluate_search_pages_for_auction(reserve_pages, item_id)
    return exact2, coarse2, had_target or had_target2, pages + reserve_pages


def _try_resolve_quick_item_pages(pages, original_url, expected_item_id):
    """Try to finish an auction from one/few already fetched item pages.

    Used immediately after a route-specific search 403 on the same fixed Session, before
    rotating through reserve proxies. Returns None only when more network evidence is needed.
    """
    if not pages:
        return None
    parsed = []
    htmls = []
    coarse = None
    for html, final_url, proxy_used in pages:
        if not html:
            continue
        htmls.append(html)
        parse_url = final_url if extract_ebay_item_id_any(final_url or '') else original_url
        result = parse_auction_page(html, parse_url)
        item_id, title, end_time_utc, parse_source, auction_status = result
        logging.info(
            f"🧪 Auction quick-main parse: proxy={_proxy_log_name(proxy_used)}, item={item_id}, "
            f"status={auction_status}, source={parse_source}, "
            f"end={end_time_utc.isoformat() if end_time_utc else None}"
        )
        if expected_item_id and item_id and item_id != expected_item_id:
            continue
        parsed.append(result)
        if auction_status == 'active' and item_id and end_time_utc:
            return _finish_exact_auction_save(item_id, title, end_time_utc, parse_source, notify=True)
        if not end_time_utc and coarse is None:
            coarse = extract_coarse_auction_page_observation(
                html, parse_url, expected_item_id=expected_item_id,
                observed_at=datetime.now(timezone.utc),
            )

    if htmls:
        timer_end, timer_source, _obs = _resolve_localized_timer_evidence(htmls)
        if timer_end and any(
            _page_has_active_auction_evidence(h, item_id=expected_item_id) for h in htmls
        ):
            representative = next((r for r in parsed if r[0]), None)
            item_id = expected_item_id or (representative[0] if representative else None)
            title = (representative[1] if representative else '') or f'eBay item {item_id}'
            if item_id:
                return _finish_exact_auction_save(
                    item_id, title, timer_end, timer_source, notify=True
                )

    if coarse:
        return _save_pending_from_observation(coarse, original_url, notify=True)

    statuses = [r[4] for r in parsed]
    if 'ended' in statuses:
        ended = next(r for r in parsed if r[4] == 'ended')
        line = f"\n\n🕒 Окончание по Киеву: {format_kyiv_datetime(ended[2])}" if ended[2] else ''
        publish_auction_status(
            expected_item_id or ended[0],
            "⌛ <b>Этот аукцион уже завершён.</b>" + line,
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
    return None


def process_auction_link(url):
    if not _is_allowed_ebay_url(url):
        return 'ignored'

    expected_item_id = extract_ebay_item_id_any(url or '')
    search_pages = []

    # 1) First use the already proven persistent fixed Session. A search-route 403 is
    # route-specific evidence only; before any reserve rotation immediately try the
    # canonical /itm/<id> page through the SAME Session.
    if expected_item_id:
        exact, coarse, had_target, fixed_search_pages = _fetch_exact_item_search_pages(
            expected_item_id,
            reserve_if_needed=False,
            prefer_current_fixed=True,
        )
        search_pages.extend(fixed_search_pages)
        if exact:
            item_id, title, end_time_utc, source, status = exact
            return _finish_exact_auction_save(item_id, title, end_time_utc, source, notify=True)
        if coarse:
            return _save_pending_from_observation(coarse, url, notify=True)

        canonical_item_url = f"https://www.ebay.co.uk/itm/{expected_item_id}"
        fixed_item_pages = fetch_auction_pages(
            canonical_item_url,
            max_reserve_proxies=0,
            connect_timeout=min(AUCTION_FETCH_CONNECT_TIMEOUT, 4.0),
            read_timeout=min(AUCTION_FETCH_READ_TIMEOUT, 10.0),
            max_pages=1,
            canonicalize_item=True,
            prefer_current_fixed=True,
            penalize_main_failure=True,
        )
        quick_result = _try_resolve_quick_item_pages(
            fixed_item_pages, canonical_item_url, expected_item_id
        )
        if quick_result is not None:
            return quick_result

        # Only now rotate to two reserve search candidates. This avoids putting a good
        # main proxy in auction cooldown merely because /sch/i.html was blocked.
        exact, coarse, had_target2, reserve_search_pages = _fetch_exact_item_search_pages(
            expected_item_id,
            reserve_if_needed=True,
            prefer_current_fixed=False,
        )
        search_pages.extend(reserve_search_pages)
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


def _reconcile_completed_auction_queue_job(item_id, status_message_id, state):
    """Finish the tiny crash window: saved exact/pending row exists but queue row remains."""
    kind = (state or {}).get('state')
    canonical = f"https://www.ebay.co.uk/itm/{item_id}"
    try:
        if kind == 'exact' and status_message_id:
            title = html_lib.escape(_clean_auction_title(state.get('title'), item_id))
            end_dt = state.get('end_time_utc')
            publish_auction_status(
                item_id,
                "✅ <b>Аукцион сохранён</b> 🇬🇧\n\n"
                f"📦 <b>{title}</b>\n\n"
                f"🕒 Окончание по Киеву: {format_kyiv_datetime(end_dt)}\n\n"
                f"🔗 <a href='{html_lib.escape(canonical, quote=True)}'>Открыть аукцион на eBay</a>",
                reply_markup=auction_message_keyboard(item_id, canonical),
                preview_url=canonical,
                existing_message_id=status_message_id,
            )
        elif kind == 'pending':
            if status_message_id:
                set_pending_status_message(item_id, status_message_id)
                title = html_lib.escape(_clean_auction_title(state.get('title'), item_id))
                remaining = _format_pending_remaining_ru(state.get('remaining_text'))
                publish_auction_status(
                    item_id,
                    "🟡 <b>Аукцион сохранён</b> 🇬🇧\n\n"
                    f"📦 <b>{title}</b>\n\n"
                    f"⏳ Сейчас по eBay: <b>{html_lib.escape(remaining)}</b>\n\n"
                    "🔄 Точное время окончания уточню автоматически.",
                    reply_markup=auction_message_keyboard(item_id, canonical),
                    preview_url=canonical,
                    existing_message_id=status_message_id,
                    persist_pending=True,
                )
    except Exception as e:
        # Durable schedule already exists; Telegram cosmetics must not re-open network work.
        logging.warning(f"Не удалось восстановить auction status message для {item_id}: {e}")


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

            # Main new-item monitor has priority over auction enrichment under failover/RAM
            # pressure. The job stays due in PostgreSQL (no attempt/backoff increment), so
            # it resumes within a couple seconds when the main path is stable.
            if main_discovery_active_event.is_set() or memory_pressure_event.is_set():
                auction_link_wakeup_event.wait(timeout=2.0)
                continue

            # Crash-safe reconciliation: exact/pending save is committed BEFORE queue deletion.
            # If Render died in that tiny window, do not scrape eBay again after restart.
            if item_id:
                saved_state = get_saved_auction_state(item_id)
                if saved_state is not None:
                    _reconcile_completed_auction_queue_job(item_id, status_message_id, saved_state)
                    delete_auction_link_job(queue_key)
                    logging.info(
                        f"♻️ Auction queue reconciled after restart/crash: key={queue_key}, "
                        f"durable_state={saved_state.get('state')}"
                    )
                    continue

            try:
                try:
                    created_utc = _ensure_aware_utc(created_at) if created_at is not None else None
                    queue_age = max(0.0, (datetime.now(timezone.utc) - created_utc).total_seconds()) if created_utc else 0.0
                except Exception:
                    queue_age = 0.0
                logging.info(
                    f"🎯 Auction user-job start: key={queue_key}, item={item_id or 'unknown'}, "
                    f"attempt={int(attempt_count or 0) + 1}, queue_age={queue_age:.1f}s"
                )
                auction_user_job_active_event.set()
                result = process_auction_link(url)
            except Exception as e:
                logging.error(f"Ошибка обработки auction URL {url}: {e}", exc_info=True)
                postpone_auction_link_job(queue_key, attempt_count, 'internal_error')
                continue
            finally:
                auction_user_job_active_event.clear()
                auction_status_wakeup_event.set()

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
            try:
                created_utc = _ensure_aware_utc(created_at) if created_at is not None else None
                total_age = max(0.0, (datetime.now(timezone.utc) - created_utc).total_seconds()) if created_utc else 0.0
            except Exception:
                total_age = 0.0
            logging.info(
                f"✅ Auction queue job завершён: key={queue_key}, result={result!r}, "
                f"total_latency={total_age:.1f}s"
            )

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
    # Saved-auction background checks should first reuse the currently proven fixed Session.
    # The V6.25 log showed an old auction-good proxy timing out repeatedly while a newer main
    # fixed proxy was serving the normal eBay search successfully. Only if current fixed cannot
    # serve the item page do we spend one reserve candidate.
    html, final_url, proxy_used = fetch_auction_page(
        url,
        max_reserve_proxies=max_reserve_proxies,
        connect_timeout=AUCTION_STATUS_CONNECT_TIMEOUT,
        read_timeout=AUCTION_STATUS_READ_TIMEOUT,
        prefer_current_fixed=True,
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
    """Refine pending with current fixed Session first, then a tiny reserve fallback.

    V6.26: search-route 403/no-target is not enough reason to rotate through Webshare/free.
    Try canonical /itm/<id> through the same proven main Session before reserve search.
    """
    # 1) Lightweight exact search through the current fixed Session only.
    exact, coarse, _had_target, _pages = _fetch_exact_item_search_pages(
        item_id, reserve_if_needed=False, prefer_current_fixed=True
    )
    if exact:
        exact_id, exact_title, exact_end, source, status = exact
        logging.info(
            f"✅ Pending auction уточнён через current fixed search: item={item_id}, "
            f"end={exact_end.isoformat()}, source={source}"
        )
        return 'exact' if _finish_exact_auction_save(
            exact_id, exact_title or title, exact_end, source, notify=True, refined=True
        ) else 'finished'
    if coarse:
        pending_state = _save_pending_from_observation(coarse, url, notify=False, refined=True)
        if pending_state == 'exact':
            return 'exact'
        logging.info(
            f"🟡 Pending auction пока coarse через current fixed search: item={item_id}, "
            f"remaining={coarse['remaining_text']!r}"
        )
        return 'coarse'

    # 2) Search route may be blocked while canonical item page works on the SAME Session.
    canonical = f"https://www.ebay.co.uk/itm/{item_id}"
    fixed_item_pages = fetch_auction_pages(
        canonical,
        max_reserve_proxies=0,
        connect_timeout=min(AUCTION_STATUS_CONNECT_TIMEOUT, 4.0),
        read_timeout=min(AUCTION_STATUS_READ_TIMEOUT, 8.0),
        max_pages=1,
        canonicalize_item=True,
        prefer_current_fixed=True,
        penalize_main_failure=True,
    )
    fixed_htmls = []
    coarse_item = None
    for html, final_url, proxy_used in fixed_item_pages:
        if not html:
            continue
        fixed_htmls.append(html)
        parse_url = final_url if extract_ebay_item_id_any(final_url or '') else canonical
        parsed_id, parsed_title, parsed_end, source, status = parse_auction_page(html, parse_url)
        if parsed_id and parsed_id != str(item_id):
            continue
        if status == 'active' and parsed_end:
            logging.info(
                f"✅ Pending auction уточнён через current fixed item-page: item={item_id}, "
                f"end={parsed_end.isoformat()}, source={source}"
            )
            return 'exact' if _finish_exact_auction_save(
                str(item_id), parsed_title or title, parsed_end, source, notify=True, refined=True
            ) else 'finished'
        if coarse_item is None:
            coarse_item = extract_coarse_auction_page_observation(
                html, parse_url, expected_item_id=str(item_id), observed_at=datetime.now(timezone.utc)
            )

    if fixed_htmls:
        timer_end, timer_source, _timer_obs = _resolve_localized_timer_evidence(fixed_htmls)
        if timer_end and any(_page_has_active_auction_evidence(h, item_id=str(item_id)) for h in fixed_htmls):
            logging.info(
                f"✅ Pending auction уточнён по timer через current fixed item-page: "
                f"item={item_id}, end={timer_end.isoformat()}, source={timer_source}"
            )
            return 'exact' if _finish_exact_auction_save(
                str(item_id), title, timer_end, timer_source, notify=True, refined=True
            ) else 'finished'
    if coarse_item:
        pending_state = _save_pending_from_observation(coarse_item, url, notify=False, refined=True)
        if pending_state == 'exact':
            return 'exact'
        logging.info(
            f"🟡 Pending auction пока coarse через current fixed item-page: item={item_id}, "
            f"remaining={coarse_item['remaining_text']!r}"
        )
        return 'coarse'

    # 3) Only now spend the small reserve search fallback (max two candidates internally).
    exact, coarse, _had_target, _pages = _fetch_exact_item_search_pages(
        item_id, reserve_if_needed=True, prefer_current_fixed=False
    )
    if exact:
        exact_id, exact_title, exact_end, source, status = exact
        logging.info(
            f"✅ Pending auction уточнён через reserve search: item={item_id}, "
            f"end={exact_end.isoformat()}, source={source}"
        )
        return 'exact' if _finish_exact_auction_save(
            exact_id, exact_title or title, exact_end, source, notify=True, refined=True
        ) else 'finished'
    if coarse:
        pending_state = _save_pending_from_observation(coarse, url, notify=False, refined=True)
        if pending_state == 'exact':
            return 'exact'
        logging.info(
            f"🟡 Pending auction пока coarse после reserve search: item={item_id}, "
            f"remaining={coarse['remaining_text']!r}; следующая проверка будет рассчитана заново"
        )
        return 'coarse'

    # Network/HTML ambiguity never deletes pending. Close to end, keep reminders responsive.
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
        f"🟡 Pending auction {item_id}: exact time пока недоступен; "
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
            healthy = (
                (is_paused or (had_success and elapsed < 180))
                and not main_discovery_active_event.is_set()
                and not memory_pressure_event.is_set()
                and not auction_user_job_active_event.is_set()
            )
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
                # Old DB rows may theoretically miss URL. Reconstruct the canonical item
                # URL so every 60/30/10/5 reminder ALWAYS has a clickable text link,
                # independently of the inline Telegram button.
                current_url = str(current_url or f"https://www.ebay.co.uk/itm/{item_id}")
                safe_current_url = html_lib.escape(current_url, quote=True)
                text_link = f"\n\n🔗 <a href='{safe_current_url}'>Открыть аукцион на eBay</a>"

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
                    f"{text_link}"
                )
                keyboard = (
                    auction_open_list_keyboard(current_url)
                    if is_final_five
                    else auction_message_keyboard(item_id, current_url)
                )

                # Use the same explicit preview path as the successful "Auction saved"
                # message.  The clickable text link remains in the message, while Telegram
                # is explicitly asked to build the eBay item preview for all 60/30/10/5 notices.
                if send_telegram_message(
                    msg,
                    reply_markup=keyboard,
                    preview_url=current_url,
                    preview_small=True,
                ):
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

                update_processed_ok = False
                try:
                    callback = update.get('callback_query')
                    if callback:
                        handle_telegram_callback(callback)
                    else:
                        # Besides ordinary text messages, accept edited messages and channel
                        # posts.  Duplicate auction protection is ItemID/queue-key based, so an
                        # edited/replayed update is safe and cannot create a second durable job.
                        message = (
                            update.get('message')
                            or update.get('edited_message')
                            or update.get('channel_post')
                            or update.get('edited_channel_post')
                            or update.get('business_message')
                            or update.get('edited_business_message')
                        )
                        if message and str(message.get('chat', {}).get('id')) == str(TELEGRAM_CHAT_ID):
                            text = str(message.get('text') or '').strip()
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
                                send_telegram_message(
                                    "▶ Основной мониторинг продолжает работу",
                                    reply_markup=bot_main_reply_keyboard(),
                                )
                                logging.info("Команда /start - продолжение; watchdog-таймер перезапущен")
                            elif text in ('/auctions', '/list', '📋 Аукционы', 'Аукционы'):
                                # Persistent button and slash command deliberately share one handler.
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
                                ebay_urls = extract_ebay_urls_from_telegram_message(message)
                                if ebay_urls:
                                    logging.info(
                                        f"📨 Telegram eBay update {update_id}: распознано {len(ebay_urls)} URL; "
                                        f"text_len={len(str(message.get('text') or ''))}, "
                                        f"caption_len={len(str(message.get('caption') or ''))}"
                                    )
                                    if len(ebay_urls) > 20:
                                        logging.warning(
                                            f"Telegram update {update_id}: получено {len(ebay_urls)} eBay URL; "
                                            "обрабатываем первые 20"
                                        )
                                    for ebay_url in ebay_urls[:20]:
                                        (
                                            key, item_id, canonical_url, existing_message_id,
                                            is_new, duplicate_state
                                        ) = enqueue_auction_link(ebay_url)
                                        if duplicate_state is not None:
                                            send_duplicate_auction_notice(item_id, canonical_url, duplicate_state)
                                            continue
                                        # Одно компактное жёлтое status-сообщение. Оно не накапливается:
                                        # после получения результата бот отредактирует ЭТО ЖЕ сообщение в pending/exact.
                                        if is_new and not existing_message_id:
                                            msg_id = send_telegram_message(
                                                "🟡 <b>Аукцион добавлен</b> 🇬🇧\n\n"
                                                "⏳ Проверяю время окончания автоматически.\n"
                                                "💾 Ссылка уже сохранена в надёжной очереди и не пропадёт после перезапуска Render.",
                                                reply_markup=auction_queue_keyboard(canonical_url),
                                                preview_url=canonical_url,
                                                return_message_id=True,
                                            )
                                            if msg_id:
                                                set_auction_link_status_message(key, msg_id)
                                            else:
                                                # Queue insert is already COMMITted, so a Telegram
                                                # send failure cannot lose the auction.  The worker's
                                                # final pending/exact publication will create a new
                                                # message if there is no status_message_id.
                                                logging.warning(
                                                    f"⚠️ Auction {item_id or key} сохранён в очереди, "
                                                    "но Telegram не подтвердил жёлтый status-message; "
                                                    "финальный результат всё равно будет опубликован worker-ом"
                                                )
                                elif telegram_message_has_ebay_signal(message):
                                    # Critical anti-silence guard: never ACK an eBay-looking update
                                    # without telling the user that its URL could not be parsed.
                                    logging.warning(
                                        f"⚠️ Telegram eBay update {update_id} не удалось распознать: "
                                        f"text_len={len(str(message.get('text') or ''))}, "
                                        f"caption_len={len(str(message.get('caption') or ''))}"
                                    )
                                    send_telegram_message(
                                        "⚠️ <b>Я вижу сообщение с eBay, но не смог распознать ссылку.</b>\n\n"
                                        "Пожалуйста, отправьте полный URL лота ещё раз. "
                                        "Сообщение не было принято как аукцион."
                                    )
                    # ВАЖНО: update считается обработанным только после того, как все
                    # необходимые durable DB-операции (включая enqueue auction link) завершились.
                    update_processed_ok = True
                except Exception as e:
                    logging.error(f"Ошибка обработки Telegram update {update_id}: {e}", exc_info=True)

                if not update_processed_ok:
                    # Не подтверждаем Telegram update при DB/network exception внутри handler.
                    # getUpdates вернёт его снова, а DB-dedupe не даст создать второй auction job.
                    # Это закрывает редкое окно потери ссылки при рестарте Render прямо во время enqueue.
                    logging.warning(
                        f"⚠️ Telegram update {update_id} НЕ подтверждён; повторим после восстановления"
                    )
                    break

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


def _is_ebay_block_page(response, html_text=None):
    """Определяем именно защитную страницу eBay, а не слово robot в обычном JS.

    V6.25 accepts the already-decoded body so a 1.6 MB search page is not repeatedly
    decoded/retained while memory pressure is high.
    """
    if html_text is None:
        html_text = response.text or ""
    text_lower = html_text.lower()
    title_lower = _response_title(html_text).lower()
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


def _make_request(proxy, profile, session=None, timeout=None, request_kind="unknown"):
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

    def tracked_return(result, html_value, session_value, body_bytes=0):
        # One central accounting point prevents the misleading v6.20 stats where only
        # a fixed-session recovery was counted while discovery/fixed traffic was missing.
        if result != 'profile_error':
            provider_manager.record_result(
                proxy, result, body_bytes=body_bytes, request_kind=request_kind
            )
            if 'external_free_manager' in globals():
                external_free_manager.record_result(proxy, result, request_kind=request_kind)
            provider_manager.maybe_warn_webshare_usage()
        return result, html_value, session_value

    own_session = session is None
    if session is None:
        try:
            session = _create_session(proxy, profile, timeout=timeout)
        except Exception as e:
            logging.error(f"Не удалось создать session для {profile['name']}: {e}")
            return tracked_return('profile_error', None, None)

    try:
        # Параметр request() переопределяет timeout Session — это позволяет
        # discovery быстро отбрасывать медленные proxy, не затрагивая fixed session.
        response = session.get(EBAY_SEARCH_URL, timeout=timeout)

        # Decode exactly once. curl_cffi keeps the raw body on Response; repeated
        # response.text access during parallel discovery can create avoidable temporary
        # allocations near Render's 512 MB ceiling.
        body_text = response.text or ''
        title = _response_title(body_text)
        final_url = str(getattr(response, 'url', '') or '')
        try:
            body_len = len(response.content or b'')
        except Exception:
            body_len = len(body_text)

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
                return tracked_return('blocked', None, None if own_session else session, body_len)

            if not _looks_like_search_results(body_text):
                logging.warning(
                    f"⚠️ HTTP 200, но выдача eBay не распознана "
                    f"(bytes={body_len}, title={title!r})"
                )
                if own_session:
                    close_session(session)
                return tracked_return('http_error', None, None if own_session else session, body_len)

            logging.info(f"✅ УСПЕШНО c прокси {_proxy_log_name(proxy)}, профиль {profile['name']}")
            return tracked_return('success', body_text, session, body_len)

        if response.status_code == 403:
            logging.warning(f"🚫 eBay HTTP 403 для прокси {_proxy_log_name(proxy)}, профиль {profile['name']}")
            if own_session:
                close_session(session)
            return tracked_return('blocked', None, None if own_session else session, body_len)

        if response.status_code == 429:
            logging.warning(f"⏳ eBay HTTP 429 для прокси {_proxy_log_name(proxy)}, профиль {profile['name']}")
            if own_session:
                close_session(session)
            return tracked_return('rate_limited', None, None if own_session else session, body_len)

        if response.status_code == 407:
            logging.warning(f"🔐 Прокси требует авторизацию: {_proxy_log_name(proxy)}")
            if own_session:
                close_session(session)
            return tracked_return('proxy_error', None, None if own_session else session, body_len)

        logging.warning(
            f"⚠️ НЕУДАЧА: HTTP {response.status_code} "
            f"для прокси {_proxy_log_name(proxy)}, профиль {profile['name']}"
        )
        if own_session:
            close_session(session)
        return tracked_return('http_error', None, None if own_session else session, body_len)

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
            return tracked_return('profile_error', None, None)

        if own_session:
            close_session(session)

        if 'curl: (28)' in low or 'timed out' in low:
            return tracked_return('proxy_timeout', None, None if own_session else session)
        if 'curl: (60)' in low or 'certificate' in low or 'self signed' in low:
            return tracked_return('proxy_ssl', None, None if own_session else session)
        if (
            'connect tunnel failed' in low or
            'proxy connect aborted' in low or
            'wrong_version_number' in low or
            'wrong version number' in low or
            re.search(r'connect[^\n]*(?:response )?(?:400|405|500|501|502|503)', low)
        ):
            return tracked_return('proxy_rejected', None, None if own_session else session)
        return tracked_return('proxy_error', None, None if own_session else session)


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
        request_kind='discovery',
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



def _discard_webshare_handoff_ready(reason='stale'):
    global webshare_handoff_ready
    ready = None
    with webshare_handoff_state_lock:
        ready = webshare_handoff_ready
        webshare_handoff_ready = None
    if ready:
        close_session(ready[2])
        logging.debug(f"Webshare handoff candidate discarded: {reason}")


def _store_webshare_handoff_ready(proxy, profile, session):
    """Store one already-proven FREE Session for atomic adoption by main worker."""
    global webshare_handoff_ready
    if not proxy or session is None:
        close_session(session)
        return False
    current = fixed_proxy
    if not current or provider_manager.source_fast(current) != 'webshare':
        close_session(session)
        return False
    if provider_manager.source_fast(proxy) != 'free':
        close_session(session)
        return False
    with webshare_handoff_state_lock:
        old = webshare_handoff_ready
        webshare_handoff_ready = (proxy, profile, session, time.monotonic())
    if old:
        close_session(old[2])
    webshare_handoff_wakeup_event.set()
    logging.info(
        f"🟢 Webshare bridge: FREE replacement already proved eBay 200: {_proxy_log_name(proxy)}; "
        "switching before next main check"
    )
    return True


def _adopt_webshare_handoff_if_ready():
    """Make-before-break swap; never drops working Webshare before FREE is proven."""
    global fixed_proxy, fixed_profile, fixed_session, fixed_pair_since_monotonic, webshare_handoff_ready
    with webshare_handoff_state_lock:
        ready = webshare_handoff_ready
        if ready is None:
            return False
        webshare_handoff_ready = None
    proxy, profile, session, created = ready
    if (time.monotonic() - created) > WEBSHARE_HANDOFF_READY_TTL:
        close_session(session)
        return False

    adopted = False
    old_session = None
    with main_fixed_request_lock:
        if (
            fixed_proxy is not None
            and provider_manager.source_fast(fixed_proxy) == 'webshare'
            and provider_manager.source_fast(proxy) == 'free'
        ):
            old_session = fixed_session
            fixed_proxy = proxy
            fixed_profile = profile
            fixed_session = session
            fixed_pair_since_monotonic = time.monotonic()
            proxy_manager.mark_success(proxy)
            proxy_manager.clear_outage_memory()
            adopted = True
    if adopted:
        close_session(old_session)
        logging.info(
            f"♻️ Webshare bridge handoff complete: now using proven FREE {_proxy_log_name(proxy)}; "
            "metered Webshare traffic stopped without an outage"
        )
        wake_queued_auctions_for_new_fixed()
        _queue_restart_sticky_persist(proxy, force=True)
        return True
    close_session(session)
    return False


def webshare_handoff_worker():
    """Background FREE scout used only while Webshare is the working fixed proxy."""
    db_ready_event.wait()
    logging.info(
        f"🌉 Webshare bridge worker started: handoff after {WEBSHARE_HANDOFF_AFTER:.0f}s "
        f"(high-usage {WEBSHARE_HANDOFF_HIGH_USAGE_AFTER:.0f}s), "
        f"batch={WEBSHARE_HANDOFF_BATCH}, concurrency={WEBSHARE_HANDOFF_CONCURRENCY}"
    )
    while True:
        try:
            current = fixed_proxy
            # Handoff is an optimization, not a reason to compete with the main failover
            # for RAM. Under pressure/discovery we keep the proven Webshare fixed alive
            # and resume the free-scout as soon as the main path is stable.
            if main_discovery_active_event.is_set() or memory_pressure_event.is_set() or main_fixed_request_lock.locked():
                webshare_handoff_wakeup_event.wait(timeout=5.0)
                webshare_handoff_wakeup_event.clear()
                continue
            if is_paused or current is None or provider_manager.source_fast(current) != 'webshare':
                if current is None or provider_manager.source_fast(current) != 'webshare':
                    _discard_webshare_handoff_ready('fixed is not Webshare')
                webshare_handoff_wakeup_event.wait(timeout=5.0)
                webshare_handoff_wakeup_event.clear()
                continue

            started = fixed_pair_since_monotonic or time.monotonic()
            age = max(0.0, time.monotonic() - started)
            handoff_after = provider_manager.webshare_handoff_after_for_proxy(current)
            if age < handoff_after:
                webshare_handoff_wakeup_event.wait(timeout=min(5.0, handoff_after - age))
                webshare_handoff_wakeup_event.clear()
                continue

            with webshare_handoff_state_lock:
                ready_exists = webshare_handoff_ready is not None
            if ready_exists:
                webshare_handoff_wakeup_event.wait(timeout=5.0)
                webshare_handoff_wakeup_event.clear()
                continue

            profile = get_preferred_profile()
            if profile is None:
                webshare_handoff_wakeup_event.wait(timeout=WEBSHARE_HANDOFF_INTERVAL)
                webshare_handoff_wakeup_event.clear()
                continue

            excluded = {_proxy_host(current)}
            candidates = proxy_manager.get_free_handoff_candidates(
                WEBSHARE_HANDOFF_BATCH, excluded_hosts=excluded
            )
            if not candidates:
                proxy_manager.refresh_proxies(force=False, emergency=False)
                candidates = proxy_manager.get_free_handoff_candidates(
                    WEBSHARE_HANDOFF_BATCH, excluded_hosts=excluded
                )
            if not candidates:
                webshare_handoff_wakeup_event.wait(timeout=WEBSHARE_HANDOFF_INTERVAL)
                webshare_handoff_wakeup_event.clear()
                continue

            logging.info(
                f"🌉 Webshare bridge: quietly checking {len(candidates)} FREE replacement(s); "
                "current Webshare stays online"
            )
            executor = ThreadPoolExecutor(
                max_workers=min(WEBSHARE_HANDOFF_CONCURRENCY, len(candidates)),
                thread_name_prefix='webshare-handoff',
            )
            future_to_proxy = {
                executor.submit(
                    _probe_proxy, candidate, profile,
                    (PROBE_CONNECT_TIMEOUT, PROBE_READ_TIMEOUT),
                ): candidate
                for candidate in candidates
            }
            winner = None
            try:
                while future_to_proxy and winner is None:
                    done, _ = wait(tuple(future_to_proxy), timeout=1.0, return_when=FIRST_COMPLETED)
                    if not done:
                        if fixed_proxy != current or provider_manager.source_fast(fixed_proxy) != 'webshare':
                            break
                        continue
                    for future in done:
                        candidate = future_to_proxy.pop(future, None)
                        try:
                            result, _html, session = future.result()
                        except Exception:
                            result, session = 'proxy_error', None
                        if result == 'success' and winner is None:
                            # Do not abandon a stable metered bridge for a FREE proxy that
                            # happened to answer only once. Require consecutive eBay 200s
                            # in the SAME Session before make-before-break adoption.
                            confirmed = True
                            confirm_result = 'success'
                            for _confirm_idx in range(1, WEBSHARE_HANDOFF_CONFIRMATIONS):
                                if fixed_proxy != current or provider_manager.source_fast(fixed_proxy) != 'webshare':
                                    confirmed = False
                                    confirm_result = 'profile_error'  # cancellation, do not punish candidate
                                    break
                                time.sleep(WEBSHARE_HANDOFF_CONFIRM_DELAY)
                                if fixed_proxy != current or provider_manager.source_fast(fixed_proxy) != 'webshare':
                                    confirmed = False
                                    confirm_result = 'profile_error'
                                    break
                                confirm_result, _confirm_html, confirm_session = _make_request(
                                    candidate, profile, session=session,
                                    timeout=(PROBE_CONNECT_TIMEOUT, PROBE_READ_TIMEOUT),
                                    request_kind='discovery',
                                )
                                session = confirm_session
                                if confirm_result != 'success':
                                    confirmed = False
                                    break
                            if confirmed:
                                proxy_manager.mark_success(candidate)
                                logging.info(
                                    f"🟢 Handoff FREE confirmed {WEBSHARE_HANDOFF_CONFIRMATIONS}x eBay 200: "
                                    f"{_proxy_log_name(candidate)}"
                                )
                                winner = (candidate, profile, session)
                                break
                            result = confirm_result

                        close_session(session)
                        if result != 'profile_error':
                            # V6.35: the handoff scout is background/opportunistic. A transient
                            # timeout/error on a recently eBay-good FREE proxy is weak evidence
                            # and must not remove that proxy from PRIMARY failover for minutes.
                            # Keep hard signals (403/429, SSL, CONNECT reject, etc.) unchanged.
                            isolated = proxy_manager.mark_handoff_transient_failure(candidate, result)
                            if not isolated:
                                proxy_manager.mark_failure(
                                    candidate, result, reason=f'{result} during Webshare handoff scout'
                                )
                            _auction_proxy_soft_host_penalty(candidate, result)

                if winner is not None:
                    candidate, winner_profile, winner_session = winner
                    if fixed_proxy == current and provider_manager.source_fast(fixed_proxy) == 'webshare':
                        _store_webshare_handoff_ready(candidate, winner_profile, winner_session)
                    else:
                        close_session(winner_session)
            finally:
                for future, candidate in list(future_to_proxy.items()):
                    if not future.cancel():
                        future.add_done_callback(
                            lambda fut, p=candidate: _cleanup_late_probe_future(fut, p)
                        )
                executor.shutdown(wait=False, cancel_futures=True)

            webshare_handoff_wakeup_event.wait(timeout=WEBSHARE_HANDOFF_INTERVAL)
            webshare_handoff_wakeup_event.clear()
        except Exception as e:
            logging.warning(f"Webshare bridge worker: {e}")
            webshare_handoff_wakeup_event.wait(timeout=WEBSHARE_HANDOFF_INTERVAL)
            webshare_handoff_wakeup_event.clear()


def fetch_ebay_html_with_fixed_pair():
    global fixed_proxy, fixed_profile, fixed_session, fixed_pair_since_monotonic

    _adopt_webshare_handoff_if_ready()

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
                request_kind='fixed',
            )
        fixed_attempt_elapsed = time.monotonic() - fixed_attempt_started

        if result == 'success':
            fixed_session = returned_session
            proxy_manager.mark_success(old_proxy)
            record_ebay_success()
            _queue_restart_sticky_persist(old_proxy, force=False)
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
                    request_kind='recovery',
                )
            if retry_result == 'success':
                fixed_proxy = old_proxy
                fixed_profile = old_profile
                fixed_session = retry_session
                if fixed_pair_since_monotonic is None:
                    fixed_pair_since_monotonic = time.monotonic()
                proxy_manager.mark_success(old_proxy)
                record_ebay_success()
                _queue_restart_sticky_persist(old_proxy, force=True)
                logging.info("✅ Proxy восстановился после пересоздания session")
                return retry_html

            close_session(retry_session)
            result = retry_result

        close_session(fixed_session)
        fixed_session = None
        fixed_proxy = None
        fixed_profile = None
        fixed_pair_since_monotonic = None
        webshare_handoff_wakeup_event.set()

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
            WARM_STANDBY_TOTAL_LIMIT,
            excluded_hosts={_proxy_host(old_proxy)},
        )
        # HTTP won every discovery in the supplied production window. Keep SOCKS5 as
        # fallback, but let equally warm HTTP endpoints occupy the earliest slots.
        fast_standby_queue = (
            [p for p in fast_standby_queue if _proxy_scheme(p) in ('http', 'https')]
            + [p for p in fast_standby_queue if _proxy_scheme(p) == 'socks5']
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
    # максимум 6 при нормальном RSS; emergency/deep tiers по-прежнему расширяют сам пул.
    # Сначала расширяемся до ProxyScrape 3000 ms, а 4000 ms используем только как
    # последний deep-emergency tier. Hard cooldown никогда не снимаются.
    # V6.20: сначала обновляем управляемые источники. Ошибка их API полностью fail-open:
    # старый бесплатный ProxyScrape остаётся независимым fallback.
    # V6.25 trims old parser/curl arenas before starting a multi-session failover burst.
    _memory_maintenance('before discovery', force=True)
    provider_manager.refresh_all(force=False, include_stats=False)
    # Do not delay failover on third-party list APIs. If background SmartReserve already
    # refreshed them, they are ready immediately; otherwise this daemon refresh becomes
    # available to later rolling batches while the normal discovery is already running.
    external_free_manager.refresh_all_async(force=False)

    # На свежем старте сначала ОБЯЗАТЕЛЬНО загружаем обычный Render PROXY_LIST
    # (timeout=1500). Emergency 3000 не имеет права включаться на пустом 0/0 pool.
    if not proxy_manager.standard_pool_loaded():
        proxy_manager.refresh_proxies(force=True, emergency=False)

    # V6.30: on a fresh leader process, try the exact last eBay-proven endpoint ONCE
    # before broad discovery. Render deploys discard RAM state, but the old process may
    # have confirmed this proxy only seconds earlier. A dead/stale candidate gets only a
    # short bounded attempt and then normal 4→5→6 discovery proceeds unchanged.
    sticky_record = _consume_restart_sticky_candidate()
    if sticky_record is not None:
        sticky_proxy = _resolve_restart_sticky_proxy(sticky_record)
        sticky_profile = get_preferred_profile()
        if sticky_proxy is None:
            logging.info(
                "♻️ Restart-sticky: managed endpoint больше не доступен в свежем provider snapshot; "
                "переходим к обычному discovery"
            )
        elif sticky_profile is None:
            logging.warning("♻️ Restart-sticky: нет поддерживаемого browser-profile; обычный discovery")
        else:
            sticky_started = time.monotonic()
            logging.info(
                f"♻️ Restart-sticky FIRST: проверяем последний eBay-good proxy "
                f"{_proxy_log_name(sticky_proxy)} перед новым discovery"
            )
            with main_fixed_request_lock:
                sticky_result, sticky_html, sticky_session = _make_request(
                    sticky_proxy,
                    sticky_profile,
                    session=None,
                    timeout=(RESTART_STICKY_CONNECT_TIMEOUT, RESTART_STICKY_READ_TIMEOUT),
                    request_kind='recovery',
                )
            sticky_elapsed = time.monotonic() - sticky_started
            if sticky_result == 'success':
                fixed_proxy = sticky_proxy
                fixed_profile = sticky_profile
                fixed_session = sticky_session
                fixed_pair_since_monotonic = time.monotonic()
                proxy_manager.mark_success(sticky_proxy)
                proxy_manager.clear_outage_memory()
                record_ebay_success()
                _queue_restart_sticky_persist(sticky_proxy, force=True)
                proxy_preflight_wakeup_event.set()
                wake_queued_auctions_for_new_fixed()
                if provider_manager.source_fast(sticky_proxy) == 'webshare':
                    webshare_handoff_wakeup_event.set()
                logging.info(
                    f"✅ Restart-sticky восстановлен за {sticky_elapsed:.1f} сек.: "
                    f"{_proxy_log_name(sticky_proxy)}; широкий discovery не понадобился"
                )
                return sticky_html
            close_session(sticky_session)
            if sticky_result != 'profile_error':
                proxy_manager.mark_failure(
                    sticky_proxy, sticky_result, reason=f'restart-sticky {sticky_result}'
                )
                _auction_proxy_soft_host_penalty(sticky_proxy, sticky_result)
            logging.info(
                f"♻️ Restart-sticky не подтвердился ({sticky_result}) за {sticky_elapsed:.1f} сек.; "
                "сразу запускаем обычный discovery"
            )

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

    # Build the executor first, then advertise discovery ownership. If executor
    # construction itself ever failed, background workers must not remain paused by a
    # stale event. From this point main failover owns the network/memory budget.
    executor = ThreadPoolExecutor(
        max_workers=PROBE_MAX_CONCURRENCY,
        thread_name_prefix='proxy-probe',
    )
    main_discovery_active_event.set()
    future_to_proxy = {}
    last_concurrency_logged = None

    def desired_concurrency():
        nonlocal last_concurrency_logged
        elapsed_now = time.monotonic() - started
        rss_now = _memory_rss_mb() if MEMORY_GUARD_ENABLED else None
        # Hard safety margin: if RSS is already extremely close to Render's 512 MB cap,
        # do not create more simultaneous full-page curl responses. Existing in-flight work
        # can still finish; this does NOT stop the main failover.
        if rss_now is not None and rss_now >= MEMORY_EMERGENCY_MB:
            desired = 2
            reason = f'emergency-memory RSS≈{rss_now:.0f}MB'
        # Real protective mode starts at 450 MB by default. Check RSS directly as well as
        # the background Event so a fast discovery spike cannot wait for the next 10-sec guard tick.
        elif (rss_now is not None and rss_now >= MEMORY_HIGH_MB) or memory_pressure_event.is_set():
            desired = PROBE_CONCURRENCY
            reason = f'high-memory RSS≈{rss_now:.0f}MB' if rss_now is not None else 'memory-pressure'
        # Do not enter 7/8-worker burst when RSS is already elevated. This intermediate
        # guard preserves a wide margin below the 450/485 MB protective thresholds.
        elif rss_now is not None and rss_now >= PROBE_BURST_MEMORY_CEILING_MB:
            desired = PROBE_DEEP_CONCURRENCY if elapsed_now >= PROBE_DEEP_ESCALATE_AFTER else (
                PROBE_ESCALATED_CONCURRENCY if elapsed_now >= PROBE_ESCALATE_AFTER else PROBE_CONCURRENCY
            )
            reason = f'burst-memory-cap RSS≈{rss_now:.0f}MB'
        elif elapsed_now >= PROBE_MAX_ESCALATE_AFTER:
            desired = PROBE_MAX_CONCURRENCY
            reason = f'elapsed={elapsed_now:.1f}s'
        elif elapsed_now >= PROBE_BURST_ESCALATE_AFTER:
            desired = PROBE_BURST_CONCURRENCY
            reason = f'elapsed={elapsed_now:.1f}s'
        elif elapsed_now >= PROBE_DEEP_ESCALATE_AFTER:
            desired = PROBE_DEEP_CONCURRENCY
            reason = f'elapsed={elapsed_now:.1f}s'
        elif elapsed_now >= PROBE_ESCALATE_AFTER:
            desired = PROBE_ESCALATED_CONCURRENCY
            reason = f'elapsed={elapsed_now:.1f}s'
        else:
            desired = PROBE_CONCURRENCY
            reason = f'elapsed={elapsed_now:.1f}s'

        if desired != last_concurrency_logged:
            if last_concurrency_logged is not None:
                logging.info(
                    f"⚡ Discovery concurrency {last_concurrency_logged}→{desired} ({reason})"
                )
            last_concurrency_logged = desired
        return desired

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
        elapsed_for_mix = time.monotonic() - started
        preferred_scheme = (
            'socks5'
            if elapsed_for_mix >= SOCKS_MIX_DELAY and desired >= 3 and inflight_socks == 0
            else None
        )
        inflight_hosts = {_proxy_host(p) for p in future_to_proxy.values()}
        batch = []
        batch_kinds = {}

        if need > 0:
            # 0) V6.22 continuity-first: reserve exactly ONE immediate Webshare slot
            # before draining warm FREE. The latest log contained two full 75-second
            # no-winner cycles while managed addresses were unavailable; Webshare had
            # the best distinct-IP success rate, and a single rescue probe costs little
            # compared with running it as fixed for hours.
            if WEBSHARE_FIRST_BATCH and attempts == 0 and len(batch) < need:
                active_ws_accounts = provider_manager.usable_webshare_account_count()
                unique_ws_hosts = provider_manager.webshare_unique_host_count()
                # Multiple keys often expose the SAME 10 exit IPs. Treat extra accounts
                # as bandwidth capacity, not artificial IP diversity. With two funded
                # accounts we can afford two different Webshare exits early, but never
                # consume 3/4 first-wave workers just because more credentials exist.
                ws_first_limit = min(
                    active_ws_accounts,
                    unique_ws_hosts,
                    2,
                    max(1, need - 1) if need > 1 else 1,
                )
                ws_rescues = proxy_manager.get_webshare_rescue_candidates(
                    ws_first_limit,
                    excluded_hosts=(
                        tried_hosts
                        | inflight_hosts
                        | {_proxy_host(p) for p in batch}
                    ),
                )
                for ws_rescue in ws_rescues:
                    if len(batch) >= need:
                        break
                    batch.append(ws_rescue)
                    batch_kinds[ws_rescue] = 'Webshare bridge'

            # V6.28: while the shared Webshare 403-circuit is active, allow exactly one
            # controlled half-open probe roughly once per minute. This is intentionally
            # independent from normal Webshare availability: the whole point is to detect
            # recovery before the full 5-minute circuit expires.
            if len(batch) < need:
                ws_half_open = proxy_manager.get_webshare_half_open_candidate(
                    excluded_hosts=(
                        tried_hosts
                        | inflight_hosts
                        | {_proxy_host(p) for p in batch}
                    ),
                )
                if ws_half_open is not None:
                    batch.append(ws_half_open)
                    batch_kinds[ws_half_open] = 'Webshare half-open'
                    logging.info(
                        f"🟡 Webshare half-open recovery probe: {_proxy_log_name(ws_half_open)}"
                    )

            # V6.31: one controlled Premium recovery probe while its 403 circuit is active.
            if len(batch) < need:
                premium_half_open = proxy_manager.get_premium_half_open_candidate(
                    excluded_hosts=(
                        tried_hosts | inflight_hosts | {_proxy_host(p) for p in batch}
                    ),
                )
                if premium_half_open is not None:
                    batch.append(premium_half_open)
                    batch_kinds[premium_half_open] = 'Premium half-open'
                    logging.info(
                        f"🟣 Premium half-open recovery probe: {_proxy_log_name(premium_half_open)}"
                    )

            # Fill the remaining first-wave workers from already proven warm reserve.
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

            # V6.36: Proxio is warmed as reserve while Premium works. It joins direct eBay
            # discovery after the first escalation (~8s), OR immediately when Premium is
            # unavailable. It consumes the SAME external slot; global workers stay 4→5→6→7→8.
            external_slot_cap = (
                EXTERNAL_FREE_ESCALATED_SLOTS
                if elapsed_for_mix >= PROBE_ESCALATE_AFTER
                else EXTERNAL_FREE_EARLY_SLOTS
            )
            external_need = min(max(0, need - len(batch)), external_slot_cap)
            if external_need > 0:
                premium_usable = provider_manager.has_usable_premium()
                include_proxio = (elapsed_for_mix >= PROBE_ESCALATE_AFTER) or (not premium_usable)
                external_rows = proxy_manager.get_external_free_candidates(
                    max_total=external_need,
                    excluded_hosts=(
                        tried_hosts | inflight_hosts | {_proxy_host(p) for p in batch}
                    ),
                    include_proxio=include_proxio,
                    prefer_proxio=(not premium_usable),
                )
                for ext_proxy, ext_source in external_rows:
                    if len(batch) >= need:
                        break
                    batch.append(ext_proxy)
                    external_free_manager.claim_credit(ext_proxy, ext_source)
                    batch_kinds[ext_proxy] = {
                        'hproxy': 'HProxy probe',
                        'databay': 'Databay probe',
                        'proxio': 'Proxio reserve probe',
                    }.get(ext_source, 'External probe')

            remaining_need = need - len(batch)
            if remaining_need > 0:
                elapsed_for_provider = time.monotonic() - started
                first_wave_has_webshare = any(
                    kind == 'Webshare bridge' for kind in batch_kinds.values()
                )
                allow_webshare = (
                    (not first_wave_has_webshare)
                    and (
                        (WEBSHARE_FIRST_BATCH and provider_manager.has_usable_webshare())
                        or elapsed_for_provider >= WEBSHARE_UNLOCK_AFTER
                        or attempts >= WEBSHARE_UNLOCK_ATTEMPTS
                    )
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
            if source == 'free' and kind not in ('HProxy probe', 'Databay probe'):
                external_free_manager.claim_credit(proxy, 'proxyscrape')
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

    try:
        submit_more()
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
                fixed_pair_since_monotonic = time.monotonic()
                record_ebay_success()
                _queue_restart_sticky_persist(winner_proxy, force=True)
                proxy_preflight_wakeup_event.set()
                # A newly proven Session is also a new chance for durable auction jobs.
                # Do not leave them sleeping behind an old 30/60-second retry deadline.
                wake_queued_auctions_for_new_fixed()
                if provider_manager.source_fast(winner_proxy) == 'webshare':
                    webshare_handoff_wakeup_event.set()
                discovery_elapsed = time.monotonic() - started
                provider_manager.record_winner(winner_proxy, discovery_elapsed, attempts)
                external_free_manager.record_winner(winner_proxy)
                winner_source = provider_manager.source_fast(winner_proxy)
                if winner_source == 'free':
                    winner_source = external_free_manager.credit_source_fast(winner_proxy) or 'proxyscrape'
                logging.info(
                    f"✅ Найдена рабочая пара: proxy {_proxy_log_name(winner_proxy)}, "
                    f"source={winner_source}, профиль {profile['name']}; "
                    f"проверено {attempts} proxy за {discovery_elapsed:.1f} сек."
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
        main_discovery_active_event.clear()
        _memory_maintenance('after discovery', force=False)

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
    soup = None
    try:
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
    finally:
        # BeautifulSoup creates a large cyclic object graph from a ~1.6 MB eBay page.
        # Explicitly breaking it is important on a 512 MB Render instance; waiting for
        # a later cyclic-GC pass can leave several generations resident at once.
        if soup is not None:
            try:
                soup.decompose()
            except Exception:
                pass


def perform_initial_snapshot():
    logging.info("Начальный снимок...")
    html = fetch_ebay_html_with_retry()
    if not html:
        return False
    items = parse_ebay_listings(html, max_items=MAX_ITEMS)
    html = None
    _memory_maintenance('initial snapshot', force=False)
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
    html = None
    _memory_maintenance('main parse', force=False)
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
                or main_discovery_active_event.is_set()
                or memory_pressure_event.is_set()
                or main_fixed_request_lock.locked()
            ):
                proxy_preflight_wakeup_event.wait(timeout=PROXY_PREFLIGHT_WARM_INTERVAL)
                proxy_preflight_wakeup_event.clear()
                continue

            # Managed provider lists are metadata/API calls only; they do not consume proxy bandwidth.
            provider_manager.refresh_all(force=False)
            # External metadata/API feeds only. Proxio consumes one quota-controlled API
            # call per due refresh; no external refresh runs on the normal fixed eBay path.
            external_free_manager.refresh_all(force=False)

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
                # Reserve a small part of the existing preflight batch for HProxy/Databay/Proxio.
                # These checks are neutral CONNECT+TLS only (no eBay), so bad external
                # endpoints are filtered before they can consume scarce discovery slots.
                external_candidates = proxy_manager.get_external_quality_preflight_candidates(
                    min(EXTERNAL_FREE_PREFLIGHT_SLOTS, batch_limit),
                    excluded_hosts=excluded,
                )
                external_hosts = {_proxy_host(p) for p in external_candidates if _proxy_host(p)}
                base_limit = max(0, batch_limit - len(external_candidates))
                candidates = proxy_manager.get_quality_preflight_candidates(
                    base_limit,
                    excluded_hosts=excluded | external_hosts,
                )
                candidates.extend(external_candidates)
                external_candidate_set = set(external_candidates)

                checked = 0
                ok_count = 0
                external_checked = 0
                external_ok = 0
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
                        for future, checked_proxy in future_map.items():
                            checked += 1
                            is_external = checked_proxy in external_candidate_set
                            if is_external:
                                external_checked += 1
                            try:
                                ok, reason = future.result()
                            except Exception:
                                ok, reason = False, 'proxy_error'
                            if ok:
                                ok_count += 1
                                if is_external:
                                    external_ok += 1
                            else:
                                reason = reason or 'proxy_error'
                                reason_counts[reason] = reason_counts.get(reason, 0) + 1

                quality_after, tcp_after = proxy_manager.warm_reserve_stats()
                # Логируем и maintenance-проходы: по нему можно реально оценивать качество пула.
                if checked:
                    failure_summary = ', '.join(
                        f"{k}={v}" for k, v in sorted(reason_counts.items())
                    ) or 'нет'
                    ext_suffix = (
                        f", external_preflight={external_checked}/{external_ok}"
                        if external_checked else ""
                    )
                    logging.info(
                        f"🧪 Smart reserve: HTTPS-ready={quality_after}, TCP-only={tcp_after}; "
                        f"проверено={checked}, quality_ok={ok_count}{ext_suffix}, failures[{failure_summary}]"
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
    while True:
        try:
            db_empty = is_db_empty()
            break
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            logging.warning(
                f"⚠️ Aiven временно недоступен при старте main worker: {e}; "
                f"повтор через {DB_MAIN_RETRY_WAIT:.0f} сек."
            )
            time.sleep(DB_MAIN_RETRY_WAIT)

    if db_empty:
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
        "\n🇬🇧 eBay UK monitor v6.36 ProxioReserveQuotaSafe+HandoffSafe+ReliableTelegram+AdaptiveBurst работает." +
        seen_line +
        "\nКоманды: /stop /start /list (/auctions) /delauction НОМЕР_ЛОТА"
        "\nМожно отправить ссылку на eBay-аукцион — сохраню точное время и напомню заранее."
        "\nКнопка «📋 Аукционы» открывает тот же список, что и /auctions.",
        reply_markup=bot_main_reply_keyboard(),
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
                outage_elapsed, _, had_success = _connection_outage_snapshot()
                if had_success:
                    logging.info(
                        f"⚠️ Рабочий proxy пока не найден. Новый цикл через {wait:.1f} секунд; "
                        f"суммарно без валидного eBay HTTP 200 уже {outage_elapsed:.1f} сек. "
                        f"({outage_elapsed / 60.0:.1f} мин.)"
                    )
                else:
                    logging.info(
                        f"⚠️ Рабочий proxy пока не найден. Новый цикл через {wait:.1f} секунд."
                    )
            time.sleep(wait)
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            # eBay/proxy may be perfectly healthy. A transient DB outage must not mark or
            # rotate the fixed proxy; simply wait for Aiven and retry the normal cycle.
            logging.warning(
                f"⚠️ Временный сбой Aiven в основном цикле: {e}. "
                f"Рабочий proxy не меняем; повтор через {DB_MAIN_RETRY_WAIT:.0f} сек."
            )
            time.sleep(DB_MAIN_RETRY_WAIT)
        except Exception as e:
            logging.error(f"Ошибка в основном цикле: {e}", exc_info=True)
            time.sleep(5)

def start_leader_workers():
    """Инициализирует БД и запускает фоновые задачи только в leader-instance."""
    init_db()
    initialize_seen_count_cache()
    # Durable auction rows live in PostgreSQL. Recovery acceleration is an optimization:
    # if this diagnostics UPDATE happens to fail during a brief Aiven hiccup, do NOT block
    # the whole bot startup; the queue rows are still durable and its worker will retry.
    try:
        recover_auction_queue_after_startup()
    except Exception as e:
        logging.warning(f"⚠️ Не удалось ускорить auction queue при старте: {e}; durable rows сохранены")
        auction_link_wakeup_event.set()
    # Load credential-free restart hint before main worker starts. Provider credentials,
    # when needed, are resolved later from the freshly fetched API snapshot.
    _load_restart_sticky_candidate()
    db_ready_event.set()
    leader_active_event.set()
    logging.info("👑 Эта Render-копия стала leader; запускаем фоновые worker-ы")
    logging.info(
        "🌐 Multi-provider v6.36: "
        f"ProxyScrape Premium={'ON' if PROXYSCRAPE_PREMIUM_API_KEY else 'OFF'}, "
        f"Webshare={'ON (' + str(len(WEBSHARE_API_KEYS)) + ' account(s))' if WEBSHARE_API_KEYS else 'OFF'}, "
        f"Webshare first-batch={'ON (1-2 unique-host slots; extra keys=bandwidth)' if WEBSHARE_FIRST_BATCH else 'OFF'}, "
        f"bridge-handoff={WEBSHARE_HANDOFF_AFTER:.0f}s, warm-first-wave={WARM_STANDBY_TOTAL_LIMIT}, "
        f"ws403-circuit={WEBSHARE_BLOCK_CIRCUIT_STREAK}, "
        f"premium403-throttle/circuit={PREMIUM_BLOCK_THROTTLE_STREAK}/{PREMIUM_BLOCK_CIRCUIT_STREAK}"
    )
    proxio_calls_day = int((86400 + PROXIO_FREE_REFRESH - 1) // PROXIO_FREE_REFRESH) if PROXIO_FREE_ENABLED else 0
    proxio_calls_per_key = (proxio_calls_day / len(PROXIO_API_KEYS)) if PROXIO_API_KEYS else 0.0
    logging.info(
        f"🧭 External sources v6.36: HProxy={'ON' if HPROXY_FREE_ENABLED else 'OFF'} "
        f"(HTTPS, elite+anonymous, cap={HPROXY_FREE_ELITE_LIMIT + HPROXY_FREE_ANON_LIMIT}), "
        f"Databay={'ON' if DATABAY_FREE_ENABLED else 'OFF'} "
        f"(HTTPS strict+fast, elite+anonymous, cap={DATABAY_FREE_ELITE_LIMIT + DATABAY_FREE_ANON_LIMIT}), "
        f"Proxio={'ON (' + str(len(PROXIO_API_KEYS)) + ' key(s))' if PROXIO_FREE_ENABLED else 'OFF'} "
        f"(Elite HTTPS, cap={PROXIO_SNAPSHOT_LIMIT}, refresh={PROXIO_FREE_REFRESH}s, "
        f"designed≈{proxio_calls_day} calls/day total≈{proxio_calls_per_key:.1f}/key); "
        f"eBay slots={EXTERNAL_FREE_EARLY_SLOTS} early/{EXTERNAL_FREE_ESCALATED_SLOTS} escalated, "
        f"neutral_preflight={EXTERNAL_FREE_PREFLIGHT_SLOTS}, global ceiling={PROBE_MAX_CONCURRENCY}"
    )
    logging.info(
        f"⚡ Adaptive discovery v6.36: {PROBE_CONCURRENCY}→{PROBE_ESCALATED_CONCURRENCY}→"
        f"{PROBE_DEEP_CONCURRENCY}→{PROBE_BURST_CONCURRENCY}→{PROBE_MAX_CONCURRENCY} workers "
        f"at ~0/{PROBE_ESCALATE_AFTER:.0f}/{PROBE_DEEP_ESCALATE_AFTER:.0f}/"
        f"{PROBE_BURST_ESCALATE_AFTER:.0f}/{PROBE_MAX_ESCALATE_AFTER:.0f}s; "
        f"7/8-worker burst only while RSS<{PROBE_BURST_MEMORY_CEILING_MB} MB"
    )
    rss = _memory_rss_mb()
    logging.info(
        f"🧠 MemorySafe: RSS≈{rss:.0f} MB, soft/high/emergency={MEMORY_SOFT_MB}/{MEMORY_HIGH_MB}/{MEMORY_EMERGENCY_MB} MB; "
        f"background TLS workers={PROXY_PREFLIGHT_WARM_CONCURRENCY}" if rss is not None else
        f"🧠 MemorySafe enabled: soft/high/emergency={MEMORY_SOFT_MB}/{MEMORY_HIGH_MB}/{MEMORY_EMERGENCY_MB} MB"
    )

    threading.Thread(target=telegram_listener, daemon=True, name='telegram-listener').start()
    logging.info("📨 Telegram intake v6.36: text+caption+URL/text_link entities; eBay-сообщения больше не игнорируются молча")
    logging.info(f"🌉 Handoff-safe v6.36: recent-good transient scout failures are isolated for {WEBSHARE_HANDOFF_TRANSIENT_COOLDOWN:.0f}s; main cooldown untouched")
    threading.Thread(target=connection_watchdog, daemon=True, name='connection-watchdog').start()
    threading.Thread(target=memory_guard_worker, daemon=True, name='memory-guard-worker').start()
    threading.Thread(target=restart_sticky_persist_worker, daemon=True, name='restart-sticky-persist').start()
    threading.Thread(target=proxy_preflight_warm_worker, daemon=True, name='proxy-preflight-worker').start()
    threading.Thread(target=webshare_handoff_worker, daemon=True, name='webshare-handoff-worker').start()
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
    return f"eBay бот работает (Великобритания, adaptive parallel UK v6.36 ProxioReserveQuotaSafe+HandoffSafe+ReliableTelegram+AdaptiveBurst, {role})"


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
