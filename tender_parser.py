import time
import logging
import json
import re
import tempfile
import shutil
import uuid
import atexit
import signal
import os
import sys
from typing import Dict, Optional, List, Any
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException, WebDriverException
from utils import extract_products_from_excel, save_results_into_tender_format
import subprocess
import requests
import zipfile
import io
from pathlib import Path
SEARCH_URL_TEMPLATE = "https://market.yandex.ru/search?text={query}"


def _normalize_search_term(search_term: str, max_len: int = 120) -> str:
    """Нормализует строку поиска перед вводом в маркет."""
    cleaned = re.sub(r"\s+", " ", str(search_term or "")).strip()
    return cleaned[:max_len]


def _perform_direct_search_navigation(driver, search_term: str) -> bool:
    """Fallback: выполняет прямой переход на URL поиска."""
    normalized = _normalize_search_term(search_term)
    if not normalized:
        return False

    try:
        encoded_query = requests.utils.quote(normalized)
        driver.get(SEARCH_URL_TEMPLATE.format(query=encoded_query))
        time.sleep(1.2)
        return "search" in driver.current_url
    except Exception as e:
        logger.warning(f"Не удалось перейти по прямому URL поиска: {e}")
        return False

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Глобальные переменные для управления парсингом и автосохранения
STOP_PARSING = False
CREATED_PROFILES = set()
CURRENT_DATAFRAME = None
CURRENT_OUTPUT_FILE = None
CURRENT_INPUT_FILE = None

def setup_signal_handlers():
    """Настройка обработчиков сигналов для автосохранения при завершении"""
    def signal_handler(signum, frame):
        global STOP_PARSING
        logger.info(f"Получен сигнал завершения ({signum}), выполняю автосохранение...")
        STOP_PARSING = True
        force_save_results()
        cleanup_profiles()
        logger.info("Автосохранение завершено, выход из программы")
        os._exit(0)

    # Обработчики для Windows и Unix
    try:
        signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
        signal.signal(signal.SIGTERM, signal_handler)  # Terminate
        if hasattr(signal, 'SIGBREAK'):  # Windows
            signal.signal(signal.SIGBREAK, signal_handler)
    except Exception as e:
        logger.warning(f"Не удалось установить обработчики сигналов: {e}")

def force_save_results():
    """Принудительное сохранение результатов при завершении"""
    global CURRENT_DATAFRAME, CURRENT_OUTPUT_FILE, CURRENT_INPUT_FILE

    if CURRENT_DATAFRAME is not None and CURRENT_OUTPUT_FILE and CURRENT_INPUT_FILE:
        try:
            # Считаем сколько товаров обработано
            processed = len([r for r in CURRENT_DATAFRAME['цена'] if r and r not in ['', 'ОШИБКА']])
            total = len(CURRENT_DATAFRAME)

            # ИСПОЛЬЗУЕМ НОВУЮ ФУНКЦИЮ ТЕНДЕРНОГО ФОРМАТА
            save_results_into_tender_format(CURRENT_INPUT_FILE, CURRENT_OUTPUT_FILE, CURRENT_DATAFRAME)
            logger.info(f"ЭКСТРЕННОЕ СОХРАНЕНИЕ ТЕНДЕРА: обработано {processed}/{total} товаров в {CURRENT_OUTPUT_FILE}")
        except Exception as e:
            logger.error(f"Ошибка экстренного сохранения: {e}")
    else:
        logger.info("Нет данных для экстренного сохранения")

def stop_all_parsing():
    """Останавливает все процессы парсинга"""
    global STOP_PARSING
    STOP_PARSING = True
    logger.info("Получен сигнал остановки парсинга")

def cleanup_single_profile(profile_path: str) -> bool:
    """Аккуратно очищает один профиль Edge после закрытия драйвера"""
    if not profile_path or not os.path.exists(profile_path):
        return False

    try:
        time.sleep(0.3)

        try:
            import psutil
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if proc.info['name'] and 'msedge' in proc.info['name'].lower():
                        if proc.info['cmdline']:
                            cmdline = ' '.join(proc.info['cmdline'])
                            if profile_path in cmdline:
                                return False
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    continue
        except ImportError:
            time.sleep(0.5)

        shutil.rmtree(profile_path, ignore_errors=True)
        success = not os.path.exists(profile_path)

        return success

    except Exception as e:
        return False

def cleanup_profiles():
    """Глобальная очистка всех профилей"""
    global CREATED_PROFILES
    cleanup_count = 0
    for profile_path in CREATED_PROFILES.copy():
        try:
            if os.path.exists(profile_path):
                shutil.rmtree(profile_path, ignore_errors=True)
                cleanup_count += 1
        except:
            pass
    CREATED_PROFILES.clear()
    if cleanup_count > 0:
        logger.info(f"Очищено {cleanup_count} профилей Edge")

atexit.register(cleanup_profiles)

def kill_zombie_edges():
    """Убивает Edge процессы"""
    print("Закрываю Edge процессы...")
    try:
        import psutil
        killed_count = 0
        for p in psutil.process_iter(['pid', 'name']):
            if p.info['name'] and 'msedge' in p.info['name'].lower():
                try:
                    p.terminate()
                    killed_count += 1
                except:
                    pass
        if killed_count > 0:
            print(f"Закрыто {killed_count} процессов")
    except:
        pass

EDGE_VERSION = "144.0.3719.82"

def ensure_edgedriver(driver_dir: Path) -> Path:
    driver_dir.mkdir(parents=True, exist_ok=True)
    driver_path = driver_dir / "msedgedriver.exe"

    # Берём основную часть версии для скачивания
    edge_major = EDGE_VERSION.split('.')[0]

    # Проверяем, есть ли драйвер и совпадает ли версия
    if driver_path.exists():
        try:
            out = subprocess.check_output(
                f'"{driver_path}" --version', shell=True, text=True
            )
            if edge_major in out:
                return driver_path
        except Exception:
            pass

    # Скачиваем нужный драйвер
    url = f"https://msedgedriver.azureedge.net/{EDGE_VERSION}/edgedriver_win64.zip"
    r = requests.get(url, timeout=30)
    r.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        z.extract("msedgedriver.exe", driver_dir)

    return driver_path

def _get_chrome_major_version() -> str:
    output = subprocess.check_output(
        r'reg query "HKLM\SOFTWARE\Google\Chrome\BLBeacon" /v version',
        shell=True,
        text=True
    )
    return re.search(r'(\d+)\.', output).group(1)

def ensure_chromedriver(driver_dir: Path) -> Path:
    driver_dir.mkdir(parents=True, exist_ok=True)
    driver_path = driver_dir / "chromedriver.exe"

    chrome_major = _get_chrome_major_version()

    if driver_path.exists():
        try:
            out = subprocess.check_output(
                f'"{driver_path}" --version', shell=True, text=True
            )
            if chrome_major in out:
                return driver_path
        except Exception:
            pass

    # Получаем актуальную версию Chrome for Testing
    versions_url = "https://googlechromelabs.github.io/chrome-for-testing/latest-patch-versions-per-build.json"
    data = requests.get(versions_url, timeout=30).json()

    full_version = data["builds"][chrome_major]["version"]

    download_url = (
        f"https://storage.googleapis.com/chrome-for-testing-public/"
        f"{full_version}/win64/chromedriver-win64.zip"
    )

    r = requests.get(download_url, timeout=30)
    r.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        for name in z.namelist():
            if name.endswith("chromedriver.exe"):
                z.extract(name, driver_dir)
                extracted = driver_dir / name
                extracted.replace(driver_path)
                break

    return driver_path




def create_driver(
    headless: bool = True,
    driver_path: Optional[str] = None,
    use_auth: bool = False,
    browser: str = "edge"
):
    """
    Создание драйвера Edge / Chrome с автоподбором WebDriver
    """
    global CREATED_PROFILES

    profile_dir = None

    if browser == "edge":
        options = webdriver.EdgeOptions()
    elif browser == "chrome":
        options = webdriver.ChromeOptions()
    else:
        raise ValueError("browser должен быть 'edge' или 'chrome'")

    common_args = [
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--disable-extensions",
        "--disable-plugins",
        "--disable-web-security",
        "--no-default-browser-check",
        "--no-first-run",
        "--disable-default-apps",
        "--disable-sync",
        "--disable-logging",
        "--log-level=3",
        "--silent",
    ]

    for arg in common_args:
        options.add_argument(arg)

    if use_auth:
        timestamp = int(time.time() * 1000)
        worker_id = uuid.uuid4().hex[:8]
        app_dir = Path.home() / ".yandex_parser_auth"
        app_dir.mkdir(exist_ok=True)

        profile_dir = app_dir / f"{browser}_profile_{worker_id}_{timestamp}"
        profile_dir.mkdir(parents=True, exist_ok=True)
        options.add_argument(f"--user-data-dir={profile_dir}")
        CREATED_PROFILES.add(str(profile_dir))
    else:
        temp_dir = tempfile.mkdtemp(prefix=f"{browser}_temp_{uuid.uuid4().hex[:8]}_")
        options.add_argument(f"--user-data-dir={temp_dir}")
        CREATED_PROFILES.add(temp_dir)


    if headless:
        options.add_argument("--headless=new")
        options.add_argument("--window-size=1280,800")

    try:
        base_dir = Path(__file__).parent / "browserdriver"
        base_dir.mkdir(exist_ok=True)

        custom_driver = Path(driver_path).expanduser() if driver_path else None
        if custom_driver and not custom_driver.exists():
            raise FileNotFoundError(f"Не найден указанный драйвер: {custom_driver}")

        if browser == "edge":
            driver_exe = custom_driver if custom_driver else ensure_edgedriver(base_dir)
            service = Service(str(driver_exe))
            driver = webdriver.Edge(service=service, options=options)

        else:  # chrome
            from selenium.webdriver.chrome.service import Service as ChromeService
            driver_exe = custom_driver if custom_driver else ensure_chromedriver(base_dir)
            service = ChromeService(str(driver_exe))
            driver = webdriver.Chrome(service=service, options=options)

        driver.set_page_load_timeout(15)
        driver.implicitly_wait(3)
        driver.profile_path = str(profile_dir) if profile_dir else temp_dir

        return driver

    except Exception as e:
        if profile_dir and str(profile_dir) in CREATED_PROFILES:
            try:
                shutil.rmtree(profile_dir, ignore_errors=True)
                CREATED_PROFILES.discard(str(profile_dir))
            except:
                pass

        logger.error(f"Ошибка создания драйвера ({browser}): {e}")
        raise



def load_cookies_for_auth(driver):
    """ЗАГРУЗКА COOKIES ИЗ ПАПКИ ПРИЛОЖЕНИЯ (ИЗМЕНЕНО ТОЛЬКО ЭТО)"""
    from pathlib import Path

    if STOP_PARSING:
        return False

    # Определяем директорию приложения (.exe или .py)
    if getattr(sys, 'frozen', False):
        app_dir = Path(sys.executable).parent
    else:
        app_dir = Path(os.path.abspath(__file__)).parent

    # Вариант 1: cookies.json в корне папки приложения
    cookies_file = app_dir / "cookies.json"

    # Вариант 2: если нет в корне, ищем в .yandex_parser_auth (для совместимости)
    if not cookies_file.exists():
        cookies_file = Path.home() / ".yandex_parser_auth" / "cookies.json"

    if not cookies_file.exists():
        logger.warning(f"Cookies НЕ найдены")
        return False

    try:
        with open(cookies_file, 'r', encoding='utf-8') as f:
            cookies_data = json.loads(f.read().strip())

        if isinstance(cookies_data, list):
            cookies = cookies_data
        elif isinstance(cookies_data, dict) and 'cookies' in cookies_data:
            cookies = cookies_data['cookies']
        else:
            return False

        driver.get("https://market.yandex.ru")
        time.sleep(0.5)

        loaded_count = 0
        for cookie in cookies:
            if STOP_PARSING:
                break

            try:
                if not isinstance(cookie, dict) or 'name' not in cookie or 'value' not in cookie:
                    continue

                clean_cookie = {
                    'name': str(cookie['name']),
                    'value': str(cookie['value']),
                    'path': str(cookie.get('path', '/'))
                }

                if 'domain' in cookie:
                    clean_cookie['domain'] = str(cookie['domain'])

                if cookie.get('secure', False):
                    clean_cookie['secure'] = True

                driver.add_cookie(clean_cookie)
                loaded_count += 1
            except:
                continue

        if loaded_count > 0:
            driver.refresh()
            time.sleep(0.5)
            logger.info(f"✅ Загружено {loaded_count} cookies")
            return loaded_count > 0

        return False

    except Exception as e:
        logger.warning(f"Ошибка при загрузке cookies: {e}")
        return False

def extract_prices_fast(driver):
    """Быстрое извлечение цен: массово считывает первые 4 ds.valueLine + подписи"""
    price_data = {
        'обычная цена': '',
        'цена для юрлиц': ''
    }

    if STOP_PARSING:
        return price_data

    try:
        logger.debug("Извлечение цен из карточки товара...")


        script = """
        var result = {
            prices: [],
            labels: []
        };

        var valuelines = document.querySelectorAll("span.ds-valueLine");
        var targetElements = Array.from(valuelines).slice(0, 4);

        for (var i = 0; i < targetElements.length; i++) {
            var element = targetElements[i];
            var priceText = element.textContent.trim();
            result.prices.push(priceText);

            // Поиск подписей в соседних элементах
            var labelText = "";
            var parent = element.parentElement;

            if (parent && parent.parentElement) {
                var textLines = parent.parentElement.querySelectorAll(".ds-textLine");
                for (var j = 0; j < Math.min(textLines.length, 3); j++) {
                    var text = textLines[j].textContent.trim().toLowerCase();
                    if (text && text.length < 25) {
                        labelText = text;
                        break;
                    }
                }
            }

            result.labels.push(labelText);
        }

        return result;
        """

        try:
            bulk_data = driver.execute_script(script)
        except Exception as e:
            logger.warning(f"JavaScript ошибка, используем fallback: {e}")
            # Fallback
            all_valuelines = driver.find_elements(By.CSS_SELECTOR, "span.ds-valueLine")
            target_valuelines = all_valuelines[:4] if all_valuelines else []

            if not target_valuelines:
                return price_data

            bulk_data = {'prices': [], 'labels': []}
            for valueline in target_valuelines:
                bulk_data['prices'].append(valueline.text.strip())
                bulk_data['labels'].append("")

        if not bulk_data or not bulk_data.get('prices'):
            return price_data

        prices = bulk_data['prices']
        labels = bulk_data['labels']

        # Формируем данные для классификации
        prices_with_labels = []
        for i, (price_text, label_text) in enumerate(zip(prices, labels)):
            prices_with_labels.append({
                'text': price_text,
                'label': label_text.lower(),
                'index': i + 1
            })

        # Классификация по подписям
        regular_found = False
        vat_found = False

        # 1. Ищем "пэй" для обычной цены
        for item in prices_with_labels:
            if 'пэй' in item['label'] or 'pay' in item['label']:
                price_data['обычная цена'] = item['text']
                regular_found = True
                break

        # 2. Ищем "с НДС" для юрлиц
        for item in prices_with_labels:
            if 'с ндс' in item['label'] or 'ндс' in item['label'] or 'для юрлиц' in item['label']:
                price_data['цена для юрлиц'] = item['text']
                vat_found = True
                break

        # 3. Если не нашли "пэй" → первая цена как обычная
        if not regular_found and prices_with_labels:
            price_data['обычная цена'] = prices_with_labels[0]['text']

        return price_data

    except Exception as e:
        logger.error(f"Ошибка извлечения цен: {e}")
        return price_data

def extract_products_smart(driver) -> List[Dict[str, Any]]:
    products = []

    try:
        script = """
        const selectors = [
            'a[data-auto="snippet-link"]',
            'a[data-zone-name="title"]',
            'a[href*="/product--"]',
            'span[role="link"][data-auto="snippet-title"]'
        ];

        const nodes = [];
        selectors.forEach((selector) => {
            document.querySelectorAll(selector).forEach((node) => nodes.push(node));
        });

        const seen = new Set();
        const products = [];

        for (let i = 0; i < nodes.length; i++) {
            const node = nodes[i];
            const title = (node.textContent || '').trim();
            if (!title) continue;

            let link = node.closest('a[href]');
            if (!link && node.parentElement) {
                link = node.parentElement.querySelector('a[href]');
            }

            const rawUrl = link && link.href ? link.href : '';
            if (!rawUrl) continue;

            const normalizedUrl = rawUrl.split('?')[0];
            if (seen.has(normalizedUrl)) continue;
            seen.add(normalizedUrl);

            products.push({
                title: title,
                url: normalizedUrl,
                index: products.length + 1
            });

            if (products.length >= 6) break;
        }

        return products;
        """

        products_data = driver.execute_script(script, PRODUCT_LINK_SELECTORS)

        if products_data:
            products = [
                {
                    'title': p['title'],
                    'url': p['url'],
                    'index': p['index']
                }
                for p in products_data[:5]
                if p.get('url') and p.get('title')
            ]

        if products:
            logger.debug(f"Найдено {len(products)} товаров")
            return products

    except Exception as e:
        logger.warning(f"Ошибка извлечения товаров: {e}")

    return products

def parse_price_to_number(price_str: str) -> float:
    """Конвертирует строку цены в число для сравнения"""
    if not price_str:
        return float('inf')  # Бесконечность для отсутствующих цен

    try:
        # Убираем все кроме цифр, запятых и точек
        clean_price = re.sub(r'[^\d,.]', '', price_str)

        # Заменяем запятые на точки для float
        clean_price = clean_price.replace(',', '.')

        # Убираем множественные точки (оставляем только последнюю)
        if clean_price.count('.') > 1:
            parts = clean_price.split('.')
            clean_price = ''.join(parts[:-1]) + '.' + parts[-1]

        return float(clean_price) if clean_price else float('inf')
    except:
        return float('inf')

def collect_prices_from_all_products(driver, products: List[Dict[str, Any]], search_term: str) -> Dict[str, str]:
    result = {"цена": "", "цена для юрлиц": "", "ссылка": ""}

    if not products:
        logger.warning("Нет товаров для обработки")
        return result

    # Контейнеры для всех найденных цен
    all_products_data = []

    logger.info(f"Собираю цены с {len(products)} карточек товаров:")

    # Проходим по ВСЕМ товарам и собираем цены
    for i, product in enumerate(products, 1):
        if STOP_PARSING:
            break

        if not product.get('url'):
            logger.debug(f"Товар {i}: нет ссылки, пропуск")
            continue

        try:
            short_title = product['title'][:45] + "..." if len(product['title']) > 45 else product['title']
            logger.info(f"  {i}. {short_title}")

            for retry in range(2):
                try:
                    driver.get(product['url'])
                    time.sleep(1.2)
                    break
                except (WebDriverException, TimeoutException):
                    if retry == 1:
                        logger.warning(f"     Ошибка загрузки после повтора")
                        break
                    time.sleep(1)
                    continue

            if STOP_PARSING:
                break

            try:
                WebDriverWait(driver, 5).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
            except:
                pass

            prices = extract_prices_fast(driver)

            product_data = {
                'title': product['title'],
                'url': product['url'],
                'index': i,
                'обычная цена': prices.get('обычная цена', ''),
                'цена для юрлиц': prices.get('цена для юрлиц', ''),
                'regular_price_num': parse_price_to_number(prices.get('обычная цена', '')),
                'vat_price_num': parse_price_to_number(prices.get('цена для юрлиц', ''))
            }

            all_products_data.append(product_data)

            price_info = []
            if prices.get('обычная цена'):
                price_info.append(f"Обычная: {prices['обычная цена']}")
            if prices.get('цена для юрлиц'):
                price_info.append(f"Юрлица: {prices['цена для юрлиц']}")

            if price_info:
                logger.info(f"     {', '.join(price_info)}")
            else:
                logger.info(f"     цены не найдены")

        except StaleElementReferenceException as e:
            logger.warning(f"     StaleElement ошибка")
            continue
        except Exception as e:
            logger.warning(f"     Ошибка: {e}")
            continue

    if not all_products_data:
        logger.warning("Ни один товар не дал результата")
        return result

    valid_products = [p for p in all_products_data if p['regular_price_num'] != float('inf')]

    if valid_products:
        best_product = min(valid_products, key=lambda x: x['regular_price_num'])

        result["цена"] = best_product['обычная цена']
        result["цена для юрлиц"] = best_product['цена для юрлиц']
        result["ссылка"] = best_product['url']

        logger.info(f"ЛУЧШИЙ ВЫБОР: товар {best_product['index']} - {best_product['обычная цена']}")

        logger.info("Сравнение цен:")
        for p in sorted(all_products_data, key=lambda x: x['regular_price_num']):
            if p['regular_price_num'] != float('inf'):
                marker = "→ ВЫБРАН" if p == best_product else ""
                logger.info(f"  Товар {p['index']}: {p['обычная цена']} {marker}")
    else:
        # Если нет обычных цен, берем первый товар с любыми данными
        first_product = all_products_data[0]
        result["цена"] = first_product['обычная цена']
        result["цена для юрлиц"] = first_product['цена для юрлиц']
        result["ссылка"] = first_product['url']

        logger.warning("Обычные цены не найдены, взят первый товар")

    return result

def smart_search_input(driver, search_term: str, max_retries: int = 3) -> bool:
    """Надёжный поиск с fallback на прямой переход к странице результатов."""
    normalized_term = _normalize_search_term(search_term)
    if not normalized_term:
        logger.warning("Пустой поисковый запрос")
        return False

    current_url = driver.current_url or ""
    if 'search' in current_url and 'text=' in current_url:
        logger.debug("Уже на странице поиска, обновляем запрос")
        success = update_search_query(driver, normalized_term, max_retries)
    else:
        logger.debug("На главной странице, выполняем новый поиск")
        success = perform_new_search(driver, normalized_term, max_retries)

    if success:
        return True

    logger.warning("Поиск через поле не удался, пробую прямой URL")
    return _perform_direct_search_navigation(driver, normalized_term)

def update_search_query(driver, search_term: str, max_retries: int = 3) -> bool:
    """Обновляет поисковый запрос на странице результатов."""

    search_selectors = [
        'input[name="text"]',
        'input[data-auto="search-input"]',
        'input[placeholder*="искать" i]',
        'input[placeholder*="поиск" i]',
        '.search-input input',
        '.header-search input',
        '[data-zone="search"] input',
        'input.n-search__input',
        'input[type="search"]',
    ]

    for retry in range(max_retries):
        if STOP_PARSING:
            return False

        try:
            WebDriverWait(driver, 5).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )

            searchbox = None
            for selector in search_selectors:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for candidate in elements:
                    if candidate.is_displayed() and candidate.is_enabled():
                        searchbox = candidate
                        logger.debug(f"Найдено поле поиска: {selector}")
                        break
                if searchbox:
                    break

            if not searchbox:
                logger.warning(f"Попытка {retry + 1}: поле поиска не найдено на странице результатов")
                if retry < max_retries - 1:
                    driver.get("https://market.yandex.ru")
                    time.sleep(1)
                    continue
                return False

            driver.execute_script(
                """
                const input = arguments[0];
                const value = arguments[1];
                input.focus();
                input.value = '';
                input.dispatchEvent(new Event('input', { bubbles: true }));
                input.value = value;
                input.dispatchEvent(new Event('input', { bubbles: true }));
                input.dispatchEvent(new Event('change', { bubbles: true }));
                """,
                searchbox,
                search_term,
            )
            searchbox.send_keys(Keys.RETURN)

            WebDriverWait(driver, 8).until(lambda d: 'search' in (d.current_url or ''))
            return True

        except (TimeoutException, StaleElementReferenceException) as e:
            logger.warning(f"Попытка {retry + 1}: не удалось обновить запрос ({e})")
            if retry < max_retries - 1:
                time.sleep(1)
                continue
            return False
        except Exception as e:
            logger.warning(f"Попытка {retry + 1} обновления запроса: {e}")
            if retry < max_retries - 1:
                time.sleep(1)
                continue
            return False

    return False

def perform_new_search(driver, search_term: str, max_retries: int = 3) -> bool:
    """Выполняет новый поиск с главной страницы."""

    selectors = [
        (By.NAME, "text"),
        (By.CSS_SELECTOR, "input[name='text']"),
        (By.CSS_SELECTOR, "[data-auto='search-input']"),
        (By.CSS_SELECTOR, "input[type='search']"),
    ]

    for retry in range(max_retries):
        if STOP_PARSING:
            return False

        try:
            WebDriverWait(driver, 5).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )

            searchbox = None
            for selector_type, selector in selectors:
                elements = driver.find_elements(selector_type, selector)
                for candidate in elements:
                    if candidate.is_displayed() and candidate.is_enabled():
                        searchbox = candidate
                        break
                if searchbox:
                    break

            if not searchbox:
                logger.warning(f"Попытка {retry + 1}: поле поиска не найдено на главной")
                if retry < max_retries - 1:
                    driver.get("https://market.yandex.ru")
                    time.sleep(1)
                    continue
                return False

            driver.execute_script(
                """
                const input = arguments[0];
                const value = arguments[1];
                input.focus();
                input.value = '';
                input.dispatchEvent(new Event('input', { bubbles: true }));
                input.value = value;
                input.dispatchEvent(new Event('input', { bubbles: true }));
                input.dispatchEvent(new Event('change', { bubbles: true }));
                """,
                searchbox,
                search_term,
            )
            searchbox.send_keys(Keys.RETURN)

            WebDriverWait(driver, 8).until(lambda d: 'search' in (d.current_url or ''))
            return True

        except (TimeoutException, StaleElementReferenceException) as e:
            logger.warning(f"Попытка {retry + 1}: сбой нового поиска ({e})")
            if retry < max_retries - 1:
                time.sleep(1)
                continue
            return False
        except Exception as e:
            logger.warning(f"Попытка {retry + 1} нового поиска: {e}")
            if retry < max_retries - 1:
                time.sleep(1)
                continue
            return False

    return False

def get_prices(product_name: str, headless: bool = True, driver_path: Optional[str] = None,
              timeout: int = 15, use_business_auth: bool = True) -> Dict[str, str]:
    """Главная функция получения цен с выбором наименьшей из 5 карточек"""
    result = {"цена": "", "цена для юрлиц": "", "ссылка": ""}
    driver = None
    current_profile_path = None

    if STOP_PARSING:
        return result

    try:
        driver = create_driver(headless=headless, driver_path=driver_path, use_auth=use_business_auth)

        driver.get("https://market.yandex.ru/")
        time.sleep(0.5)

        # Отслеживаем профиль текущего драйвера для точечной очистки
        current_profile_path = getattr(driver, "profile_path", None)

        # Загрузка cookies для авторизации и поиска
        if use_business_auth and not STOP_PARSING:
            load_cookies_for_auth(driver)

        if STOP_PARSING:
            return result

        # Переход на маркет (только если не на странице поиска)
        current_url = driver.current_url
        if 'market.yandex.ru' not in current_url:
            try:
                driver.get("https://market.yandex.ru")
                time.sleep(0.8)
            except Exception as e:
                logger.error(f"Ошибка перехода на маркет: {e}")
                return result

        if STOP_PARSING:
            return result

        # УЛУЧШЕННЫЙ поиск с определением состояния страницы
        search_success = smart_search_input(driver, product_name)
        if not search_success:
            logger.error("Не удалось выполнить поиск")
            return result

        if STOP_PARSING:
            return result

        # Извлечение товаров
        products = extract_products_smart(driver)
        if not products:
            logger.warning("Товары не найдены")
            return result

        if STOP_PARSING:
            return result

        # Собираем цены со ВСЕХ товаров и выбираем НАИМЕНЬШУЮ
        result = collect_prices_from_all_products(driver, products, product_name)

        return result

    except Exception as e:
        logger.error(f"Ошибка обработки товара {product_name[:30]}...: {e}")
        return result

    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass

        # Очистка профиля по завершении
        if current_profile_path:
            success = cleanup_single_profile(current_profile_path)
            if success:
                CREATED_PROFILES.discard(current_profile_path)

def _make_product_cache_key(product_name: str) -> str:
    """Ключ кэша для повторяющихся наименований товаров."""
    return re.sub(r"\s+", " ", str(product_name or "")).strip().lower()

def parse_tender_excel(input_file: str, output_file: str, headless: bool = True,
                      workers: int = 1, driver_path: Optional[str] = None,
                      auto_save: bool = True, use_business_auth: bool = False) -> pd.DataFrame:
    """ОСНОВНАЯ функция парсинга с автосохранением и ТЕНДЕРНЫМ ФОРМАТОМ"""
    global STOP_PARSING, CURRENT_DATAFRAME, CURRENT_OUTPUT_FILE, CURRENT_INPUT_FILE

    # Настройка автосохранения при завершении
    setup_signal_handlers()

    STOP_PARSING = False
    CURRENT_INPUT_FILE = input_file
    CURRENT_OUTPUT_FILE = output_file


    items = extract_products_from_excel(input_file)
    if items.empty:
        raise ValueError("Не найдены товары в файле")

    # DataFrame для хранения результатов парсинга
    df = pd.DataFrame({
        'наименование': items['name'],
        'цена': '',
        'цена для юрлиц': '',
        'ссылка': ''
    })

    CURRENT_DATAFRAME = df  # Для автосохранения

    effective_workers = max(1, int(workers or 1))
    if effective_workers != 1:
        logger.warning("Параметр workers сейчас не используется: обработка выполняется последовательно")

    auth_text = "с авторизацией" if use_business_auth else "без авторизации"
    logger.info(f"Начинаю обработку {len(df)} товаров {auth_text}")
    logger.info("🔄 Автосохранение при принудительном завершении АКТИВНО")
    logger.info("📋 РЕЗУЛЬТАТ: тендерная таблица с колонкой 'Яндекс Маркет'")
    logger.info("Режим: поиск наименьшей цены среди 5 карточек")

    cache: Dict[str, Dict[str, str]] = {}

    try:
        for idx, row in enumerate(df.itertuples(index=False), start=1):
            if STOP_PARSING:
                logger.info("Парсинг остановлен")
                break

            try:
                product_name = row.наименование
                logger.info(f"Обработка: {idx}/{len(df)} - {product_name[:40]}...")

                cache_key = _make_product_cache_key(product_name)
                if cache_key in cache:
                    prices = cache[cache_key]
                    logger.info(f"Повтор товара, использую кэш: {product_name[:40]}...")
                else:
                    prices = get_prices(product_name, headless, driver_path, 20, use_business_auth)
                    if any(prices.get(k) for k in ("цена", "цена для юрлиц", "ссылка")):
                        cache[cache_key] = prices.copy()

                row_idx = idx - 1
                df.at[row_idx, 'цена'] = prices.get('цена', '')
                df.at[row_idx, 'цена для юрлиц'] = prices.get('цена для юрлиц', '')
                df.at[row_idx, 'ссылка'] = prices.get('ссылка', '')

                # Лог результата
                price_summary = []
                if prices.get('цена'):
                    price_summary.append(f"Лучшая цена: {prices['цена'][:15]}")
                if prices.get('цена для юрлиц'):
                    price_summary.append(f"Для юрлиц: {prices['цена для юрлиц'][:15]}")

                if price_summary:
                    logger.info(f"Результат {idx}/{len(df)}: {', '.join(price_summary)}")
                else:
                    logger.info(f"Результат {idx}/{len(df)}: цены не найдены")

                # Автосохранение каждые 3 товара В ТЕНДЕРНОМ ФОРМАТЕ
                if auto_save and idx % 3 == 0:
                    try:
                        save_results_into_tender_format(input_file, output_file, df)
                        logger.info(f"Автосохранение тендера: {idx}/{len(df)}")
                    except Exception as e:
                        logger.warning(f"Ошибка автосохранения: {e}")

            except Exception as e:
                logger.error(f"Ошибка товара {idx}: {e}")
                df.at[idx - 1, 'цена'] = "ОШИБКА"
                df.at[idx - 1, 'цена для юрлиц'] = "ОШИБКА"

    finally:
        cleanup_profiles()
        CURRENT_DATAFRAME = None  # Очищаем глобальную переменную

    # Финальное сохранение В ТЕНДЕРНОМ ФОРМАТЕ
    if output_file != "auto":
        save_results_into_tender_format(input_file, output_file, df)
        logger.info(f"🎯 ТЕНДЕРНАЯ ТАБЛИЦА ГОТОВА: {output_file}")
        logger.info("📊 Создана точная копия оригинала + колонка 'Яндекс Маркет'")

    return df

if __name__ == "__main__":
    test_product = "Точка доступа Ubiquiti UniFi AC Pro AP"
    print("Тест финальной версии с тендерным форматом...")
    result = get_prices(test_product, headless=False, use_business_auth=True)

    print(f"Товар: {test_product}")
    print(f"Лучшая цена: {result['цена']}")
    print(f"Цена для юрлиц: {result['цена для юрлиц'] or 'НЕ НАЙДЕНА'}")
    print(f"Ссылка: {result['ссылка']}")
    print("-" * 50)
