import time
import logging
from typing import Dict, Optional
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException, WebDriverException

from utils import normalize, extract_products_from_excel, save_results_into_excel

try:
    from webdriver_manager.chrome import ChromeDriverManager
    _HAS_WDM = True
except Exception:
    _HAS_WDM = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _create_driver(headless: bool = True, driver_path: Optional[str] = None):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--start-maximized")
    options.add_argument("--window-size=1920,1080")

    if _HAS_WDM and driver_path is None:
        service = Service(ChromeDriverManager().install())
    else:
        service = Service(driver_path) if driver_path else Service()

    driver = webdriver.Chrome(service=service, options=options)
    return driver


def _title_matches(query: str, title: str) -> bool:
    q = normalize(query)
    t = normalize(title)
    q_words = [w for w in q.split() if len(w) > 2]
    t_words = t.split()
    if not q_words:
        return False
    matched = sum(1 for w in q_words if w in t_words)
    needed = max(1, len(q_words) // 2)
    return matched >= needed


def _get_href_from_element(elem) -> Optional[str]:
    """
    Попытаться получить href из найденного элемента:
    - если сам <a> — взять href,
    - иначе подняться по родителям до <a>,
    - вернуть None, если не удалось.
    Это минимальная, надёжная логика.
    """
    try:
        href = elem.get_attribute("href")
        if href:
            return href
    except StaleElementReferenceException:
        return None
    except Exception:
        # возможен случай, что span не имеет get_attribute('href') — игнорируем
        pass

    # поднимаемся по родителям до 4 уровней
    parent = elem
    for _ in range(4):
        try:
            parent = parent.find_element(By.XPATH, "./parent::*")
        except Exception:
            parent = None
        if not parent:
            break
        try:
            if parent.tag_name.lower() == "a":
                href = parent.get_attribute("href")
                if href:
                    return href
        except StaleElementReferenceException:
            return None
        except Exception:
            continue

    # попробовать ancestor::a[1]
    try:
        a = elem.find_element(By.XPATH, "./ancestor::a[1]")
        href = a.get_attribute("href")
        if href:
            return href
    except Exception:
        pass

    return None


def get_prices(product_name: str,
               headless: bool = True,
               driver_path: Optional[str] = None,
               timeout: int = 20) -> Dict[str, str]:
    """
    Возвращает словарь с ценой и ссылкой ('Основная', 'Без карты', 'Для юр. лиц', 'Ссылка').
    Минимальные изменения от твоего первоначального кода: после открытия карточки сохраняем URL.
    """
    res = {"Основная": "—", "Без карты": "—", "Для юр. лиц": "—", "Ссылка": ""}
    driver = None
    try:
        driver = _create_driver(headless=headless, driver_path=driver_path)
        driver.get("https://market.yandex.ru/")
        wait = WebDriverWait(driver, timeout)

        # вводим запрос
        search_box = wait.until(EC.presence_of_element_located((By.NAME, "text")))
        search_box.clear()
        search_box.send_keys(product_name)
        search_box.send_keys(Keys.RETURN)

        # пытаемся найти карточки — используем твой изначальный селектор как основной
        try:
            cards = WebDriverWait(driver, timeout + 5).until(
                EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, "span[role='link'][data-auto='snippet-title'][title]")
                )
            )
        except Exception:
            # если ничего не найдено — пробуем более явный поиск всех ссылок
            try:
                cards = driver.find_elements(By.CSS_SELECTOR, "a[href]")
            except Exception:
                cards = []

        if not cards:
            return res

        # выбираем target (как раньше)
        target = None
        for c in cards:
            try:
                title_attr = c.get_attribute("title") or c.text or ""
            except StaleElementReferenceException:
                continue
            if _title_matches(product_name, title_attr):
                target = c
                break
        if target is None:
            target = cards[0]

        # Пытаемся получить href у target (родительский <a> если нужно)
        href = _get_href_from_element(target)
        # Если href не найден — используем кликом старую логику и затем current_url
        if not href:
            try:
                driver.execute_script("arguments[0].click();", target)
                # если открылась новая вкладка — переключаем (как раньше)
                if len(driver.window_handles) > 1:
                    driver.switch_to.window(driver.window_handles[-1])
                # попытаемся прочитать текущий URL
                try:
                    href = driver.current_url
                except Exception:
                    href = None
            except Exception:
                href = None

        # если нашли href — сохраняем и переходим на него (без клика)
        if href:
            res["Ссылка"] = href
            try:
                driver.get(href)
            except Exception:
                # если driver.get падает — продолжаем, может быть мы уже на карточке
                pass
        else:
            # ничего не нашли — возвращаем прочерки
            return res

        # теперь парсим цену — ожидаем несколько селекторов (тот же набор, минимальные изменения)
        price_selectors = [
            "span.ds-text_color_price-term",
            "div[data-auto='mainPrice'] span",
            "span.price",
        ]
        found_price = False
        for sel in price_selectors:
            try:
                # короткий wait: если селектор не появится за timeout/2 — пропускаем
                try:
                    elem = WebDriverWait(driver, min(timeout, 8)).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, sel))
                    )
                except TimeoutException:
                    # попробовать напрямую find_element без wait (иногда айтем уже есть)
                    elem = driver.find_element(By.CSS_SELECTOR, sel)
                text = elem.text.strip()
                if text:
                    res["Основная"] = text
                    found_price = True
                    break
            except Exception:
                continue

        # попробуем более редкий вариант (meta itemprop="price")
        if not found_price:
            try:
                meta_price = driver.find_element(By.CSS_SELECTOR, 'meta[itemprop="price"]')
                content = meta_price.get_attribute("content")
                if content:
                    res["Основная"] = content.strip()
                    found_price = True
            except Exception:
                pass

        # небольшая пауза для стабильности
        time.sleep(0.25)
        return res

    except WebDriverException as e:
        logger.exception("WebDriver exception при парсинге %s: %s", product_name, e)
        return res
    except Exception as e:
        logger.exception("Ошибка при парсинге %s: %s", product_name, e)
        return res
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


def parse_tender_excel(input_file: str,
                       output_file: str,
                       headless: bool = True,
                       workers: int = 1,
                       driver_path: Optional[str] = None) -> pd.DataFrame:
    items = extract_products_from_excel(input_file)
    if items.empty:
        raise ValueError("Не обнаружено ни одной позиции в файле.")

    df = items.rename(columns={"raw": "Исходная ячейка", "name": "Название"})
    df["Normalized"] = df["Название"].apply(normalize)
    df["Основная"] = "—"
    df["Без карты"] = "—"
    df["Для юрлиц"] = "—"
    df["Ссылка"] = ""

    logger.info("Начинаем парсинг %d позиций (workers=%d)", len(df), workers)
    tasks = {}
    with ThreadPoolExecutor(max_workers=workers) as exe:
        for idx, row in df.iterrows():
            future = exe.submit(get_prices, row["Название"], headless, driver_path)
            tasks[future] = idx
        for future in as_completed(tasks):
            idx = tasks[future]
            try:
                prices = future.result()
                df.at[idx, "Основная"] = prices.get("Основная", "—")
                df.at[idx, "Без карты"] = prices.get("Без карты", "—")
                df.at[idx, "Для юрлиц"] = prices.get("Для юр. лиц", "—")
                df.at[idx, "Ссылка"] = prices.get("Ссылка", "")
            except Exception as e:
                logger.exception("Ошибка обработки позиции idx=%s: %s", idx, e)
                df.at[idx, "Основная"] = df.at[idx, "Без карты"] = df.at[idx, "Для юрлиц"] = "ERR"

    save_results_into_excel(input_file, output_file, df)
    return df
