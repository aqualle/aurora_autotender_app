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


def get_prices(product_name: str,
               headless: bool = True,
               driver_path: Optional[str] = None,
               timeout: int = 20) -> Dict[str, str]:
    res = {"Основная": "—", "Без карты": "—", "Для юр. лиц": "—"}
    driver = None
    try:
        driver = _create_driver(headless=headless, driver_path=driver_path)
        driver.get("https://market.yandex.ru/")
        wait = WebDriverWait(driver, timeout)

        search_box = wait.until(EC.presence_of_element_located((By.NAME, "text")))
        search_box.clear()
        search_box.send_keys(product_name)
        search_box.send_keys(Keys.RETURN)

        try:
            cards = WebDriverWait(driver, timeout + 5).until(
                EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, "span[role='link'][data-auto='snippet-title'][title]")
                )
            )
        except Exception:
            cards = driver.find_elements(By.CSS_SELECTOR, "a[href]")

        if not cards:
            return res

        target = None
        for c in cards:
            title_attr = c.get_attribute("title") or c.text or ""
            if _title_matches(product_name, title_attr):
                target = c
                break
        if target is None:
            target = cards[0]

        driver.execute_script("arguments[0].click();", target)

        if len(driver.window_handles) > 1:
            driver.switch_to.window(driver.window_handles[-1])

        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "h1")))

        price_selectors = [
            "span.ds-text_color_price-term",
            "div[data-auto='mainPrice'] span",
            "span.price",
        ]
        for sel in price_selectors:
            try:
                elem = driver.find_element(By.CSS_SELECTOR, sel)
                text = elem.text.strip()
                if text:
                    res["Основная"] = text
                    break
            except Exception:
                continue

        time.sleep(0.4)
        return res
    except Exception as e:
        logger.exception("Ошибка при парсинге %s: %s", product_name, e)
        return res
    finally:
        if driver:
            driver.quit()


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
            except Exception as e:
                logger.exception("Ошибка обработки позиции idx=%s: %s", idx, e)
                df.at[idx, "Основная"] = df.at[idx, "Без карты"] = df.at[idx, "Для юрлиц"] = "ERR"

    save_results_into_excel(input_file, output_file, df)
    return df
