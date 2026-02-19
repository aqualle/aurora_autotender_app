# ozon_parser.py - С ПРАВИЛЬНЫМИ СЕЛЕКТОРАМИ И JS

import time
import logging
import re
import os
import requests
from typing import Dict, Optional
from selenium import webdriver
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys

from utils import get_browser_paths


logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

STOP_PARSING = False


def _normalize_ozon_query(product_name: str, max_len: int = 120) -> str:
    return re.sub(r"\s+", " ", str(product_name or "")).strip()[:max_len]


def _go_to_ozon_search(driver, query: str) -> bool:
    if not query:
        return False
    try:
        encoded_query = requests.utils.quote(query)
        driver.get(f"https://www.ozon.ru/search/?text={encoded_query}")
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'a[href*="/product/"]'))
        )
        return True
    except Exception as e:
        logger.warning(f"Не удалось перейти на страницу поиска Ozon напрямую: {e}")
        return False



def _score_ozon_relevance(search_term: str, title: str) -> int:
    query_tokens = {
        t for t in re.split(r"[^a-zA-Zа-яА-Я0-9]+", str(search_term).lower())
        if len(t) >= 3
    }
    title_tokens = {
        t for t in re.split(r"[^a-zA-Zа-яА-Я0-9]+", str(title).lower())
        if len(t) >= 3
    }
    if not query_tokens or not title_tokens:
        return 0
    return len(query_tokens & title_tokens)


def create_ozon_edge_driver(headless: bool = False):
    paths = get_browser_paths()["edge"]

    options = webdriver.EdgeOptions()
    options.binary_location = str(paths["binary"])

    # антидетект для OZON
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-infobars")
    options.add_argument("--start-maximized")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")

    # ❌ НЕ headless для озона
    if headless:
        options.add_argument("--headless=new")
        options.add_argument("--window-size=1280,800")

    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0"
    )

    service = EdgeService(str(paths["driver"]))

    driver = webdriver.Edge(service=service, options=options)
    driver.set_page_load_timeout(30)
    driver.implicitly_wait(5)

    return driver

def extract_price_js(driver) -> Dict[str, str]:
    """
    Использует JavaScript для поиска цены "без Ozon Карты"
    JS ищет селектор span.pdp_b7f.tsHeadline500Medium (цена без карты)
    """
    try:
        # JS СКРИПТ ДЛЯ ПОИСКА ЦЕНЫ
        js_script = """
        // Ищем цену БЕЗ КАРТЫ (span.pdp_b7f.tsHeadline500Medium)
        const priceElement = document.querySelector('span.pdp_b7f.tsHeadline500Medium');
        if (priceElement) {
            const priceText = priceElement.textContent.trim();
            return { price: priceText, source: 'pdp_b7f.tsHeadline500Medium' };
        }
        
        // Если не найдено, ищем через div[data-widget="webPrice"]
        const webPriceWidget = document.querySelector('div[data-widget="webPrice"]');
        if (webPriceWidget) {
            // Ищем все spans с ценой
            const spans = webPriceWidget.querySelectorAll('span.tsHeadline500Medium, span.tsHeadline600Large');
            if (spans.length > 0) {
                // Берём последний (обычно это цена без карты)
                const priceText = spans[spans.length - 1].textContent.trim();
                return { price: priceText, source: 'webPrice-widget' };
            }
        }
        
        return { price: '', source: 'not-found' };
        """
        
        result = driver.execute_script(js_script)
        logger.debug(f"JS результат: {result}")
        
        if result and result.get('price'):
            return {
                'цена': result['price'],
                'источник': result['source']
            }
        
        return {'цена': '', 'источник': 'js-не-нашла'}
    
    except Exception as e:
        logger.warning(f"Ошибка JS скрипта: {e}")
        return {'цена': '', 'источник': 'js-ошибка'}

def extract_prices_ozon(driver) -> Dict[str, str]:
    """Извлечение цены с Ozon - ИСПРАВЛЕННЫЕ СЕЛЕКТОРЫ"""
    result = {'цена': '', 'цена для юрлиц': ''}
    
    try:
        logger.debug("Извлечение цены с Ozon...")

        price_selectors = [
            # Основной селектор - цена БЕЗ Ozon Карты
            ('span.pdp_b7f.tsHeadline500Medium', 'Цена без карты (pdp_b7f)'),
            # Альтернатива - через data-widget
            ('div[data-widget="webPrice"] span.tsHeadline500Medium', 'Цена через webPrice widget'),
            # Старые селекторы на случай
            ('span.tsHeadline500Medium', 'Общий tsHeadline500Medium'),
            ('span.tsHeadline600Large', 'tsHeadline600Large'),
        ]
        
        price_text = ""
        used_selector = ""
        
        for selector, description in price_selectors:
            try:
                elements = driver.find_elements('css selector', selector)
                if elements:
                    for elem in elements:
                        text = elem.text.strip()
                        if text and '₽' in text:
                            price_text = text
                            used_selector = description
                            logger.debug(f"✅ Цена найдена: {selector} ({description}) = {text}")
                            break
                    if price_text:
                        break
            except Exception as e:
                logger.debug(f"  Селектор '{description}' не сработал: {e}")
                continue
        
        # Если селекторы не помогли, пробуем JS
        if not price_text:
            logger.debug("Селекторы не сработали, использую JS...")
            js_result = extract_price_js(driver)
            if js_result['цена']:
                price_text = js_result['цена']
                used_selector = f"JS ({js_result['источник']})"
                logger.debug(f"✅ JS нашла цену: {price_text}")
        
        if price_text:
            logger.debug(f"Используемый селектор: {used_selector}")
            price_clean = re.sub(r'[^\d]', '', price_text)
            if price_clean:
                price_num = int(price_clean)
                result['цена'] = f"{price_num:,} ₽".replace(',', ' ')
                result['цена для юрлиц'] = f"{int(price_num * 1.22):,} ₽".replace(',', ' ')
                logger.debug(f"Обработанная цена: {result['цена']}")
        else:
            logger.warning("❌ Цена не найдена ни селектором, ни JS")
        
        return result
    
    except Exception as e:
        logger.warning(f"Ошибка извлечения цены: {e}")
        return result

def get_prices(product_name: str, headless: bool = True, driver_path: Optional[str] = None,
              timeout: int = 20, **kwargs) -> Dict[str, str]:
    """Получение цен с Ozon через undetected-chromedriver"""
    
    result = {"цена": "", "цена для юрлиц": "", "ссылка": ""}
    
    if STOP_PARSING:
        return result
    
    try:
        # Пробуем импортировать undetected-chromedriver
        # try:
        #     import undetected_chromedriver as uc
        #     from selenium.webdriver.common.by import By
        #     from selenium.webdriver.support.ui import WebDriverWait
        #     from selenium.webdriver.support import expected_conditions as EC
        #     from selenium.webdriver.common.keys import Keys
        #     logger.debug("✅ undetected-chromedriver найден")
        # except ImportError:
        #     logger.error("❌ undetected-chromedriver не установлен!")
        #     logger.error("Установите: pip install undetected-chromedriver")
        #     return result
        
        query = _normalize_ozon_query(product_name)
        logger.info(f"🔍 Поиск на Ozon: {query[:40]}...")
        
        # Создаём UNDETECTED браузер
        driver = None
        try:
            # driver = uc.Chrome(headless=headless, version_main=None)
            driver = create_ozon_edge_driver(headless=headless)
            logger.debug("✅ Undetected браузер создан")
        except Exception as e:
            logger.error(f"Ошибка создания браузера: {e}")
            return result
        
        try:
            # Переход на Ozon
            logger.debug("Переход на https://www.ozon.ru")
            driver.get("https://www.ozon.ru")
            time.sleep(3)
            
            # Проверяем что НЕ заблокировано
            page_source = driver.page_source
            current_url = driver.current_url
            page_title = driver.title
            
            logger.debug(f"📍 URL: {current_url}")
            logger.debug(f"📄 Title: {page_title}")
            
            # Проверяем на реальные ошибки
            block_indicators = ["Доступ ограничен", "Access denied", "403 Forbidden", "419 Too Many Requests"]
            
            for indicator in block_indicators:
                if indicator in page_source:
                    logger.error(f"❌ Найден индикатор блокировки: {indicator}")
                    return result
            
            logger.debug("✅ Ozon не блокирует")
            
            # Поиск поля ввода
            search_input = None
            try:
                logger.debug("Ищу поле поиска...")
                search_input = WebDriverWait(driver, 8).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, 'input[name="text"]'))
                )
                logger.debug("✅ Поле поиска найдено")
            except Exception as e:
                logger.error(f"❌ Поле поиска не найдено: {e}")
                if not _go_to_ozon_search(driver, query):
                    return result
                search_input = None
            
            # Клик и ввод поиска (если нашли поле на главной)
            if search_input is not None:
                logger.debug("Начинаю ввод поиска...")
                search_input.click()
                time.sleep(0.5)
                search_input.clear()
                time.sleep(0.3)
                search_input.send_keys(query[:50])
                logger.debug(f"✅ Введён текст: {query[:50]}")
                time.sleep(0.5)
                search_input.send_keys(Keys.RETURN)
                logger.debug("✅ Нажал Enter")
                time.sleep(4)
            
            if STOP_PARSING:
                return result
            
            # Ждём результатов
            try:
                logger.debug("Жду загрузки результатов...")
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'a[href*="/product/"]'))
                )
                logger.debug("✅ Результаты загрузились")
            except Exception as e:
                logger.warning(f"❌ Результаты не загрузились: {e}")
                return result
            
            # Находим товары
            product_links = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/product/"]')
            if not product_links:
                logger.warning("❌ Товары не найдены")
                return result
            
            logger.info(f"✅ Найдено товаров: {len(product_links)}")
            
            # Собираем и ранжируем кандидатов по релевантности
            candidates = []
            seen = set()
            for link in product_links[:40]:
                try:
                    url = link.get_attribute('href')
                    if not url or '/product/' not in url:
                        continue
                    normalized_url = url.split('?')[0]
                    if normalized_url in seen:
                        continue

                    title = (link.text or '').strip()
                    if not title:
                        title = (link.get_attribute('title') or '').strip()
                    if not title:
                        title = (link.get_attribute('aria-label') or '').strip()

                    score = _score_ozon_relevance(query, title)
                    candidates.append({
                        'url': normalized_url,
                        'title': title,
                        'score': score,
                    })
                    seen.add(normalized_url)
                except Exception:
                    continue

            if not candidates:
                logger.warning("❌ Не удалось сформировать список кандидатов")
                return result

            max_score = max((c['score'] for c in candidates), default=0)
            if max_score > 0:
                selected = [c for c in candidates if c['score'] == max_score][:5]
                logger.info(f"✅ Релевантных кандидатов: {len(selected)} из {len(candidates)} (score={max_score})")
            else:
                selected = candidates[:5]
                logger.info(f"✅ Релевантность не определена, проверяю первые {len(selected)} карточек")

            if not selected:
                return result
            
            # Проверяем товары
            all_prices = []
            for i, candidate in enumerate(selected, 1):
                url = candidate['url']
                if STOP_PARSING:
                    break
                
                try:
                    logger.debug(f"Товар {i}/{len(selected)}: {url[:50]}...")
                    driver.get(url)
                    time.sleep(1.5)
                    
                    # Извлекаем цену с НОВЫМИ селекторами
                    prices = extract_prices_ozon(driver)
                    
                    if prices['цена']:
                        price_clean = re.sub(r'[^\d]', '', prices['цена'])
                        if price_clean:
                            price_num = int(price_clean)
                            all_prices.append({
                                'price_num': price_num,
                                'price': prices['цена'],
                                'price_vat': prices['цена для юрлиц'],
                                'url': url
                            })
                            logger.info(f"    ✅ Цена: {prices['цена']}")
                    else:
                        logger.debug(f"    ⚠️ Цена не найдена на странице")
                
                except Exception as e:
                    logger.warning(f"Ошибка товара {i}: {e}")
                    continue
            
            # Выбираем самый дешёвый
            if all_prices:
                best = min(all_prices, key=lambda x: x['price_num'])
                result = {
                    "цена": best['price'],
                    "цена для юрлиц": best['price_vat'],
                    "ссылка": best['url']
                }
                logger.info(f"🎯 ЛУЧШАЯ: {best['price']}")
            else:
                logger.warning("⚠️ Цены не найдены ни на одном товаре")
            
            return result
        
        finally:
            # ОБЯЗАТЕЛЬНОЕ ЗАКРЫТИЕ БРАУЗЕРА
            if driver:
                try:
                    driver.quit()
                    logger.debug("✅ Браузер закрыт корректно")
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка при закрытии браузера: {e}")
    
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        return result

if __name__ == "__main__":
    result = get_prices("Коммутатор", headless=False)
    print(result)
