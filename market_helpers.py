import re
import time
from typing import Callable, List, Optional, Sequence, Tuple

import requests
from selenium.webdriver.common.by import By

SEARCH_URL_TEMPLATE = "https://market.yandex.ru/search?text={query}"
SEARCH_INPUT_SELECTORS: List[str] = [
    "input[name=\"text\"]",
    "input[data-auto=\"search-input\"]",
    "input[placeholder*=\"искать\" i]",
    "input[placeholder*=\"поиск\" i]",
    ".search-input input",
    ".header-search input",
    "[data-zone=\"search\"] input",
    "input.n-search__input",
    "input[type=\"search\"]",
]
HOME_SEARCH_SELECTORS: List[Tuple[str, str]] = [
    (By.NAME, "text"),
    (By.CSS_SELECTOR, "input[name=\"text\"]"),
    (By.CSS_SELECTOR, "[data-auto=\"search-input\"]"),
    (By.CSS_SELECTOR, "input[type=\"search\"]"),
]
PRODUCT_LINK_SELECTORS: List[str] = [
    "a[data-auto=\"snippet-link\"]",
    "a[data-zone-name=\"title\"]",
    "a[href*=\"/product--\"]",
    "span[role=\"link\"][data-auto=\"snippet-title\"]",
]


def normalize_search_term(search_term: str, max_len: int = 120) -> str:
    """Нормализует строку поиска перед вводом в маркет."""
    cleaned = re.sub(r"\s+", " ", str(search_term or "")).strip()
    return cleaned[:max_len]


def perform_direct_search_navigation(driver, search_term: str, log_warning: Optional[Callable[[str], None]] = None) -> bool:
    """Fallback: выполняет прямой переход на URL поиска."""
    normalized = normalize_search_term(search_term)
    if not normalized:
        return False

    try:
        encoded_query = requests.utils.quote(normalized)
        driver.get(SEARCH_URL_TEMPLATE.format(query=encoded_query))
        time.sleep(1.2)
        return "search" in (driver.current_url or "")
    except Exception as exc:
        if log_warning:
            log_warning(f"Не удалось перейти по прямому URL поиска: {exc}")
        return False


def fill_search_input_js(driver, input_element, value: str) -> None:
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
        input_element,
        value,
    )


def find_first_interactable(driver, selectors: Sequence[Tuple[str, str]]):
    for selector_type, selector in selectors:
        elements = driver.find_elements(selector_type, selector)
        for candidate in elements:
            if candidate.is_displayed() and candidate.is_enabled():
                return candidate
    return None


def find_first_interactable_css(driver, selectors: Sequence[str]):
    return find_first_interactable(driver, [(By.CSS_SELECTOR, selector) for selector in selectors])
