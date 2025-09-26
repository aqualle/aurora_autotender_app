import pandas as pd
import re
from openpyxl import load_workbook
from openpyxl.styles import Font
import pickle
from typing import Any
import os


def normalize(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text = text.lower()
    text = re.sub(r"[^a-zа-я0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def debug_print_excel_rows(path: str, n: int = 50):
    df = pd.read_excel(path, header=None)
    print("\n=== Первые строки Excel ===")
    for i, row in df.head(n).iterrows():
        print(f"{i}: {[str(x) for x in row.tolist()]}")


def extract_products_from_excel(path: str):
    all_sheets = pd.read_excel(path, header=None, sheet_name=None)

    found_df = None
    col_index = None
    start_row = None
    end_row = None

    for sheet_name, df in all_sheets.items():
        for i, row in df.iterrows():
            for j, val in row.items():
                if isinstance(val, str) and "наименование" in val.lower():
                    found_df = df
                    col_index = j
                    start_row = i + 1
                    break
            if found_df is not None:
                break
        if found_df is not None:
            break

    if found_df is None:
        raise ValueError("Не найден лист с колонкой 'Наименование...'")

    for i, val in enumerate(found_df[col_index]):
        if isinstance(val, str) and "итого без ндс" in val.lower():
            end_row = i
            break
    if end_row is None:
        raise ValueError("Не найден конец таблицы ('Итого без НДС')")

    items = []
    for text in found_df.loc[start_row:end_row - 1, col_index]:
        if not isinstance(text, str):
            continue
        raw = text.strip()
        name = re.split(r"\n", raw)[0].strip()
        if name:
            items.append({"raw": raw, "name": name})

    return pd.DataFrame(items)


def save_results_into_excel(original_path: str,
                            output_path: str,
                            df: pd.DataFrame,
                            original_sheet_name="Original",
                            prices_sheet_name="Prices"):
    """
    Записывает исходный лист (копия) и лист с результатами.
    Превращает содержимое колонки 'Ссылка' в кликабельную гиперссылку "Открыть".
    """
    original = pd.read_excel(original_path, header=None)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        original.to_excel(writer, sheet_name=original_sheet_name, index=False, header=False)
        df.to_excel(writer, sheet_name=prices_sheet_name, index=False)

    wb = load_workbook(output_path)
    ws = wb[prices_sheet_name]

    if "Ссылка" in df.columns:
        link_col = list(df.columns).index("Ссылка") + 1
        for row in range(2, ws.max_row + 1):
            url = ws.cell(row=row, column=link_col).value
            if url and isinstance(url, str) and url.strip():
                cell = ws.cell(row=row, column=link_col)
                cell.value = "Открыть"
                cell.hyperlink = url
                cell.font = Font(color="0000FF", underline="single")

    wb.save(output_path)
    print(f"Результаты сохранены в {output_path}")


def save_cookies_pickle(driver, path: str):
    try:
        cookies = driver.get_cookies()
        with open(path, "wb") as f:
            pickle.dump(cookies, f)
        return True
    except Exception:
        return False


def load_cookies_pickle(driver, path: str, domain_filter: str = None):
    if not os.path.exists(path):
        return False
    try:
        with open(path, "rb") as f:
            cookies = pickle.load(f)
        for c in cookies:
            # optional filter domain to avoid cross-site issues
            if domain_filter and "domain" in c and domain_filter not in c["domain"]:
                continue
            # selenium expects expiry to be int (if it's float)
            if "expiry" in c:
                try:
                    c["expiry"] = int(c["expiry"])
                except Exception:
                    c.pop("expiry", None)
            try:
                driver.add_cookie(c)
            except Exception:
                # ignore cookies that can't be added
                continue
        return True
    except Exception:
        return False
