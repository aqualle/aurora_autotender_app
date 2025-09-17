import pandas as pd
import re


def normalize(text: str) -> str:
    """Приводим строку к нижнему регистру, убираем лишние пробелы и знаки."""
    if not isinstance(text, str):
        return ""
    text = text.lower()
    text = re.sub(r"[^a-zа-я0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def debug_print_excel_rows(path: str, n: int = 50):
    """Для отладки: печатает первые n строк Excel."""
    df = pd.read_excel(path, header=None)
    print("\n=== Первые строки Excel ===")
    for i, row in df.head(n).iterrows():
        print(f"{i}: {[str(x) for x in row.tolist()]}")


def extract_products_from_excel(path: str):
    """
    Ищет лист с колонкой 'Наименование...' и возвращает товары.
    """
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

    # ищем конец (строка 'Итого без НДС')
    for i, val in enumerate(found_df[col_index]):
        if isinstance(val, str) and "итого без ндс" in val.lower():
            end_row = i
            break
    if end_row is None:
        raise ValueError("Не найден конец таблицы ('Итого без НДС')")

    # собираем товары
    items = []
    for text in found_df.loc[start_row:end_row - 1, col_index]:
        if not isinstance(text, str):
            continue
        if text.lower().startswith("возможность поставки") or text.lower().startswith("валюта"):
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
    Сохраняет результат:
    - лист Original (копия исходника)
    - лист Prices (результаты парсинга)
    """
    original = pd.read_excel(original_path, header=None)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        original.to_excel(writer, sheet_name=original_sheet_name, index=False, header=False)
        df.to_excel(writer, sheet_name=prices_sheet_name, index=False)
    print(f"✅ Результаты сохранены в {output_path}")
