from tender_parser import parse_tender_excel
from utils import debug_print_excel_rows

if __name__ == "__main__":
    INPUT = "tender_list.xlsx"
    OUTPUT = "tender_list_result.xlsx"
    HEADLESS = False
    WORKERS = 1
    DRIVER_PATH = "chromedriver/chromedriver.exe"

    debug_print_excel_rows(INPUT, n=30)
    parse_tender_excel(INPUT, OUTPUT, headless=HEADLESS, workers=WORKERS, driver_path=DRIVER_PATH)
