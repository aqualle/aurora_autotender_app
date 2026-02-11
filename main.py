# main.py - ИСПРАВЛЕНО

import argparse
import time
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tender_parser import parse_tender_excel
from utils import extract_products_from_excel

def show_banner():
    banner = f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║ 🔍 Парсер Яндекс.Маркет - Microsoft Edge                                   ║
║                                                                              ║
║ 💰 Обычная цена • 🏷️ Цена без карты • 💼 Цена для юрлиц                   ║
║ 🚗 Использует локальный msedgedriver.exe                                   ║
║ 📊 Извлечение из ds-valueLine по порядку (1-й, 2-й, 3-й)                   ║
║ 🍪 Поддержка авторизации через Edge cookies                                ║
║                                                                              ║
║ Время запуска: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}                                ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""
    print(banner)

def check_edge_driver():
    driver_path = os.path.join("browserdriver", "msedgedriver.exe")
    if os.path.exists(driver_path):
        print(f"✅ Edge WebDriver найден: {driver_path}")
        return True
    else:
        print(f"❌ Edge WebDriver НЕ НАЙДЕН: {driver_path}")
        print("📥 Скачайте msedgedriver.exe")
        print("📁 Поместите файл в: ./browserdriver/msedgedriver.exe")
        return False

def check_cookies():
    cookies_file = os.path.expanduser("~/.yandex_parser_auth/cookies.json")
    if os.path.exists(cookies_file):
        file_size = os.path.getsize(cookies_file)
        print(f"✅ Cookies для юрлиц найдены: {cookies_file} ({file_size} байт)")
        return True
    else:
        print(f"❌ Cookies не найдены: {cookies_file}")
        return False

def check_gui_modules():
    """Проверяет наличие GUI модулей"""
    try:
        import tkinter
        print("✅ GUI модули загружены")
        return True
    except ImportError:
        print("❌ tkinter не установлен")
        return False

def check_multiprocessing():
    """Проверяет поддержку многопоточности"""
    try:
        import multiprocessing
        print("✅ Многопоточность поддерживается")
        return True
    except:
        print("❌ Многопоточность не поддерживается")
        return False

def main():

    if getattr(sys, 'frozen', False) and "--gui" not in sys.argv:
        sys.argv.append("--gui")

    show_banner()
    
    parser = argparse.ArgumentParser(description="Парсер цен Яндекс.Маркет")
    parser.add_argument("input_file", nargs="?", default="tender_list.xlsx")
    parser.add_argument("-o", "--output", default="auto")
    parser.add_argument("--gui", action="store_true", help="Запустить графический интерфейс")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--no-headless", action="store_true")
    parser.add_argument("--driver-path", default=None)
    parser.add_argument("--auth", action="store_true")
    parser.add_argument("--no-auto-save", action="store_true")


    args = parser.parse_args()
    use_business_auth = args.auth
    
    print("🔍 Проверяю зависимости...")
    
    if not check_edge_driver():
        print("\n❌ Критическая ошибка: Edge WebDriver не найден")
        return 1
    
    if args.auth:
        print("\n🔐 Режим: авторизация для всех типов цен")
        if not check_cookies():
            print("⚠️ Cookies не найдены, будут только базовые цены")
    
    # GUI режим
    if args.gui:
        print("\n🖥️ Запускаю GUI...")
        
        # Проверяем GUI модули
        if not check_gui_modules():
            print("❌ Ошибка: не удалось загрузить GUI модули")
            return 1
        
        if not check_multiprocessing():
            print("⚠️ Многопоточность недоступна")
        
        try:
            # ИСПРАВЛЕНИЕ: импортируем ParserGUI вместо YandexMarketGUI
            from gui_parser import ParserGUI
            import tkinter as tk
            
            root = tk.Tk()
            app = ParserGUI(root)
            print("✅ GUI запущен")
            root.mainloop()
        except ImportError as e:
            print(f"❌ Ошибка импорта gui_parser: {e}")
            return 1
        except Exception as e:
            print(f"❌ Ошибка GUI: {e}")
            import traceback
            traceback.print_exc()
            return 1
        
        return 0
    
    # Консольный режим
    print("\n🔍 Консольный режим...")
    
    if not os.path.exists(args.input_file):
        print(f"❌ Входной файл не найден: {args.input_file}")
        return 1
    
    try:
        products_df = extract_products_from_excel(args.input_file)
        if products_df.empty:
            print(f"❌ Товары не найдены в файле: {args.input_file}")
            return 1
        
        print(f"📦 Найдено товаров: {len(products_df)}")
        
        for i, name in enumerate(products_df['name'].head(3), 1):
            short_name = name[:50] + "..." if len(name) > 50 else name
            print(f"  {i}. {short_name}")
        
        if args.output == "auto":
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            auth_suffix = "_auth" if args.auth else ""
            output_file = f"results_{args.workers}workers{auth_suffix}_{timestamp}.xlsx"
        else:
            output_file = args.output
        
        headless = not args.no_headless
        auto_save = not args.no_auto_save
        
        print(f"\n⚙️ Настройки:")
        print(f"  🧵 Потоков: {args.workers}")
        print(f"  👁️ Режим: {'скрытый' if headless else 'видимый'}")
        print(f"  🔐 Авторизация: {'да' if args.auth else 'нет'}")
        print(f"  💾 Автосохранение: {'да' if auto_save else 'нет'}")
        print(f"  📄 Выходной файл: {output_file}")
        
        print(f"\n🚀 Начинаю парсинг...")
        start_time = time.time()
        
        result_df = parse_tender_excel(
            args.input_file,
            output_file,
            headless=headless,
            workers=args.workers,
            driver_path=args.driver_path,
            auto_save=auto_save,
            use_business_auth=args.auth
        )
        
        end_time = time.time()
        duration = end_time - start_time
        
        total = len(result_df)
        regular_count = len([r for r in result_df['цена'] if r and r != 'ОШИБКА'])
        
        print(f"\n🎉 Парсинг завершен!")
        print(f"⏱️ Время: {duration:.1f} сек")
        print(f"📊 Статистика:")
        print(f"  📦 Всего товаров: {total}")
        print(f"  💰 Обычных цен: {regular_count}")
        
        if args.auth:
            business_count = len([r for r in result_df.get('цена для юрлиц', []) if r and r != 'ОШИБКА'])
            print(f"  💼 Цен для юрлиц: {business_count}")
        
        print(f"  📄 Результаты: {output_file}")
        
        return 0
        
    except KeyboardInterrupt:
        print("\n⚠️ Парсинг прерван")
        return 1
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = main()
    if os.name == 'nt':
        print('nt')
    sys.exit(exit_code)
