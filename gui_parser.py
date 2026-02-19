# gui_parser.py - ВЕРСИЯ С ВЫБОРОМ ПУТИ ВЫВОДА И РАСЧЁТОМ РАЗНИЦЫ ЦЕН

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import threading
import pandas as pd
import os
import time
from datetime import datetime

try:
    from tender_parser import get_prices as get_prices_yandex
    from ozon_parser import get_prices as get_prices_ozon
    from utils import extract_products_from_excel, save_results_into_tender_format
except ImportError as e:
    print(f"Ошибка импорта: {e}")
    exit(1)

def kill_all_edge_processes():
    """Принудительно убивает все процессы Edge"""
    try:
        import psutil
        killed = 0
        for proc in psutil.process_iter(['pid', 'name']):
            try:
                if proc.info['name'] and 'msedge' in proc.info['name'].lower():
                    proc.kill()
                    killed += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        if killed > 0:
            print(f"🔪 Убито {killed} процессов Edge")
            time.sleep(2)
        return killed
    except ImportError:
        print("⚠️ psutil не установлен, пропускаем убийство процессов")
        return 0

class ParserGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Парсер Яндекс Маркет")
        self.root.geometry("950x750")
        
        # Переменные
        self.input_file = tk.StringVar(value="1.xlsx")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.output_file = tk.StringVar(value=f"results_{timestamp}.xlsx")
        self.output_dir = tk.StringVar(value="./")
        self.headless_mode = tk.BooleanVar(value=False)
        self.marketplace = tk.StringVar(value="yandex")  # yandex, ozon, both
        
        # Данные
        self.products_list = []
        self.yandex_results = {}
        self.ozon_results = {}
        self.is_parsing = False
        
        self.create_ui()
    
    def create_ui(self):
        # ==================== ВХОДНОЙ ФАЙЛ ====================
        file_frame = ttk.LabelFrame(self.root, text="Входной файл (с тендером)", padding=10)
        file_frame.pack(fill=tk.X, padx=10, pady=5)
        
        ttk.Entry(file_frame, textvariable=self.input_file, width=60).pack(side=tk.LEFT, padx=5)
        ttk.Button(file_frame, text="Обзор...", command=self.browse_input).pack(side=tk.LEFT, padx=5)
        
        # ==================== ВЫХОДНОЙ ПУТь ====================
        output_frame = ttk.LabelFrame(self.root, text="Пут​ь для сохранения результатов", padding=10)
        output_frame.pack(fill=tk.X, padx=10, pady=5)
        
        ttk.Entry(output_frame, textvariable=self.output_dir, width=60).pack(side=tk.LEFT, padx=5)
        ttk.Button(output_frame, text="Обзор...", command=self.browse_output_dir).pack(side=tk.LEFT, padx=5)
        
        # ==================== ИМЯФАЙЛА ====================
        filename_frame = ttk.LabelFrame(self.root, text="Имя файла результата", padding=10)
        filename_frame.pack(fill=tk.X, padx=10, pady=5)
        
        ttk.Entry(filename_frame, textvariable=self.output_file, width=60).pack(side=tk.LEFT, padx=5)
        
        # ==================== НАСТРОЙКИ ====================
        settings_frame = ttk.LabelFrame(self.root, text="Настройки", padding=10)
        settings_frame.pack(fill=tk.X, padx=10, pady=5)
        
 #       ttk.Checkbutton(settings_frame, text="Headless режим (без окна браузера)",
 #                      variable=self.headless_mode).pack(anchor=tk.W)
        
         # Маркетплейс
        mp_frame = ttk.LabelFrame(self.root, text="Маркетплейс", padding=10)
        mp_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Radiobutton(mp_frame, text="Яндекс Маркет",
                       variable=self.marketplace, value="yandex").pack(side=tk.LEFT, padx=5)
        ttk.Radiobutton(mp_frame, text="Ozon",
                       variable=self.marketplace, value="ozon").pack(side=tk.LEFT, padx=5)
        ttk.Radiobutton(mp_frame, text="Оба (последовательно)",
                       variable=self.marketplace, value="both").pack(side=tk.LEFT, padx=5)

        # ==================== КНОПКИ ====================
        btn_frame = ttk.Frame(self.root)
        btn_frame.pack(fill=tk.X, padx=10, pady=10)
        
        self.start_btn = ttk.Button(btn_frame, text="▶ Запустить парсинг", 
                                    command=self.start_parsing)
        self.start_btn.pack(side=tk.LEFT, padx=5)
        
        ttk.Button(btn_frame, text="💾 Сохранить вручную", 
                  command=self.save_results).pack(side=tk.LEFT, padx=5)
        
        # ==================== ЛОГ ====================
        log_frame = ttk.LabelFrame(self.root, text="Лог парсинга", padding=5)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        self.log = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, height=20, font=("Courier", 9))
        self.log.pack(fill=tk.BOTH, expand=True)
    
    def browse_input(self):
        f = filedialog.askopenfilename(filetypes=[("Excel", "*.xlsx"), ("All", "*.*")])
        if f:
            self.input_file.set(f)
    
    def browse_output_dir(self):
        d = filedialog.askdirectory(title="Выберите папку для сохранения результатов")
        if d:
            self.output_dir.set(d)
    
    def log_msg(self, msg):
        self.log.insert(tk.END, f"{msg}\n")
        self.log.see(tk.END)
        self.root.update()
    
    def start_parsing(self):
        if self.is_parsing:
            messagebox.showwarning("Внимание", "Парсинг уже запущен")
            return
        
        if not os.path.exists(self.input_file.get()):
            messagebox.showerror("Ошибка", "Входной файл не найден")
            return
        
        self.is_parsing = True
        self.start_btn.config(state=tk.DISABLED)
        self.yandex_results.clear()
        self.ozon_results.clear()
        self.log.delete(1.0, tk.END)
        
        thread = threading.Thread(target=self.parse_worker, daemon=True)
        thread.start()
    
    def parse_worker(self):
        try:
            # Читаем товары
            df = extract_products_from_excel(self.input_file.get())
            self.products_list = df["name"].tolist()
            self.log_msg(f"✅ Найдено {len(self.products_list)} товаров\n")
            
            mode = self.marketplace.get()
            headless = self.headless_mode.get()
            
            # Парсим каждый товар
            for i, name in enumerate(self.products_list, 1):
                self.log_msg(f"[{i}/{len(self.products_list)}] {name[:50]}...")
                
                # Яндекс Маркет
                if mode in ["yandex", "both"]:
                    self.log_msg("  🔍 Яндекс Маркет...")
                    try:
                        result = get_prices_yandex(name, headless=headless, timeout=20, use_business_auth=True)
                        self.yandex_results[i] = {
                            "цена": result.get("цена", ""),
                            "цена для юрлиц": result.get("цена для юрлиц", ""),
                            "ссылка": result.get("ссылка", "")
                        }
                        
                        if result.get("цена"):
                            self.log_msg(f"  ✅ {result['цена']}")
                        else:
                            self.log_msg("  ❌ Не найдено")
                    except Exception as e:
                        self.log_msg(f"  ❌ Ошибка: {e}")
                        self.yandex_results[i] = {"цена": "", "цена для юрлиц": "", "ссылка": ""}
                
                # Убиваем Edge процессы перед Ozon
                if mode == "both":
                    self.log_msg("  🔪 Очистка Edge процессов...")
                    kill_all_edge_processes()
                    time.sleep(2)
                
                # Ozon
                if mode in ["ozon", "both"]:
                    self.log_msg("  🔍 Ozon...")
                    try:
                        result = get_prices_ozon(name, headless, None, 20)
                        self.ozon_results[i] = {
                            "цена": result.get("цена", ""),
                            "цена для юрлиц": result.get("цена для юрлиц", ""),
                            "ссылка": result.get("ссылка", "")
                        }
                        
                        if result.get("цена"):
                            self.log_msg(f"  ✅ {result['цена']}")
                        else:
                            self.log_msg("  ❌ Не найдено")
                    except Exception as e:
                        self.log_msg(f"  ❌ Ошибка: {e}")
                        self.ozon_results[i] = {"цена": "", "цена для юрлиц": "", "ссылка": ""}
                
                self.log_msg("")
            
            self.log_msg("\n✅ Парсинг завершён!")
            self.save_results()
            
        except Exception as e:
            self.log_msg(f"\n❌ Критическая ошибка: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.is_parsing = False
            self.start_btn.config(state=tk.NORMAL)
            
            # Финальная очистка всех Edge процессов
#            kill_all_edge_processes()
    
    def save_results(self):
        if not self.products_list:
            messagebox.showwarning("Внимание", "Нет данных для сохранения")
            return
        
        try:
            self.log_msg("\n💾 Сохранение результатов...")
            
            # Конечный путь файла
            output_dir = self.output_dir.get()
            output_filename = self.output_file.get()
            output_path = os.path.join(output_dir, output_filename)
            
            self.log_msg(f"📁 Путь вывода: {output_path}")
            
            mode = self.marketplace.get()
            
            # Сохраняем Яндекс Маркет
            if mode in ["yandex", "both"] and self.yandex_results:
                y_data = []
                for i, name in enumerate(self.products_list, 1):
                    res = self.yandex_results.get(i, {"цена": "", "цена для юрлиц": "", "ссылка": ""})
                    y_data.append({
                        "наименование": name,
                        "цена": res["цена"],
                        "цена для юрлиц": res["цена для юрлиц"],
                        "ссылка": res["ссылка"]
                    })
                
                df_y = pd.DataFrame(y_data)
                
                # Используем НОВЫЙ utils с расчётом разницы
                save_results_into_tender_format(
                    self.input_file.get(),
                    output_path,
                    df_y,
                    column_name="Яндекс Маркет"
                )
                
                self.log_msg("✅ Колонка 'Яндекс Маркет' + 'Разница' сохранена")
            
            # Сохраняем Ozon
            if mode in ["ozon", "both"] and self.ozon_results:
                o_data = []
                for i, name in enumerate(self.products_list, 1):
                    res = self.ozon_results.get(i, {"цена": "", "цена для юрлиц": "", "ссылка": ""})
                    o_data.append({
                        "наименование": name,
                        "цена": res["цена"],
                        "цена для юрлиц": res["цена для юрлиц"],
                        "ссылка": res["ссылка"]
                    })
                
                df_o = pd.DataFrame(o_data)
                
                # Используем НОВЫЙ utils с расчётом разницы
                save_results_into_tender_format(
                    self.input_file.get(),
                    output_path,
                    df_o,
                    column_name="Ozon"
                )
                
                self.log_msg("✅ Колонка 'Ozon' + 'Разница' сохранена")
            
            self.log_msg(f"\n🎉 Файл сохранён: {output_path}")
            messagebox.showinfo("Успех", f"Результаты сохранены!\n\n{output_path}")
            
        except Exception as e:
            self.log_msg(f"\n❌ Ошибка сохранения: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    root = tk.Tk()
    app = ParserGUI(root)
    root.mainloop()

