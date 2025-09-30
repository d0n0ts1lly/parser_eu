import os
import csv
import time
import random
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import concurrent.futures
import threading
import glob
import shutil
'''
# =======================
# Настройка виртуального дисплея
# =======================
# Проверяем, запущено ли на GitHub Actions
is_github_actions = os.getenv('GITHUB_ACTIONS') is not None

if is_github_actions:
    try:
        from pyvirtualdisplay import Display
        display = Display(visible=0, size=(1920, 1080))
        display.start()
        print("🖥️ Виртуальный дисплей запущен на GitHub Actions")
    except Exception as e:
        print(f"⚠ Не удалось запустить виртуальный дисплей: {e}")
else:
    print("🖥️ Локальный запуск")

# Абсолютный путь к папке загрузок
download_dir = os.path.join(os.getcwd(), "downloads")
os.makedirs(download_dir, exist_ok=True)

print(f"Current working directory: {os.getcwd()}")
print(f"Download folder: {download_dir}")
print(f"Exists? {os.path.exists(download_dir)}")
print(f"Files in download directory: {os.listdir(download_dir)}")

# Данные из GitHub Secrets
COPART_USER = os.environ["COPART_USER"]
COPART_PASS = os.environ["COPART_PASS"]
FLASK_CLEAR_URL = os.environ["FLASK_CLEAR_URL"]
FLASK_UPLOAD_URL = os.environ["FLASK_UPLOAD_URL"]

# =======================
# Selenium настройки БЕЗ headless
# =======================
chrome_path = "/usr/bin/chromium-browser"
driver_path = "/usr/bin/chromedriver"

options = webdriver.ChromeOptions()
options.binary_location = chrome_path

# УБИРАЕМ headless аргумент для работы с виртуальным дисплеем
# options.add_argument("--headless=new")  # ЗАКОММЕНТИРОВАНО

options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1920,1080")
options.add_argument("--disable-features=VizDisplayCompositor")
options.add_argument("--disable-software-rasterizer")

# Улучшенные настройки для загрузки файлов
prefs = {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": False,  # Отключаем для ускорения
    "profile.default_content_settings.popups": 0,
    "profile.content_settings.exceptions.automatic_downloads.*.setting": 1
}
options.add_experimental_option("prefs", prefs)

driver = webdriver.Chrome(service=Service(driver_path), options=options)

# Принудительно устанавливаем поведение загрузки через CDP
driver.execute_cdp_cmd(
    "Page.setDownloadBehavior",
    {"behavior": "allow", "downloadPath": download_dir}
)

wait = WebDriverWait(driver, 30)

def move_and_rename_file(original_filename, new_filename):
    """Перемещает и переименовывает скачанный файл"""
    original_path = os.path.join(download_dir, original_filename)
    new_path = os.path.join(download_dir, new_filename)
    
    if os.path.exists(original_path):
        # Ждем пока файл освободится
        time.sleep(2)
        shutil.move(original_path, new_path)
        print(f"✅ Файл переименован: {original_filename} -> {new_filename}")
        return True
    return False

# =======================
# Функция ожидания завершения загрузки
# =======================
def wait_for_download_complete(timeout=60):
    """Ожидает завершения загрузки файла"""
    import time
    
    start_time = time.time()
    last_temp_files = []
    
    while time.time() - start_time < timeout:
        # Ищем временные файлы загрузки (.crdownload или .tmp)
        temp_files = glob.glob(os.path.join(download_dir, "*.crdownload"))
        temp_files.extend(glob.glob(os.path.join(download_dir, "*.tmp")))
        
        if temp_files:
            if temp_files != last_temp_files:
                print(f"⏳ Загружаются файлы: {[os.path.basename(f) for f in temp_files]}")
                last_temp_files = temp_files
        else:
            # Если нет временных файлов, проверяем наличие CSV файлов
            csv_files = glob.glob(os.path.join(download_dir, "*.csv"))
            if csv_files:
                print(f"✅ Загрузка завершена. Файлы: {[os.path.basename(f) for f in csv_files]}")
                return True
                
        time.sleep(2)
    
    print("❌ Таймаут ожидания загрузки")
    return False

# =======================
# Функция скачивания CSV
# =======================
def dwn(file_number):
    """Скачивает CSV файл и переименовывает его с номером"""
    # Сохраняем список файлов до скачивания
    files_before = set(os.listdir(download_dir))
    print(f"📁 Файлов до скачивания: {len(files_before)}")
    
    time.sleep(3)
    
    # Нажимаем кнопку экспорта
    down_but = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".cprt-btn-white.export-csv-button")))
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", down_but)
    driver.execute_script("arguments[0].click();", down_but)
    print("✅ Кнопка экспорта нажата")
    
    time.sleep(5)
    
    # Обрабатываем кнопки OK
    ok_buttons = driver.find_elements(By.CSS_SELECTOR, ".cprt-btn-yellow")
    clicked = False
    for ok_button in ok_buttons:
        try:
            WebDriverWait(driver, 5).until(EC.element_to_be_clickable(ok_button))
            ok_button.click()
            print("✅ Кнопка OK нажата")
            clicked = True
            time.sleep(3)
            break
        except Exception as e:
            print(f"⚠ Не удалось нажать кнопку OK: {e}")
            continue
    
    if not clicked:
        print("ℹ Кнопка OK не появилась — продолжаем")
    
    # Ждем завершения загрузки
    download_success = wait_for_download_complete(timeout=45)
    
    # Проверяем результат и переименовываем файл
    files_after = set(os.listdir(download_dir))
    new_files = files_after - files_before
    
    if new_files:
        # Находим новый CSV файл
        new_csv_files = [f for f in new_files if f.endswith('.csv')]
        if new_csv_files:
            original_filename = new_csv_files[0]
            # Переименовываем с номером
            new_filename = f"copart_{file_number}.csv"
            
            original_path = os.path.join(download_dir, original_filename)
            new_path = os.path.join(download_dir, new_filename)
            
            if os.path.exists(original_path):
                os.rename(original_path, new_path)
                print(f"✅ Файл переименован в: {new_filename}")
                return True
    else:
        print("❌ Новые файлы не обнаружены")
    
    return False
    
# =======================
# Функция для логина (ИСПРАВЛЕНА)
# =======================
def login_to_copart():
    """Выполняет вход в Copart"""
    try:
        print("🔐 Выполняем вход в Copart...")
        
        email_input = wait.until(EC.presence_of_element_located((By.ID, "username")))
        email_input.clear()
        email_input.send_keys("")
        print("✅ Логин введен")
        time.sleep(2)

        password_input = wait.until(EC.presence_of_element_located((By.ID, "password")))
        password_input.clear()
        password_input.send_keys(COPART_PASS)
        print("✅ Пароль введен")
        time.sleep(2)

        login_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button.cprt-btn-yellow")))
        login_button.click()
        print("✅ Кнопка входа нажата")
        time.sleep(5)
        
        return True
    except Exception as e:
        print(f"❌ Ошибка входа: {e}")
        return False

# =======================
# Скачиваем CSV с сайта Copart
# =======================
start_time = time.perf_counter()
try:
#Germany
    driver.get("https://www.copart.de/lotSearchResults?query=&searchCriteria=%7B%22query%22:%5B%22*%22%5D,%22filter%22:%7B%7D,%22searchName%22:%22%22,%22watchListOnly%22:false,%22freeFormSearch%22:false%7D")
    time.sleep(5)

    # Закрываем куки, если есть
    try:
        cookie_accept = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
        )
        cookie_accept.click()
        print("✅ Cookie баннер закрыт")
    except:
        print("ℹ Cookie баннер не найден")

    try:
        # Ждем появление модального окна
        modal_close = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#UnAuthorizedAccessModal .close, [data-dismiss='modal']"))
        )
        modal_close.click()
        print("✅ Модальное окно закрыто")
        time.sleep(2)
    except Exception as e:
        print(f"ℹ Модальное окно не найдено или не требует закрытия: {e}")
    
    export_button = wait.until(
    EC.element_to_be_clickable((By.CSS_SELECTOR, "button.export-csv-button"))
    )
    export_button.click()

    time.sleep(5)

    # ИСПРАВЛЕНО: используем функцию логина вместо прямого ввода
    if login_to_copart():
        print("✅ Успешный вход в систему")
    else:
        print("❌ Не удалось войти в систему")

    # УБИРАЕМ лишний клик - файл УЖЕ должен скачаться после логина
    down_but = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".cprt-btn-white.export-csv-button")))
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", down_but)
    driver.execute_script("arguments[0].click();", down_but)
    time.sleep(5)

    # Ждем скачивания первого файла и переименовываем его
    if wait_for_download_complete(timeout=45):
        # Переименовываем первый файл
        csv_files = glob.glob(os.path.join(download_dir, "*.csv"))
        if csv_files:
            original_filename = os.path.basename(csv_files[0])
            new_filename = f"germany.csv"
            original_path = os.path.join(download_dir, original_filename)
            new_path = os.path.join(download_dir, new_filename)
            if os.path.exists(original_path):
                os.rename(original_path, new_path)
                print(f"✅ Первый файл переименован в: {new_filename}")


    time.sleep(5)


#Spain
    driver.get("https://www.copart.es/en/lotSearchResults?query=")
    time.sleep(5)

    # Закрываем куки, если есть
    try:
        cookie_accept = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
        )
        cookie_accept.click()
        print("✅ Cookie баннер закрыт")
    except:
        print("ℹ Cookie баннер не найден")

    try:
        # Ждем появление модального окна
        modal_close = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#UnAuthorizedAccessModal .close, [data-dismiss='modal']"))
        )
        modal_close.click()
        print("✅ Модальное окно закрыто")
        time.sleep(2)
    except Exception as e:
        print(f"ℹ Модальное окно не найдено или не требует закрытия: {e}")
    
    export_button = wait.until(
    EC.element_to_be_clickable((By.CSS_SELECTOR, "button.export-csv-button"))
    )
    export_button.click()

    time.sleep(5)

    # ИСПРАВЛЕНО: используем функцию логина вместо прямого ввода
    if login_to_copart():
        print("✅ Успешный вход в систему")
    else:
        print("❌ Не удалось войти в систему")

    # УБИРАЕМ лишний клик - файл УЖЕ должен скачаться после логина
    down_but = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".cprt-btn-white.export-csv-button")))
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", down_but)
    driver.execute_script("arguments[0].click();", down_but)
    time.sleep(5)

    # Ждем скачивания первого файла и переименовываем его
    if wait_for_download_complete(timeout=45):
        # Переименовываем первый файл
        csv_files = glob.glob(os.path.join(download_dir, "*.csv"))
        if csv_files:
            original_filename = os.path.basename(csv_files[0])
            new_filename = f"spain.csv"
            original_path = os.path.join(download_dir, original_filename)
            new_path = os.path.join(download_dir, new_filename)
            if os.path.exists(original_path):
                os.rename(original_path, new_path)
                print(f"✅ Первый файл переименован в: {new_filename}")


    time.sleep(5)

#Finland
    driver.get("https://www.copart.fi/lotSearchResults?query=")
    time.sleep(5)

    # Закрываем куки, если есть
    try:
        cookie_accept = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
        )
        cookie_accept.click()
        print("✅ Cookie баннер закрыт")
    except:
        print("ℹ Cookie баннер не найден")

    try:
        # Ждем появление модального окна
        modal_close = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#UnAuthorizedAccessModal .close, [data-dismiss='modal']"))
        )
        modal_close.click()
        print("✅ Модальное окно закрыто")
        time.sleep(2)
    except Exception as e:
        print(f"ℹ Модальное окно не найдено или не требует закрытия: {e}")
        
    export_button = wait.until(
    EC.element_to_be_clickable((By.CSS_SELECTOR, "button.export-csv-button"))
    )
    export_button.click()

    time.sleep(5)

    # ИСПРАВЛЕНО: используем функцию логина вместо прямого ввода
    if login_to_copart():
        print("✅ Успешный вход в систему")
    else:
        print("❌ Не удалось войти в систему")

    # УБИРАЕМ лишний клик - файл УЖЕ должен скачаться после логина
    down_but = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".cprt-btn-white.export-csv-button")))
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", down_but)
    driver.execute_script("arguments[0].click();", down_but)
    time.sleep(5)

    # Ждем скачивания первого файла и переименовываем его
    if wait_for_download_complete(timeout=45):
        # Переименовываем первый файл
        csv_files = glob.glob(os.path.join(download_dir, "*.csv"))
        if csv_files:
            original_filename = os.path.basename(csv_files[0])
            new_filename = f"finland.csv"
            original_path = os.path.join(download_dir, original_filename)
            new_path = os.path.join(download_dir, new_filename)
            if os.path.exists(original_path):
                os.rename(original_path, new_path)
                print(f"✅ Первый файл переименован в: {new_filename}")


    time.sleep(5)

    

except Exception as e:
    print(f"❌ Произошла ошибка: {e}")
finally:
    # Закрываем драйвер
    driver.quit()
    # Останавливаем виртуальный дисплей если он был запущен
    if is_github_actions and 'display' in locals():
        display.stop()
        print("🖥️ Виртуальный дисплей остановлен")
print(f"⏱ CSV скачаны за {time.perf_counter() - start_time:.2f} секунд")
'''


download_dir = os.path.join(os.getcwd(), "downloads")
# =======================
# Нормализация марок
# =======================
MAKE_MAP = {
    "AUDI": "AUDI",
    "BMW": "BMW",
    "CHEV": "CHEVROLET",
    "DODG": "DODGE",
    "FORD": "FORD",
    "HOND": "HONDA",
    "HYUN": "HYUNDAI",
    "INFI": "INFINITI",
    "JEP": "JEEP",
    "KIA": "KIA",
    "LAND": "LAND ROVER",
    "LEXS": "LEXUS",
    "MAZD": "MAZDA",
    "MERZ": "MERCEDES-BENZ",
    "MCRE": "MERCEDES-BENZ",
    "MITS": "MITSUBISHI",
    "NISS": "NISSAN",
    "PORS": "PORSHE",
    "RAM": "RAM",
    "SUBA": "SUBARU",
    "TESL": "TESLA",
    "TOYT": "TOYOTA",
    "VOLK": "VOLKSWAGEN",
    "VOLV": "VOLVO",
    "ACUR": "ACURA",
    "ALFA": "ALFA ROMEO",
    "CHRY": "CHRYSLER",
    "BUIC": "BUICK",
    "BENT": "BENTLEY",
    "CADI": "CADILLAC",
    "GENS": "GENESIS",
    "FIAT": "FIAT",
    "DUCA": "DUCATI",
    "GMC": "GMC",
    "LINC": "LINCOLN",
    "LUCI": "LUCID",
    "MIN": "MINI",
    "MASE": "MASERATI",
    "JAGU": "JAGUAR",
    "PLSR": "POLESTAR",
    "SUZI": "SUZUKI",
    "ISU": "ISUZU"
}

def normalize_make(make):
    make = make.strip().upper()
    return MAKE_MAP.get(make, make)

# =======================
# Чистим таблицу через Flask API
# =======================
flask_clear_url = 'http://www.bwauto.com.ua/clear_table_eu'
flask_upload_url = 'http://www.bwauto.com.ua/upload_data_eu'

start_clear = time.perf_counter()
response = requests.post(flask_clear_url)
if response.ok:
    print(f"✅ Таблица cars очищена за {time.perf_counter() - start_clear:.2f} секунд")
else:
    print(f"❌ Ошибка очистки: {response.text}")

# =======================
# Читаем CSV и собираем данные
# =======================
all_data = []
start_csv = time.perf_counter()

for file_name in os.listdir(download_dir):
    if not file_name.endswith(".csv"):
        continue

    file_path = os.path.join(download_dir, file_name)
    print(f"Обработка файла: {file_name}")

    if "germany" in file_name.lower() in file_name.lower():
        country = "Germany"
    elif "spain" in file_name.lower() in file_name.lower():
        country = "Spain"
    elif "finland" in file_name.lower() in file_name.lower():
        country = "Finland"
    else:
        country = "Unknown"  # или значение по умолчанию

    with open(file_path, mode='r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        for row in reader:
            if not row or len(row) != 21:
                continue
            row[5] = normalize_make(row[5])
            sort_order_value = random.randint(1, 1_000_000)
            all_data.append({
                "lot_url": row[0],
                "lot_number": row[1],
                "retail_value": row[2],
                "sale_date": row[3],
                "year": int(row[4]) if row[4].isdigit() else None,
                "make": row[5],
                "model": row[6],
                "engine": row[7],
                "cylinders": row[8],
                "vin": row[9],
                "setka": row[10],
                "title": row[11],
                "category": row[12],
                "odometer": row[13],
                "odometer_desc": row[14],
                "damage": row[15],
                "current_bid": row[16],
                "my_bid": row[17],
                "item_number": row[18],
                "sale_name": row[19],
                "remont": row[20],
                "country": country,
                "sort_order": sort_order_value
            })
    os.remove(file_path)
    print(f"Файл {file_name} удален")

print(f"⏱ CSV обработаны за {time.perf_counter() - start_csv:.2f} секунд")
print(f"Всего записей: {len(all_data)}")

import concurrent.futures
import threading

batch_size = 200
batches = [all_data[i:i+batch_size] for i in range(0, len(all_data), batch_size)]
total_batches = len(batches)

batch_list = list(enumerate(batches, start=1))  # [(1, batch1), (2, batch2), ...]

inserted_total, failed_total = 0, 0
lock = threading.Lock()

def send_batch(batch_num, batch):
    global inserted_total, failed_total
    try:
        print(f"⏳ Отправка пакета {batch_num}/{total_batches}...")
        response = requests.post(flask_upload_url, json=batch)
        if response.ok:
            inserted = len(batch)
            failed = 0
            print(f"✅ Пакет {batch_num}/{total_batches} успешно вставлен ({inserted} записей)")
        else:
            inserted = 0
            failed = len(batch)
            print(f"❌ Ошибка вставки пакета {batch_num}: {response.text}")
    except Exception as e:
        inserted = 0
        failed = len(batch)
        print(f"❌ Exception в пакете {batch_num}: {e}")

    with lock:
        inserted_total += inserted
        failed_total += failed
        print(f"ℹ Прогресс: {inserted_total} вставлено, {failed_total} неудачных")

    return inserted, failed

# ============================
# Параллельная отправка
# ============================
start_upload = time.perf_counter()

with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
    results = executor.map(lambda args: send_batch(*args), batch_list)

for inserted, failed in results:
    pass  # счётчики уже обновляются внутри send_batch

print(f"✅ Данные отправлены за {time.perf_counter() - start_upload:.2f} секунд")
print(f"Всего вставлено: {inserted_total}, неудачных пакетов: {failed_total}")
