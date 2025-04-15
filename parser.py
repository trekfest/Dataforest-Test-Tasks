import requests
from bs4 import BeautifulSoup
import sqlite3
import threading
import queue
import os

# Очереди для задач и результатов
task_queue = queue.Queue()
result_queue = queue.Queue()

# Имя файла базы данных
DB_NAME = "vendr_products.db"


def clear_db():
    # Удаляем базу данных, если она существует
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)
        print("Файл базы данных удалён.")


def create_db():
    # Создаём базу данных и таблицу, если её ещё нет
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            category TEXT,
            price_range TEXT,
            description TEXT
        )
    ''')
    conn.commit()
    conn.close()
    print("База данных готова.")


def get_product_links_from_category(category_name, category_url):
    # Получаем ссылки на продукты из указанной категории
    print(f"Получаем продукты из категории: {category_name}")
    try:
        response = requests.get(category_url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        product_links = []
        cards = soup.find_all('a', href=True)
        for card in cards:
            href = card['href']
            if href.startswith("/marketplace/"):
                # Формируем полный URL продукта
                full_url = f"https://www.vendr.com{href}"
                product_links.append({'url': full_url, 'category': category_name})
                print(f"   └─ Найден продукт: {full_url}")
        return product_links
    except Exception as e:
        print(f"[!] Ошибка при загрузке категории {category_url}: {e}")
        return []


def parse_product_page(task):
    # Парсим страницу продукта
    url = task['url']
    category = task['category']
    print(f" Парсим продукт: {url}")
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Название продукта
        title_tag = soup.find("h1", class_="rt-Heading")
        name = title_tag.text.strip() if title_tag else "No name"

        # Описание продукта
        desc_tag = soup.find("p", class_="rt-Text")
        description = desc_tag.text.strip() if desc_tag else "No description"

        # Средняя цена
        median_price = "N/A"
        try:
            median = soup.find("span", string=lambda x: x and "Median" in x)
            median_price = median.find_next("span").text.strip() if median else "N/A"
        except Exception:
            pass

        # Минимальная цена
        low_price = "N/A"
        try:
            low_tag = soup.find("span", class_="v-fw-600 v-fs-12")
            low_price = low_tag.text.strip() if low_tag else "N/A"
        except Exception:
            pass

        # Максимальная цена
        high_price = "N/A"
        try:
            high_tag = soup.find("span", class_=lambda x: x and x.startswith("_rangeSliderLastNumber"))
            if high_tag:
                high_price = high_tag.text.strip()
            else:
                prices = soup.find_all("span", class_="v-fw-600 v-fs-12")
                if len(prices) > 1:
                    high_price = prices[1].text.strip()
        except Exception:
            pass

        # Формируем диапазон цен
        price_range = f"{low_price} - {high_price}, Median: {median_price}"

        print(f" {name} | {price_range}")
        return {
            'product_name': name,
            'category': category,
            'price_range': price_range,
            'description': description
        }

    except Exception as e:
        print(f" Ошибка при парсинге {url}: {e}")
        return None


def worker():
    # Поток для обработки задач из очереди
    while True:
        task = task_queue.get()
        if task is None:
            break
        result = parse_product_page(task)
        if result:
            result_queue.put(result)
        task_queue.task_done()


def db_writer():
    # Поток для записи данных в базу данных
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    while True:
        data = result_queue.get()
        if data is None:
            break
        try:
            cursor.execute('''
                INSERT INTO products (name, category, price_range, description)
                VALUES (?, ?, ?, ?)
            ''', (data['product_name'], data['category'], data['price_range'], data['description']))
            conn.commit()
            print(f" Сохранено: {data['product_name']}")
        except Exception as e:
            print(f"Ошибка при записи в БД: {e}")
        result_queue.task_done()
    conn.close()
    print(" Поток записи завершён.")


def main():
    # Основная функция
    clear_db()
    create_db()

    # Категории для парсинга
    categories = {
        "DevOps": "https://www.vendr.com/categories/devops",
        "IT Infrastructure": "https://www.vendr.com/categories/it-infrastructure",
        "Data Analytics and Management": "https://www.vendr.com/categories/data-analytics-and-management"
    }

    # Получаем ссылки на продукты и добавляем их в очередь задач
    for name, url in categories.items():
        links = get_product_links_from_category(name, url)
        for task in links:
            task_queue.put(task)

    # Запускаем потоки для обработки задач
    workers = []
    for _ in range(5):  # 5 потоков для парсинга
        t = threading.Thread(target=worker)
        t.start()
        workers.append(t)

    # Поток для записи в базу данных
    db_thread = threading.Thread(target=db_writer)
    db_thread.start()

    # Ожидаем завершения всех задач
    task_queue.join()
    for _ in workers:
        task_queue.put(None)
    for t in workers:
        t.join()

    # Завершаем поток записи в базу данных
    result_queue.put(None)
    result_queue.join()
    db_thread.join()

    print("\n Все данные собраны и записаны. Работа завершена.")


if __name__ == "__main__":
    main()