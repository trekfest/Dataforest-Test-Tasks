import multiprocessing
import time
from playwright.sync_api import sync_playwright
from multiprocessing import Manager, Process
from db import init_db, save_book
import threading

BASE_URL = "https://books.toscrape.com"
error_log_lock = threading.Lock()


def log_error(url, reason=""):
    with error_log_lock:
        with open("errors.txt", "a", encoding="utf-8") as f:
            f.write(f"{url}  # {reason}\n")


class BookScraperProcess(Process):
    def __init__(self, task_queue, name):
        super().__init__()
        self.task_queue = task_queue
        self.name = name

    def run(self):
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            while True:
                try:
                    url = self.task_queue.get(timeout=3)
                except:
                    break

                try:
                    print(f"[{self.name}] Парсим: {url}")
                    page.goto(url, wait_until="domcontentloaded", timeout=20000)

                    if "404" in page.title():
                        print(f"[{self.name}]  Страница содержит 404: {url}")
                        log_error(url, "Title contains 404")
                        continue

                    data = self.parse_book(page)
                    if data:
                        save_book(data)
                except Exception as e:
                    print(f"[{self.name}]  Ошибка на {url}: {e}")
                    log_error(url, f"Exception: {str(e)}")
            browser.close()

    def parse_book(self, page):
        try:
            title = page.locator("h1").inner_text()
            if title.strip().lower() == "404 not found":
                print(f"[{self.name}]  Страница — это заглушка 404")
                log_error(page.url, "H1 = 404 Not Found")
                return None
        except:
            print(f"[{self.name}]  Не удалось получить заголовок")
            log_error(page.url, "Exception reading <h1>")
            return None

        def safe_text(selector):
            try:
                return page.locator(selector).first.inner_text().strip()
            except:
                return "N/A"

        category = safe_text(".breadcrumb li:nth-child(3)")
        price = safe_text(".price_color")
        availability = safe_text(".instock.availability")
        try:
            rating_class = page.locator(".star-rating").first.get_attribute("class") or ""
            rating = rating_class.split()[-1] if "star-rating" in rating_class else "N/A"
        except:
            rating = "N/A"

        image_url = page.locator(".item.active img").get_attribute("src") or ""
        image_url = BASE_URL + image_url.replace("..", "")

        try:
            description = ""
            if page.locator("#product_description + p").count():
                description = page.locator("#product_description + p").inner_text()
        except:
            description = ""

        product_info = {}
        try:
            rows = page.locator("table.table.table-striped tr")
            for i in range(rows.count()):
                label = rows.nth(i).locator("th").inner_text()
                value = rows.nth(i).locator("td").inner_text()
                product_info[label] = value
        except:
            pass

        print(f"[{self.name}]  {title} | {category} | {price}")

        return {
            "title": title,
            "category": category,
            "price": price,
            "availability": availability,
            "rating": rating,
            "image_url": image_url,
            "description": description,
            "product_info": product_info
        }


def get_all_book_urls():
    urls = []
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(BASE_URL)
        while True:
            books = page.locator(".product_pod h3 a")
            for i in range(books.count()):
                href = books.nth(i).get_attribute("href")
                href = href.replace("../../../", "")
                full_url = f"{BASE_URL}/catalogue/{href}"
                urls.append(full_url)
            if page.locator(".next a").count() > 0:
                page.locator(".next a").click()
                page.wait_for_timeout(1000)
            else:
                break
        browser.close()
    return urls


class ProcessManager:
    def __init__(self, num=3):
        self.num = num
        self.manager = Manager()
        self.task_queue = self.manager.Queue()
        self.processes = []

    def add_tasks(self, tasks):
        for task in tasks:
            self.task_queue.put(task)

    def start(self):
        for i in range(self.num):
            proc = BookScraperProcess(self.task_queue, f"Worker-{i+1}")
            proc.start()
            self.processes.append(proc)

    def monitor(self):
        while any(p.is_alive() for p in self.processes):
            time.sleep(2)
        print("Парсинг завершён.")


def main():
    init_db()
    print("[+] Собираем ссылки на все книги...")
    urls = get_all_book_urls()
    print(f"Найдено книг: {len(urls)}")

    open("errors.txt", "w").close()  # очистим старый лог

    manager = ProcessManager(num=3)
    manager.add_tasks(urls)
    manager.start()
    manager.monitor()


if __name__ == "__main__":
    main()
