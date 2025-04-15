import os
from playwright.sync_api import sync_playwright
from db import save_book, init_db

BASE_URL = "https://books.toscrape.com"
ERROR_FILE = "errors.txt"
FIXED_RETRY_LOG = "errors_retry_fixed.txt"


def fix_catalogue_urls():
    """Возвращает список исправленных URL из errors.txt"""
    if not os.path.exists(ERROR_FILE):
        return []

    with open(ERROR_FILE, "r", encoding="utf-8") as f:
        raw_lines = f.readlines()

    fixed_urls = []
    for line in raw_lines:
        url = line.split("#")[0].strip()
        if "/catalogue/catalogue/" in url:
            fixed_url = url.replace("/catalogue/catalogue/", "/catalogue/")
            fixed_urls.append(fixed_url)

    return fixed_urls


def parse_book(page, url):
    """Парсит страницу книги и возвращает словарь"""
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=15000)

        if "404" in page.title():
            print(f" Пропущено (404): {url}")
            return None

        title = page.locator("h1").inner_text()

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
            rating = rating_class.split()[-1]
        except:
            rating = "N/A"

        image = page.locator(".item.active img").get_attribute("src") or ""
        image_url = BASE_URL + image.replace("..", "")

        try:
            description = ""
            if page.locator("#product_description + p").count():
                description = page.locator("#product_description + p").inner_text()
        except:
            description = ""

        product_info = {}
        rows = page.locator("table.table.table-striped tr")
        for i in range(rows.count()):
            key = rows.nth(i).locator("th").inner_text()
            val = rows.nth(i).locator("td").inner_text()
            product_info[key] = val

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

    except Exception as e:
        print(f" Ошибка на {url}: {e}")
        with open(FIXED_RETRY_LOG, "a", encoding="utf-8") as logf:
            logf.write(f"{url}  # {e}\n")
        return None


def main():
    urls = fix_catalogue_urls()
    if not urls:
        print(" Нет ссылок для повторной обработки.")
        return

    print(f"Начинаем повторную обработку {len(urls)} ссылок...")

    init_db()
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()

        for url in urls:
            print(f"[→] {url}")
            data = parse_book(page, url)
            if data:
                save_book(data)
                print(f" Добавлено: {data['title']}")
            else:
                print(f" Не удалось распарсить: {url}")

        browser.close()

    print("Повторная обработка завершена.")


if __name__ == "__main__":
    main()
