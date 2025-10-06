# pip install playwright
# python -m playwright install chromium

from playwright.sync_api import sync_playwright

URL = "https://www.legis.iowa.gov/legislation/BillBook?ga=84&ba=HF27"

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(user_agent="Mozilla/5.0 (date-scrape)")
        page.goto(URL, timeout=60000)
        # expand history if collapsed
        try:
            page.locator("a.actionWidgetExpand").first.click(timeout=2000)
        except:
            pass

        rows = page.locator("div.billAction table.billActionTable tbody tr")
        n = rows.count()
        introduced = []
        for i in range(n):
            tds = rows.nth(i).locator("td")
            date = tds.nth(0).inner_text().strip()
            action = tds.nth(1).inner_text().strip()
            if "introduced" in action.lower():
                introduced.append(date)

        if introduced:
            # earliest (by M/D/YYYY)
            def key(d):
                m, d2, y = [int(x) for x in d.split("/")]
                return (y, m, d2)
            introduced.sort(key=key)
            print("Introduced dates:", ", ".join(introduced))
            print("Earliest Introduced:", introduced[0])
        else:
            print("Introduced date: (not found)  — table rows parsed:", n)

        browser.close()

if __name__ == "__main__":
    main()
