import asyncio
from playwright.async_api import Playwright, async_playwright, TimeoutError as PlaywrightTimeoutError
import csv


OUTPUT_CSV = "tradingview_symbols.csv"


async def load_all_rows(page):
    # Ensure initial content is present
    await page.wait_for_selector('a[href*="/symbols/"]', timeout=60000)

    async def get_symbol_count():
        return await page.evaluate('document.querySelectorAll(\'a[href*="/symbols/"]\').length')

    # Helper to scroll every scrollable container to bottom
    async def scroll_all_scrollables():
        await page.evaluate("""
        (async () => {
            const sleep = (ms)=>new Promise(r=>setTimeout(r, ms));

            const scrollables = Array.from(document.querySelectorAll('*'))
                .filter(el => {
                    const s = getComputedStyle(el);
                    return (s.overflowY === 'auto' || s.overflowY === 'scroll') && el.scrollHeight > el.clientHeight;
                });

                // Incremental scroll for each container
        for (const el of scrollables) {
            let lastScroll = -1;
            while (true) {
                el.scrollBy(0, 400);  // scroll 400px each time
                await sleep(300);      // wait a bit for lazy loading
                if (el.scrollTop === lastScroll) break;
                lastScroll = el.scrollTop;
            }
        }

        // Incremental scroll window
        let lastWindow = -1;
        while (window.scrollY + window.innerHeight < document.body.scrollHeight) {
            window.scrollBy(0, 400);
            await sleep(300);
            if (window.scrollY === lastWindow) break;
            lastWindow = window.scrollY;
        }
        })();
        """)
        # Also press End key to poke any lazy loaders
        try:
            await page.keyboard.press("End")
        except Exception:
            pass

    # Scroll until the count stabilizes
    stable_cycles = 0
    prev_count = -1
    while stable_cycles < 5:
        await scroll_all_scrollables()
        await page.wait_for_timeout(3000)  # Slightly longer wait for slow loaders

        # Wait for network idle if needed
        try:
            await page.wait_for_load_state("networkidle", timeout=5000)
        except PlaywrightTimeoutError:
            pass

        curr_count = await get_symbol_count()
        if curr_count <= prev_count:
            stable_cycles += 1
        else:
            stable_cycles = 0
            prev_count = curr_count
        print(f"Current ticker count: {curr_count}")

    return prev_count


async def scrape_tradingview_symbols():
    url = "https://www.tradingview.com/screener/"
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-gpu",
            ],
        )
        context = await browser.new_context(
            viewport={"width": 1400, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        await page.goto(url, timeout=90000)

        # Dismiss cookie banner if present
        for sel in [
            'button:has-text("Accept")',
            'button:has-text("I Accept")',
            'button[aria-label="Accept"]',
        ]:
            try:
                btn = page.locator(sel)
                if await btn.count():
                    await btn.first.click(timeout=2000)
                    break
            except Exception:
                pass

        total_before = await page.evaluate('document.querySelectorAll(\'a[href*="/symbols/"]\').length')
        loaded = await load_all_rows(page)

        print(f"Symbols links before scrolling: {total_before}, after scrolling: {loaded}")

        # Grab only the last part of href (e.g., NASDAQ-AAPL)
        # items = await page.evaluate("""
        #     Array.from(document.querySelectorAll('a[href*="/symbols/"]'))
        #          .map(a => {
        #              const href = a.getAttribute('href') || '';
        #              const last = href.split('/').filter(Boolean).pop() || '';
        #              return last;
        #          })
        #          .filter(Boolean)
        # """)

        items = await page.evaluate("""
            Array.from(document.querySelectorAll('td a[href*="/symbols/"]')).map(a => {
                const href = a.getAttribute('href') || '';
                const last = href.split('/').filter(Boolean).pop() || '';  // e.g. "NASDAQ-NVDA"
                let exchange = '';
                let symbol = '';

                if (last.includes('-')) {
                    [exchange, symbol] = last.split('-');
                }

                const company = a.nextElementSibling ? a.nextElementSibling.textContent.trim() : '';

                return { exchange, symbol, company };
            }).filter(item => item.exchange && item.symbol);
        """)
        await browser.close()
        return items

def save_to_csv(symbols_last_parts, filename=OUTPUT_CSV):
    seen = set()
    with open(filename, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Exchange", "Symbol" ,"Company Name"])  # header
        # for item in symbols_last_parts:
        #     # Some rows can be like "NYSE-ABC" or contain extra dashes; split once
        #     if "-" in item:
        #         left, right = item.split("-", 1)
        #         key = (left, right)
        #         if key not in seen:
        #             writer.writerow([left, right])
        #             seen.add(key)

        for item in symbols_last_parts:
            key = (item["exchange"], item["symbol"])
            if key not in seen:
                writer.writerow([item["exchange"], item["symbol"], item["company"]])
                seen.add(key)

async def main():
    symbols = await scrape_tradingview_symbols()
    print(f"✅ Extracted {len(symbols)} raw items")
    save_to_csv(symbols)
    print(f"📂 Saved to {OUTPUT_CSV}")

if __name__ == "__main__":
    asyncio.run(main())
