# Stock Insight Engine - Enhanced Version with Calendar-Based & Multi-Source AI Recommendations
import numpy as np
import yfinance as yf
import pandas as pd
import requests
from datetime import datetime, timedelta, time
import feedparser  # for Google News RSS
from bs4 import BeautifulSoup
import re
import time
import sys
import csv
import io
import json
from io import StringIO
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import os
from lxml import etree
from difflib import get_close_matches
from tabula import convert_into
import pdfplumber
import base64
from PyPDF2 import PdfReader
import pyautogui
import pygetwindow as gw
import pyperclip
import webbrowser
import platform
import csv
import time
from tabula.io import read_pdf
import math
import warnings
import pdfplumber
import re
import pandas as pd
from pyspark.sql import SparkSession
import datetime

# Add more news sources or services as needed
EXTRA_NEWS_KEYWORDS = [
    "inflation", "interest rate", "recession", "conflict", "fed", "gdp",
    "policy", "regulation", "war"
]


'''def fetch_stock_data(symbol, period="6mo", interval="1d"):
    ticker = yf.Ticker(symbol)
    hist = ticker.history(period=period, interval=interval)
    hist["SMA_20"] = hist["Close"].rolling(window=20).mean()
    hist["SMA_50"] = hist["Close"].rolling(window=50).mean()
    hist["Volume_Avg"] = hist["Volume"].rolling(window=10).mean()
    hist["RSI"] = compute_rsi(hist["Close"], 14)
    return hist, ticker.info, ticker.recommendations, ticker.info.get(
        "targetMeanPrice", None), ticker'''

def fetch_stock_data(symbol, period="6mo", interval="1d"):
    import yfinance as yf
    import requests
    import re
    from bs4 import BeautifulSoup
    from time import sleep

    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period=period, interval=interval)

        if hist is None or hist.empty or "Close" not in hist.columns:
            raise ValueError("Yahoo returned empty or invalid historical data.")

        hist["SMA_20"] = hist["Close"].rolling(window=20).mean()
        hist["SMA_50"] = hist["Close"].rolling(window=50).mean()
        hist["Volume_Avg"] = hist["Volume"].rolling(window=10).mean()
        hist["RSI"] = compute_rsi(hist["Close"], 14)

        info = ticker.info or {}
        recs = ticker.recommendations if ticker.recommendations is not None else pd.DataFrame()
        target = info.get("targetMeanPrice", None)

        return hist, info, recs, target, ticker

    except Exception as e:
        print(f"⚠️ Yahoo Finance failed for {symbol}: {e}")
        sleep(1)

        try:
            print(f"🔄 Switching to MarketBeat for {symbol}")
            # Simulate fallback values from MarketBeat
            base_url = f"https://www.marketbeat.com/stocks/NASDAQ/{symbol.upper()}/"
            response = requests.get(base_url, headers={"User-Agent": "Mozilla/5.0"})
            soup = BeautifulSoup(response.text, 'html.parser')

            info = {
                "shortName": symbol,
                "sector": "Unknown",
                "industry": "Unknown",
                "marketCap": 0,
                "revenueGrowth": 0.0,
                "trailingPE": 0.0,
                "returnOnEquity": 0.0,
                "pegRatio": 0.0,
                "debtToEquity": 0.0,
                "netMargins": 0.0,
                "beta": 0.0,
                "dividendYield": 0.0
            }

            import pandas as pd
            import numpy as np
            dates = pd.date_range(end=pd.Timestamp.today(), periods=60)
            close_prices = np.linspace(100, 110, 60)
            volume = np.random.randint(100000, 500000, 60)

            hist = pd.DataFrame({
                "Close": close_prices,
                "Volume": volume
            }, index=dates)

            hist["SMA_20"] = hist["Close"].rolling(window=20).mean()
            hist["SMA_50"] = hist["Close"].rolling(window=50).mean()
            hist["Volume_Avg"] = hist["Volume"].rolling(window=10).mean()
            hist["RSI"] = compute_rsi(hist["Close"], 14)

            recommendations = pd.DataFrame()
            target = 0.0
            ticker = symbol  # Pass string symbol as fallback

            return hist, info, recommendations, target, ticker

        except Exception as mb_err:
            print(f"❌ MarketBeat also failed for {symbol}: {mb_err}")
            return None, None, None, None, symbol



def compute_rsi(series, period=14):
    delta = series.diff(1)
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


'''def fetch_news_sentiment(symbol):
    headlines = []
    sentiment_score = 0
    geopolitical_impact = 0
    rss_feed = feedparser.parse(f"https://news.google.com/rss/search?q={symbol}+stock&hl=en-US&gl=US&ceid=US:en")
    for entry in rss_feed.entries[:10]:
        content = (entry.title + " " + entry.summary).lower()
        headlines.append(entry.title)
        if any(word in content for word in ["strong", "growth", "rise", "record", "beat", "upgrade"]):
            sentiment_score += 1
        if any(word in content for word in ["fall", "drop", "miss", "concern", "downgrade"]):
            sentiment_score -= 1
        if any(word in content for word in EXTRA_NEWS_KEYWORDS):
            geopolitical_impact += 1
    return sentiment_score, geopolitical_impact, headlines'''

import feedparser
import time
from datetime import datetime, timedelta


def fetch_news_sentiment(symbol, days_window=10, max_entries=100):
    import feedparser
    import time
    from datetime import datetime, timedelta

    headlines = []
    sentiment_score = 0
    geopolitical_impact = 0

    now = datetime.utcnow()
    cutoff_date = now - timedelta(days=days_window)

    rss_feed = feedparser.parse(
        f"https://news.google.com/rss/search?q={symbol}+stock&hl=en-US&gl=US&ceid=US:en"
    )
    time.sleep(0.5)  # polite crawling

    count = 0
    for entry in rss_feed.entries:
        if hasattr(entry, 'published_parsed'):
            published = datetime(*entry.published_parsed[:6])
        else:
            continue

        if published >= cutoff_date:
            content = (entry.title + " " + entry.summary).lower()

            # 👇 Collect both headline text + date
            headlines.append({
                "title": entry.title,
                "published":
                    published.strftime("%Y-%m-%d")  # format date nicely
            })

            if any(word in content for word in [
                "strong", "growth", "rise", "record", "beat", "upgrade",
                "surge", "rally", "momentum", "all-time high",
                "buy rating", "beats estimates"
            ]):
                sentiment_score += 2
            if any(word in content for word in [
                "fall", "drop", "miss", "concern", "downgrade", "slump",
                "crash", "bearish", "selloff", "weak forecast"
            ]):
                sentiment_score -= 2
            if any(word in content for word in [
                "war", "sanction", "regulation", "tariff", "inflation",
                "recession", "bankruptcy", "default"
            ]):
                geopolitical_impact += 1

            count += 1
            if count >= max_entries:
                break

    normalized_score = max(0, min(100, sentiment_score + 50))

    return normalized_score, geopolitical_impact, headlines


def fetch_next_earnings(ticker):
    try:
        # Try yfinance first
        cal = ticker.calendar
        if isinstance(cal, pd.DataFrame) and not cal.empty:
            if 'Earnings Date' in cal.index:
                val = cal.loc['Earnings Date'][0]
                return pd.to_datetime(val).strftime("%Y-%m-%d")
            elif 'Earnings Date' in cal.columns:
                val = cal['Earnings Date'].iloc[0]
                return pd.to_datetime(val).strftime("%Y-%m-%d")
    except Exception as e:
        print(f"YFinance earnings fetch error: {e}")

    # Fallback to MarketBeat
    try:
        import re
        from bs4 import BeautifulSoup

        symbol = ticker.ticker.upper()
        base_url = f"https://www.marketbeat.com/stocks/NASDAQ/{symbol}/"
        response = requests.get(base_url, headers={"User-Agent": "Mozilla/5.0"})
        # Commented out debug logs to reduce console noise
        # print("MARKETBEAT RESPONSE STATUS:", response.status_code)
        # with open("marketbeat_response.html", "w", encoding="utf-8") as f:
        #     f.write(response.text)
        # print("Saved MarketBeat page to 'marketbeat_response.html'")
        soup = BeautifulSoup(response.text, 'html.parser')
        dd_tag = soup.find("dt", string=re.compile("Next Earnings.*"))
        if dd_tag:
            dd = dd_tag.find_next_sibling("dd")
            if dd:
                return dd.text.strip()
    except Exception as e:
        print(f"MarketBeat earnings fetch error: {e}")

    return None

def fetch_dividend_yield_marketbeat(ticker):
    import re
    from bs4 import BeautifulSoup
    import requests

    try:
        symbol = ticker.ticker.upper()
        base_url = f"https://www.marketbeat.com/stocks/NASDAQ/{symbol}/"
        response = requests.get(base_url, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(response.text, 'html.parser')

        div_tag = soup.find("dt", string=re.compile("Dividend Yield"))
        if div_tag:
            dd = div_tag.find_next_sibling("dd")
            if dd:
                text = dd.get_text(strip=True)
                # Remove % and convert to float
                if "%" in text:
                    value = float(text.replace('%', '').strip())
                    return value
    except Exception as e:
        print(f"MarketBeat dividend yield fetch error: {e}")

    return None

from webdriver_manager.chrome import ChromeDriverManager

# Ticker utilities
TICKER_CORRECTIONS = {
    "BRKB": "BRK-B",
    "BRKA": "BRK-A",
    "BFB": "BF-B",
    "BFA": "BF-A",
    # Add more if needed
}

YAHOO_MARKET_SUFFIXES = [
    "", ".NS", ".T", ".KQ", ".MI", ".AX", ".L", ".SS", ".HK"
]

ALTERNATE_TICKER_MAPPING = {
    "ORSTED": "ORSTED.CO",
    "SUZLON": "SUZLON.NS",
    "VWS": "VWS.CO",
    "SSE": "SSE.L",
    "EDP": "EDP.LS",
    "EQTL3": "EQTL3.SA",
    # Add more known exceptions as needed
}

ETF_PRODUCT_IDS = {}  # Placeholder for compatibility


# Load ticker universe from Yahoo Finance (fallback to hardcoded if fails)
def load_ticker_universe():
    try:
        url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?count=1000&scrIds=all_usa_stocks"
        df = pd.read_json(url)
        tickers = df["finance"]["result"][0]["quotes"]
        return [t["symbol"] for t in tickers if "symbol" in t]
    except:
        return ["AAPL", "MSFT", "GOOG", "AMZN", "BRK-B", "JNJ", "V", "PG"]


search_base = load_ticker_universe()


# Automatically open browser, print and save as PDF to a fixed path
def auto_save_holdings_pdf(symbol):
    try:
        url = f"https://www.financecharts.com/etfs/{symbol}/holdings"
        webbrowser.open(url)
        time.sleep(10)  # Let the site load

        window = None
        for w in gw.getWindowsWithTitle(symbol.upper()):
            if w.isActive:
                window = w
                break

        if window:
            window.activate()
            time.sleep(1)

        # Trigger Print
        if platform.system() == "Windows":
            pyautogui.hotkey("ctrl", "p")
        else:
            pyautogui.hotkey("command", "p")

        time.sleep(2)
        pyautogui.press("enter")  # Continue in print dialog
        time.sleep(3)  # Let the Save dialog open

        # Prepare file path and wipe out existing PDFs
        download_path = "C:/Users/pramo/IdeaProjects/stock_analysis/downloads"
        for file in os.listdir(download_path):
            if file.endswith(".pdf"):
                os.remove(os.path.join(download_path, file))
                print(f"🗑️ Removed: {file}")

        # Navigate to the folder
        pyautogui.hotkey("alt", "d")
        time.sleep(0.5)
        pyperclip.copy(download_path)
        pyautogui.hotkey("ctrl", "v")
        pyautogui.press("enter")
        time.sleep(2)

        # Just press Enter to accept the default file name and save
        pyautogui.press("esc")
        time.sleep(0.5)
        pyautogui.press("tab")
        time.sleep(0.5)
        pyautogui.press("tab")
        time.sleep(0.5)
        pyautogui.press("tab")
        time.sleep(0.5)
        pyautogui.press("tab")
        time.sleep(0.5)
        pyautogui.press("tab")
        time.sleep(0.5)
        pyautogui.press("tab")
        time.sleep(0.5)
        pyautogui.press("tab")
        time.sleep(0.5)
        pyautogui.press("tab")
        time.sleep(0.5)
        pyautogui.press("enter")

        print(f"📄 Saved PDF for: {symbol} in project folder.")
        time.sleep(5)

    except Exception as e:
        print(f"❌ Auto save PDF error: {e}")


# Scrape ETF holdings from locally saved PDF
def extract_etf_holdings(ticker):
    spark = SparkSession.builder.appName("ETF Holdings Parser").getOrCreate()
    downloads_folder = "C:/Users/pramo/IdeaProjects/stock_analysis/downloads"
    auto_save_holdings_pdf(ticker)
    # Find the only PDF file in downloads
    pdf_files = [f for f in os.listdir(downloads_folder) if f.endswith(".pdf")]
    if not pdf_files:
        print("❌ No PDF file found in Downloads folder.")
        return None
    if len(pdf_files) > 1:
        print("⚠️ More than one PDF found. Please ensure only one PDF exists.")
        return None

    pdf_path = os.path.join(downloads_folder, pdf_files[0])

    serial_pattern = re.compile(r"^\d{1,3}\.\s")
    records = []
    current_record = ""
    in_record = False

    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            lines = page.extract_text().splitlines()
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                if "Disclaimers:" in line:
                    break
                if serial_pattern.match(line):
                    if current_record:
                        records.append(current_record.strip())
                    current_record = line
                    in_record = True
                elif in_record:
                    current_record += " " + line

    if current_record:
        records.append(current_record.strip())

    parsed = []
    for record in records:
        try:
            m = re.match(r"^(\d{1,3})\.\s+(.*)", record)
            if not m:
                parsed.append((None, None, None, None, None))
                continue
            serial = int(m.group(1))
            rest = m.group(2)
            weight_match = re.search(r"(\d+\.\d+%)", rest)
            weight = weight_match.group(1) if weight_match else None
            if weight:
                rest = rest.replace(weight, "").strip()
            parsed.append((serial, rest, None, None, weight))
        except:
            parsed.append((None, None, None, None, None))

    df = pd.DataFrame(parsed,
                      columns=["Serial", "Raw", "Ticker", "Sector", "Weight"])
    df.dropna(subset=["Serial", "Raw"], inplace=True)

    df["Ticker"] = df["Raw"].apply(lambda raw: next((p for p in raw.split(
    )[::-1] if re.match(r"^[A-Z]{1,6}\.?[A-Z0-9]*$", p)), None))

    name_list = []
    sector_list = []
    for _, row in df.iterrows():
        raw, ticker = row["Raw"], row["Ticker"]
        if raw and ticker in raw.split():
            parts = raw.split()
            idx = parts.index(ticker)
            name = " ".join(parts[:idx])
            sector = " ".join(parts[idx + 1:])
        else:
            name = raw
            sector = ""
        name_list.append(name.strip())
        sector_list.append(sector.strip())
    df["Name"] = name_list
    df["Sector"] = sector_list

    junk_tickers = {
        "ASSETS", "ETF", "REIT", "USD", "Inc", "LLC", "Co", "Company"
    }

    def safe_lookup_ticker(name):
        try:
            if not isinstance(name, str) or len(name.strip()) < 3:
                return None
            clean_name = name.split(",")[0].split("PDF")[0].split(
                "INC")[0].strip()
            result = yf.Ticker(clean_name)
            return result.info.get("symbol")
        except:
            return None

    df["Ticker"] = df.apply(
        lambda row: safe_lookup_ticker(row["Name"])
        if row["Ticker"] in junk_tickers else row["Ticker"],
        axis=1)

    def clean(text):
        if not isinstance(text, str):
            return text
        text = re.sub(r'https:\/\/www\\.financecharts\\.com[^\s]+', '', text)
        text = re.sub(r'\d{1,2}/\d{1,2}/\d{2}', '', text)
        text = re.sub(r'#\s*NAME\s*TICKER.*?ASSETS',
                      '',
                      text,
                      flags=re.IGNORECASE)
        text = re.sub(r'iShares\s+Russell.*?Holdings',
                      '',
                      text,
                      flags=re.IGNORECASE)
        text = re.sub(r'ETF', '', text)
        text = re.sub(r'\s{2,}', ' ', text)
        text = re.sub(r'[\u2000-\u206F\u2E00-\u2E7F]', '', text)
        return text.strip()

        parts = text.split()
        if re.match(r"^[A-Z]{1,6}(\.[A-Z])?$",
                    text) and p.upper() not in {"PDF", "NONE"}:
            return text.replace(".", "-")  # BRK.B -> BRK-B

    df["Name"] = df["Name"].apply(clean)
    df["Sector"] = df["Sector"].apply(clean)

    for col in ['Serial', 'Name', 'Ticker', 'Sector', 'Weight']:
        if col not in df.columns:
            df[col] = ""

    df_final = df[['Serial', 'Name', 'Ticker', 'Sector', 'Weight']]

    from pyspark.sql.functions import col

    if df.shape[0] > 0:
        df_spark = spark.createDataFrame(df)
        return df_spark.select('Serial', 'Ticker').filter(
            col('Ticker').isNotNull() & col('Serial').isNotNull())
        spark.stop()
    else:
        print("❌ No valid rows found.")
        return None


# Auto-correct and fallback logic for tickers
'''def try_alternate_yahoo_tickers(fetch_func, symbol, name_hint=None):
    from yahooquery import Ticker, search

    if symbol in ALTERNATE_TICKER_MAPPING:
        override_symbol = ALTERNATE_TICKER_MAPPING[symbol]
        try:
            df, info, *_ = fetch_func(override_symbol)
            if df is not None and not df.empty:
                return override_symbol, df, info
        except:
            pass

    for suffix in YAHOO_MARKET_SUFFIXES:
        test_symbol = symbol + suffix
        try:
            df, info, *_ = fetch_func(test_symbol)
            if df is not None and not df.empty:
                return test_symbol, df, info
        except:
            continue

    if name_hint:
        try:
            result = search(name_hint)
            for item in result.get('quotes', []):
                possible_symbol = item.get("symbol")
                if possible_symbol:
                    try:
                        df, info, *_ = fetch_func(possible_symbol)
                        if df is not None and not df.empty:
                            return possible_symbol, df, info
                    except:
                        continue
        except:
            pass

    try:
        tickers = Ticker([symbol])
        if symbol in tickers.price:
            return symbol, None, tickers.price[symbol]
    except:
        pass

    if name_hint:
        try:
            match = get_close_matches(name_hint.upper(), search_base, n=1, cutoff=0.6)
            if match:
                df, info, *_ = fetch_func(match[0])
                if df is not None and not df.empty:
                    return match[0], df, info
        except:
            pass

    return None, None, None'''
''' def download_etf_excel(symbol):
    try:
        etf_info = ETF_PRODUCT_IDS.get(symbol.upper())
        if not etf_info:
            raise ValueError("ETF Product ID not mapped.")

        product_id, slug = etf_info
        download_dir = os.path.abspath("downloads")
        os.makedirs(download_dir, exist_ok=True)

        chrome_options = Options()
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--headless=new")
        chrome_options.add_experimental_option("prefs", {
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "download.default_directory": download_dir,
            "safebrowsing.enabled": True
        })

        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
        url = f"https://www.ishares.com/us/products/{product_id}/{slug}"
        driver.get(url)

        try:
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.ID, "onetrust-accept-btn-handler")))
            consent_button = driver.find_element(By.ID, "onetrust-accept-btn-handler")
            consent_button.click()
            time.sleep(1)
        except:
            pass

        try:
            download_btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Data Download') and contains(@href, 'fileType=xls')]"))
            )
            driver.execute_script("arguments[0].scrollIntoView();", download_btn)
            time.sleep(1)
            download_btn.click()
            print("\U0001F4E5 Clicked 'Data Download' link")
        except Exception as e:
            print("❌ Failed to find or click 'Data Download' link:", e)
            driver.quit()
            return None

        print("⏳ Waiting for file download to complete...")
        wait_time = 30
        downloaded_file = None

        for _ in range(wait_time):
            xls_files = [f for f in os.listdir(download_dir) if f.endswith(".xls") or f.endswith(".xlsx")]
            xls_files.sort(key=lambda x: os.path.getmtime(os.path.join(download_dir, x)), reverse=True)

            if xls_files:
                candidate = os.path.join(download_dir, xls_files[0])
                size = os.path.getsize(candidate)
                print(f"\U0001FAA0 Candidate file: {candidate} ({size} bytes)")
                if size > 2048:
                    downloaded_file = candidate
                    break
            time.sleep(1)

        driver.quit()

        if downloaded_file:
            print(f"📄 Found downloaded Excel: {downloaded_file}")
            return downloaded_file

        print("❌ Downloaded file not found or is too small after wait.")
        return None

    except Exception as e:
        print(f"ETF download error: {e}")
        return None'''
'''def read_etf_excel(file_path):
    try:
        ext = os.path.splitext(file_path)[-1].lower()
        print(f"📦 Attempting to read Excel with extension: {ext}")

        if ext == ".xls":
            try:
                df = pd.read_excel(file_path, engine="xlrd")
                print("📊 Successfully read ETF holdings with xlrd")
                print(df.head())
                return df
            except Exception as e:
                print(f"⚠️ xlrd failed: {e} — trying XML fallback")

        elif ext == ".xlsx":
            try:
                df = pd.read_excel(file_path, engine="openpyxl")
                print("📊 Successfully read ETF holdings with openpyxl")
                print(df.head())
                return df
            except Exception as e:
                print(f"⚠️ openpyxl failed: {e}")

        with open(file_path, 'rb') as f:
            raw_data = f.read()

        if raw_data.startswith(b'\xef\xbb\xbf'):
            raw_data = raw_data[3:]

        xml_str = raw_data.decode("utf-8")
        xml_str = re.sub(r"&(?!([a-zA-Z]+|#\d+);)", "&amp;", xml_str)

        tree = etree.fromstring(xml_str.encode("utf-8"))

        ns = {"ss": "urn:schemas-microsoft-com:office:spreadsheet"}
        rows = tree.xpath("//ss:Worksheet/ss:Table/ss:Row", namespaces=ns)
        data = []

        for row in rows:
            cells = row.xpath("ss:Cell/ss:Data", namespaces=ns)
            cell_values = [c.text for c in cells]
            data.append(cell_values)

        if not data or len(data) < 2:
            print("❌ No valid data found in XML")
            return None

        max_len = max(len(r) for r in data)
        header_row = next((r for r in data if len(r) == max_len), None)
        data_rows = [r for r in data if len(r) == max_len and r != header_row]

        df = pd.DataFrame(data_rows, columns=header_row)
        print("📊 Successfully parsed XML-format Excel")
        print(df.head())
        return df

    except Exception as e:
        print(f"❌ Failed to parse Excel: {e}")
        return None'''
'''def fetch_etf_holdings(symbol):
    file_path = download_etf_excel(symbol)
    if not file_path:
        return []

    df = read_etf_excel(file_path)
    if df is None:
        return []

    df.columns = df.columns.str.strip()

    potential_weight_columns = ["Weight (%)", "Holding %", "Weight"]
    weight_column = next((col for col in df.columns if col in potential_weight_columns), None)

    if weight_column is None:
        print("⚠️ Could not find a valid weight column in the ETF holdings")
        return []

    holdings = []
    for _, row in df.iterrows():
        raw_symbol = row.get("Ticker", "N/A")
        corrected_symbol = TICKER_CORRECTIONS.get(str(raw_symbol).upper(), str(raw_symbol).upper())
        try:
            weight_val = float(row.get(weight_column, 0))
        except:
            weight_val = 0.0
        holdings.append({
            "symbol": corrected_symbol,
            "name": row.get("Name", ""),
            "sector": row.get("Sector", ""),
            "weight": weight_val
        })

    return holdings'''

import pandas as pd
import os

import pandas as pd

def read_symbols_from_csv(csv_path):
    try:
        df = pd.read_csv(csv_path, encoding='latin1')

        print(f"✅ Loaded {len(df)} rows from CSV.")

        # 👀 Look for correct column
        if 'Symbol' in df.columns:
            input_symbol = df['Symbol'].dropna().unique().tolist()
        elif 'Ticker' in df.columns:
            input_symbol = df['Ticker'].dropna().unique().tolist()
        else:
            print("❗ No 'Symbol' or 'Ticker' column found in CSV!")
            return []

        return input_symbol

    except Exception as e:
        print(f"Error reading CSV: {e}")
        return []

def fix_yahoo_symbol(symbol):
    """Fix symbols like BRK.B -> BRK-B for Yahoo Finance compatibility."""
    return symbol.replace('.', '-')

def scan_stocks_and_find_strong_buys(symbols_list, top_n):
    import time
    import pandas as pd

    strong_buys = []

    # 🧹 Clean symbols: remove GLOBAL, ALL, NONE
    symbols = [s.strip().upper() for s in symbols_list if s.strip().upper() not in ['GLOBAL', 'ALL', 'NONE', '']]
    symbols = symbols[:top_n]  # Limit to 100 after cleaning

    print(f"🚀 Starting scan for {len(symbols)} symbols...\n")

    for idx, ticker_symbol in enumerate(symbols):
        try:
            fixed_symbol = fix_yahoo_symbol(ticker_symbol)
            df, info, recommendations, price_target, ticker = fetch_stock_data(fixed_symbol)

            if df is None or info is None:
                print(f"❗ No data for {ticker_symbol}. Skipping...")
                continue

            sentiment_score, geopolitical_impact, headlines = fetch_news_sentiment(fixed_symbol)
            earnings_date = fetch_next_earnings(ticker)

            # 🛠️ Smartly pass ticker_symbol into analyze_trend
            decision = analyze_trend(
                df, info, sentiment_score, geopolitical_impact,
                recommendations, price_target, earnings_date, ticker_symbol
            )

            # 🖨️ Print progress for every stock
            print(f"{idx+1}. {ticker_symbol} ➔ Forecast: {decision.forecast} ➔ Score: {sum(decision.score_breakdown.values())}")

            # 🏆 Collect strong buys
            if decision.forecast in ['BUY', 'STRONG BUY']:
                strong_buys.append({
                    'Symbol': ticker_symbol,
                    'Company': info.get("shortName", "N/A"),
                    'Forecast': decision.forecast,
                    'Confidence': decision.confidence,
                    'Entry Suggestion': decision.entry_suggestion,
                    'Score': sum(decision.score_breakdown.values()),
                    'Summary': decision.narrative
                })

        except Exception as e:
            print(f"⚠️ Error processing {ticker_symbol}: {e}")
            time.sleep(1)

    # 📊 After all stocks processed
    if strong_buys:
        print("\n🏆 Top BUY Signals Today:")
        for i, res in enumerate(strong_buys[:5], 1):
            print(f"{i}. 🔹 {res['Symbol']}")
            print(f"   Forecast: {res['Summary']}")
            print(f"   Suggested Entry: {res['Entry Suggestion']}")
    else:
        print("\n❗ No strong BUY signals found today.")

    time.sleep(0.2)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("StockScreener").getOrCreate()

# ✅ Create final DataFrame
    df_result = pd.DataFrame(strong_buys)

    print(f"\n✅ Scanned {len(symbols)} stocks.")
    print(f"✅ Found {len(strong_buys)} BUY signals.\n")

    # Convert to Spark DataFrame
    spark_df = spark.createDataFrame(df_result)

        # Show like a table
    spark_df.show(20000,truncate=False)   # use truncate=False for full columns

    return spark_df



    '''except Exception as e:
            print(f"❗ Error analyzing {symbol}: {e}")

    # Final collection
    print("\n🔎 Strong Buys collected:")
    for item in strong_buys:
        print(item)

    df_result = pd.DataFrame(strong_buys)

    if df_result.empty:
        print("⚠️ No Strong Buys found.")
    else:
        print("\n🏆 Top Strong Buy Stocks:")
        print(df_result[["Symbol", "Company", "Forecast", "Confidence", "Entry Suggestion"]])

    return df_result.head(top_n)'''

'''def analyze_etf(etf_symbol):
    import yfinance as yf
    import pandas as pd
    import random
    #from your_module import fetch_stock_data  # Replace with actual import if needed

    reasons = []
    ticker_obj = yf.Ticker(etf_symbol)
    info = ticker_obj.info
    summary = f"{info.get('shortName', 'This ETF')} provides exposure to multiple sectors."

    holdings = extract_etf_holdings(etf_symbol)

    if holdings is None or holdings.count() == 0:
        return {
            "summary": "Unable to fetch ETF holdings from iShares.",
            "reasons": ["ETF holding data could not be retrieved."]
        }

    fundamentals = []
    tickers = [
        row.Ticker for row in holdings.collect()
        if row.Ticker and row.Ticker != "N/A"
    ]

    for ticker in tickers:
        print(f"⏳ Fetching data for: {ticker}")
        #time.sleep(5)
        try:
            _, h_info, *_ = fetch_stock_data(ticker)
            sector_policy_impact = {
                "Financial Services": "Negative",
                "Technology": "Neutral",
                "Consumer Defensive": "Positive",
                "Healthcare": "Neutral",
                "Energy": "Mixed",
                "Utilities": "Negative",
                "Industrials": "Neutral",
                "Real Estate": "Negative",
                "Consumer Cyclical": "Mixed"
            }

            policy_effect = sector_policy_impact.get(h_info.get("sector"),
                                                     "Neutral")

            fundamentals.append({
                "ticker": ticker,
                "serial": None,
                "revenueGrowth": h_info.get("revenueGrowth"),
                "debtToEquity": h_info.get("debtToEquity"),
                "roe": h_info.get("returnOnEquity"),
                "peRatio": h_info.get("trailingPE"),
                "marketCap": h_info.get("marketCap"),
                "sector": h_info.get("sector"),
                "policyImpact": policy_effect,
                "netMargin": h_info.get("netMargins"),
                "pegRatio": h_info.get("pegRatio"),
                "beta": h_info.get("beta"),
                "dividendYield": h_info.get("dividendYield")
            })
        except Exception as e:
            print(f"❌ Failed to fetch {ticker}: {e}")
            continue

    if not fundamentals:
        return {
            "summary":
                "Fundamental data could not be gathered for any holding.",
            "reasons": ["⚠️ No valid fundamentals retrieved from holdings."]
        }

    growth_count = sum(1 for f in fundamentals
                       if f["revenueGrowth"] and f["revenueGrowth"] > 0.05)
    avg_debt = sum(f["debtToEquity"] or 0 for f in fundamentals
                   if f["debtToEquity"] is not None) / len(fundamentals)
    avg_roe = sum(f["roe"] or 0 for f in fundamentals
                  if f["roe"] is not None) / len(fundamentals)

    if growth_count / len(fundamentals) >= 0.5:
        rating = "Strong Buy"
        reasons.append("📈 Majority of holdings show positive revenue growth.")
    elif growth_count / len(fundamentals) >= 0.3:
        rating = "Buy"
        reasons.append(
            "📊 Some holdings demonstrate growth potential with sector-tailored resilience."
        )
    else:
        rating = "Sell"
        reasons.append(
            "📉 Most holdings lack growth signals and are vulnerable to macroeconomic headwinds."
        )

    summary += f" Rating: {rating}. Avg Debt/Equity: {avg_debt:.2f}, Avg ROE: {avg_roe:.2f}. Policy Impact Weighted."

    current_date = datetime.datetime.now().strftime("%B %Y")
    forecast_msg = ""
    if rating == "Strong Buy":
        forecast_msg = f"With macroeconomic resilience expected in {current_date}, the ETF is forecasted to outperform in the next 6 months."
    elif rating == "Buy":
        forecast_msg = f"Stable sectors suggest modest ETF growth potential over the next 6 months despite market volatility."
    else:
        forecast_msg = f"Due to uncertain economic outlook (inflation/recession), the ETF may underperform in the next 6 months."

    reasons.append(f"🔮 Forecast: {forecast_msg}")
    reasons.append(
        "📌 Policy-Based Analysis Included: Impact of sectors under current government and Fed actions."
    )

    # Add detailed economic impact analysis
    projected_growth_pct = (growth_count / len(fundamentals)) * 100
    reasons.append(
        f"📊 Projected Growth: Based on current fundamentals, approximately {projected_growth_pct:.2f}% of holdings show growth potential."
    )

    inflation_impact = "moderate" if avg_debt < 1.5 else "significant"
    recession_resilience = "strong" if avg_roe > 0.12 else "vulnerable"
    reasons.append(
        f"🌐 Inflation Impact: Holdings are expected to have {inflation_impact} inflation pressure based on debt-to-equity ratios."
    )
    reasons.append(
        f"📉 Recession Impact: Holdings appear {recession_resilience} based on historical profitability (ROE)."
    )

    reasons.append("📈 Additional Financial Metrics Summary:")

    avg_net_margin = sum(f["netMargin"] or 0 for f in fundamentals
                         if f.get("netMargin") is not None) / len(fundamentals)
    avg_peg = sum(f["pegRatio"] or 0 for f in fundamentals
                  if f.get("pegRatio") is not None) / len(fundamentals)
    avg_beta = sum(f["beta"] or 0 for f in fundamentals
                   if f.get("beta") is not None) / len(fundamentals)
    avg_div_yield = sum(
        f["dividendYield"] or 0 for f in fundamentals
        if f.get("dividendYield") is not None) / len(fundamentals)

    reasons.append(
        f"🧾 Net Margin Avg: {avg_net_margin:.2%} | PEG Ratio Avg: {avg_peg:.2f}"
    )
    reasons.append(
        f"📉 Volatility (Beta) Avg: {avg_beta:.2f} | Dividend Yield Avg: {avg_div_yield:.2%}"
    )

    crisis_performance = [
        "📅 2008 Financial Crisis: Many financial ETFs dropped >40%, tech & healthcare showed relative resilience.",
        "📅 2020 COVID Crash: Sharp dip (~30%) but fast recovery for growth sectors.",
        "📅 2022 Inflation Peak: Value ETFs outperformed growth, defensive sectors were safer bets."
    ]
    reasons.extend(crisis_performance)

    return {"summary": summary.strip(), "reasons": reasons'''


def analyze_etf(etf_symbol):
    import yfinance as yf
    import datetime
    import time
    import re
    from collections import Counter

    print(f"\n🔍 Starting analysis for ETF: {etf_symbol}")

    # 🧾 ETF Metadata
    etf_description = "Description not available."
    etf_fund_family = "N/A"
    etf_category = "N/A"
    etf_net_assets = 0
    etf_inception = "N/A"
    etf_current_price = "N/A"
    sector_weightings = []

    try:
        etf_obj = yf.Ticker(etf_symbol)
        etf_info = etf_obj.info
        etf_current_price = etf_info.get("regularMarketPrice", "N/A")
        etf_description = etf_info.get("longBusinessSummary", etf_description)
        etf_fund_family = etf_info.get("fundFamily", "N/A")
        etf_category = etf_info.get("category", etf_info.get("fundCategory", "N/A"))
        etf_net_assets = etf_info.get("totalAssets", etf_info.get("netAssets", 0))
        etf_inception = etf_info.get("inceptionDate", "N/A")
        sector_weightings = etf_info.get("sectorWeightings", [])
    except Exception as e:
        print(f"⚠️ Could not retrieve ETF-level info for {etf_symbol}: {e}")

    holdings = extract_etf_holdings(etf_symbol)
    if holdings is None or holdings.count() == 0:
        return {
            "summary": "Unable to fetch ETF holdings from source.",
            "reasons": ["ETF holding data could not be retrieved."]
        }

    tickers = [row.Ticker for row in holdings.collect() if row.Ticker and row.Ticker != "N/A"]

    forecasts = []
    failed = []

    for idx, ticker_symbol in enumerate(tickers):
        try:
            fixed_symbol = fix_yahoo_symbol(ticker_symbol)
            df, info, recommendations, price_target, ticker = fetch_stock_data(fixed_symbol)

            if df is None or info is None:
                print(f"❗ No data for {ticker_symbol}. Skipping...")
                continue

            sentiment_score, geopolitical_impact, headlines = fetch_news_sentiment(fixed_symbol)
            earnings_date = fetch_next_earnings(ticker)

            decision = analyze_trend(
                df, info, sentiment_score, geopolitical_impact,
                recommendations, price_target, earnings_date, ticker_symbol
            )

            forecasts.append(decision)
            print(f"{idx + 1}. {ticker_symbol} ➔ Forecast: {decision.forecast} ➔ Score: {sum(decision.score_breakdown.values())}")

        except Exception as e:
            print(f"⚠️ Error processing {ticker_symbol}: {e}")
            failed.append(ticker_symbol)
            time.sleep(1)

    if not forecasts:
        return {
            "summary": "No holdings could be analyzed.",
            "reasons": ["⚠️ All trend analyses failed or returned no data."]
        }

    # 🧠 Aggregate insights
    total = len(forecasts)
    forecast_counts = Counter([f.forecast for f in forecasts])
    strong_buy = forecast_counts.get("STRONG BUY", 0)
    buy = forecast_counts.get("BUY", 0)
    hold = forecast_counts.get("HOLD", 0)
    sell = forecast_counts.get("SELL", 0)
    strong_sell = forecast_counts.get("STRONG SELL", 0)

    buy_pct = (buy / total) * 100
    hold_pct = (hold / total) * 100
    sell_pct = (sell / total) * 100

    # 📈 Score/Valuation/Growth Aggregation
    high_growth_count = 0
    undervalued_count = 0
    total_score = 0

    for decision in forecasts:
        sb = decision.score_breakdown
        total_score += sum(sb.values())

        # Undervalued
        if "undervalued" in "".join(decision.reasons).lower():
            undervalued_count += 1

        # Growth Potential
        for reason in decision.reasons:
            match = re.search(r"Projected 3-month growth estimated at ([\d.]+)%", reason)
            if match:
                growth_pct = float(match.group(1))
                if growth_pct >= 20:
                    high_growth_count += 1
                break  # stop after finding first growth line

    avg_score = total_score / total if total else 0
    growth_potential_pct = (high_growth_count / total) * 100
    undervalued_pct = (undervalued_count / total) * 100

    # 🎯 Verdict Logic (Adjusted for real-world ETF behavior)
    if buy_pct >= 30 and sell_pct <= 15 and avg_score >= 75 and growth_potential_pct >= 15:
        etf_rating = "STRONG BUY"
    elif (buy_pct + hold_pct) >= 75 and sell_pct <= 20 and undervalued_pct >= 20:
        etf_rating = "BUY"
    elif sell_pct >= 50:
        etf_rating = "STRONG SELL"
    elif sell_pct >= 30:
        etf_rating = "SELL"
    else:
        etf_rating = "HOLD"

    # 📝 Summary
    summary_lines = [
        f"📊 ETF Summary for {etf_symbol}",
        f"Total Holdings Analyzed: {total}",
        f"✅ Strong Buy: {strong_buy}",
        f"👍 Buy: {buy}",
        f"⚖️ Hold: {hold}",
        f"👎 Sell: {sell}",
        f"❌ Strong Sell: {strong_sell}",
        f"🏁 Final Verdict: {etf_rating}",
        "",
        "📘 ETF Overview",
        f"Description: {etf_description[:500]}{'...' if len(etf_description) > 500 else ''}",
        f"Fund Family: {etf_fund_family}",
        f"Category: {etf_category}",
        f"Net Assets: ${etf_net_assets / 1e9:.2f}B" if etf_net_assets else "Net Assets: N/A",
        f"Inception Date: {etf_inception}",
        f"Current Price: ${etf_current_price}" if etf_current_price != "N/A" else "Current Price: N/A",
        "",
        "📈 Advanced Metrics",
        f"High Growth Potential Holdings: {high_growth_count} ({growth_potential_pct:.1f}%)",
        f"Undervalued Holdings: {undervalued_count} ({undervalued_pct:.1f}%)",
        f"Avg Score per Holding: {avg_score:.2f}"
    ]

    if failed:
        summary_lines.append(f"⚠️ Failed to analyze {len(failed)} tickers: {', '.join(failed)}")

    # 🎯 Show Buy-Eligible Holdings
    buy_stocks = []
    for decision in forecasts:
        if decision.forecast in ["BUY", "STRONG BUY"]:
            stock_score = sum(decision.score_breakdown.values())
            summary = decision.reasons[0] if decision.reasons else "N/A"
            buy_stocks.append(f"{decision.forecast}: {summary} ➔ Score: {stock_score}")

    summary_lines.append("")
    if buy_stocks:
        summary_lines.append("🎯 BUY-Eligible Holdings:")
        summary_lines.extend([f" - {line}" for line in buy_stocks])
    else:
        summary_lines.append("🎯 No BUY-eligible holdings found.")

    return {
        "summary": "\n".join(summary_lines),
        "forecast_breakdown": dict(forecast_counts),
        "verdict": etf_rating
    }



from dataclasses import dataclass, asdict
import pandas as pd
'''@dataclass
class StockForecast:
    forecast: str
    confidence: str
    entry_suggestion: str
    narrative: str
    reasons: list
    score_breakdown: dict'''

from dataclasses import dataclass
from typing import Optional
import pandas as pd

@dataclass
class StockForecast:
    forecast: str
    confidence: str
    entry_suggestion: str
    narrative: str
    reasons: list
    score_breakdown: dict
    buy_date_suggestion: Optional[str] = None
    sell_by_suggestion: Optional[str] = None

def analyze_trend(df, info, sentiment_score, geopolitical_impact, recommendations, price_target, earnings_date, debug=False):
    if len(df) < 2:
        return StockForecast(
            forecast="HOLD",
            confidence="Low",
            entry_suggestion="Not enough data",
            narrative="Insufficient historical data to analyze.",
            reasons=["Data Error: Less than 2 records in input data."],
            score_breakdown={},
            buy_date_suggestion=None,
            sell_by_suggestion=None
        )

    description = info.get("longBusinessSummary", "Description not available.")
    business_summary = description.split(".")[0:2]
    summary_snippet = ". ".join(business_summary).strip() + "."

    industry = info.get("industry", "N/A")
    sector = info.get("sector", "N/A")
    market_cap = info.get("marketCap", 0)
    company_name = info.get("shortName", "This company")

    reasons = [
        f"📌 {company_name} operates in the {industry} industry within the {sector} sector.",
        f"🗾 Summary: {summary_snippet}",
        f"💵 Current Price: ${df.iloc[-1].Close:.2f}"
    ]

    score = 0
    score_breakdown = {
        "Technicals": 0,
        "Volume": 0,
        "Momentum": 0,
        "RSI": 0,
        "Sentiment": sentiment_score,
        "Macro": -1 if geopolitical_impact > 0 else 0,
        "Valuation": 0,
        "Analyst": 0,
        "Target": 0,
        "Business Demand": 0
    }

    final_summary = f"{company_name} is a {industry.lower()} company in the {sector.lower()} sector. "
    projected_growth = "Unknown"

    try:
        revenue_growth = info.get("revenueGrowth")
        earnings_growth = info.get("earningsQuarterlyGrowth")
        if revenue_growth is not None and earnings_growth is not None:
            avg_growth = (revenue_growth + earnings_growth) / 2
            projected_growth = f"{avg_growth * 100:.2f}%"
            if avg_growth > 0.05:
                reasons.append(f"Fundamentals: Projected 3-month growth estimated at {projected_growth} based on revenue and earnings trends")
            elif avg_growth < -0.05:
                reasons.append(f"Fundamentals: Projected 3-month decline estimated at {projected_growth}")
            else:
                reasons.append(f"Fundamentals: Sideways projection estimated at {projected_growth}")
    except Exception:
        reasons.append("Growth Projection: Unable to compute from fundamentals")

    # ---------- Now continue with your technicals, momentum, sentiment checks ----------
    latest = df.iloc[-1]
    prev = df.iloc[-2]

    if latest.Close > latest.SMA_20 > latest.SMA_50:
        score += 2
        score_breakdown["Technicals"] += 2
        reasons.append("Technicals: Price is above both 20-day and 50-day moving averages")
    elif latest.Close < latest.SMA_20 < latest.SMA_50:
        score -= 2
        score_breakdown["Technicals"] -= 2
        reasons.append("Technicals: Price is below both 20-day and 50-day moving averages")

    if latest.Volume > latest.Volume_Avg:
        score += 1
        score_breakdown["Volume"] += 1
        reasons.append("Volume: Higher than 10-day average")
    else:
        reasons.append("Volume: Lower than average, low momentum")

    if latest.Close > prev.Close:
        score += 1
        score_breakdown["Momentum"] += 1
        reasons.append("Momentum: Today's close is higher than yesterday's")
    elif latest.Close < prev.Close:
        score -= 1
        score_breakdown["Momentum"] -= 1
        reasons.append("Momentum: Today's close is lower than yesterday's")

    if latest.RSI < 30:
        score += 1
        score_breakdown["RSI"] += 1
        reasons.append("RSI: Below 30 — potentially oversold")
    elif latest.RSI > 70:
        score -= 1
        score_breakdown["RSI"] -= 1
        reasons.append("RSI: Above 70 — potentially overbought")
    else:
        reasons.append("RSI: Neutral")

        # Adjust score based on sentiment strength (scaled, not direct)
    if sentiment_score >= 10:
        score += 2
        reasons.append(f"📰 News Sentiment: Very strong positive news (+2)")
    elif sentiment_score >= 2:
        score += 1
        reasons.append(f"📰 News Sentiment: Mildly positive news (+1)")
    elif sentiment_score <= -10:
        score -= 2
        reasons.append(f"📰 News Sentiment: Very strong negative news (-2)")
    elif sentiment_score <= -2:
        score -= 1
        reasons.append(f"📰 News Sentiment: Mildly negative news (-1)")
    else:
        reasons.append(f"📰 News Sentiment: Neutral (0)")

    if geopolitical_impact > 0:
        score -= 1
        reasons.append("Macro Influence: News includes geopolitical or regulatory topics")

    try:
        pe = info.get("trailingPE", None)
        roe = info.get("returnOnEquity", None)
        if pe is not None:
            if pe < 20:
                score += 1
                score_breakdown["Valuation"] += 1
                reasons.append(f"Valuation: Attractive PE ratio at {pe:.2f}")
            elif pe > 30:
                score -= 1
                score_breakdown["Valuation"] -= 1
                reasons.append(f"Valuation: Overvalued PE ratio at {pe:.2f}")
        if roe is not None:
            roe_percent = roe * 100
            if roe_percent > 15:
                score += 1
                reasons.append(
                    f"Profitability: Strong ROE at {roe_percent:.2f}% (+1 point)")
            elif roe_percent < 5:
                score -= 1
                reasons.append(
                    f"Profitability: Weak ROE at {roe_percent:.2f}% (-1 point)")
            else:
                reasons.append(
                    f"Profitability: Moderate ROE at {roe_percent:.2f}% (neutral)")
    except:
        reasons.append("Fundamentals: Not available")

    try:
        revenue_growth = info.get("revenueGrowth", None)
        if revenue_growth is not None:
            revenue_growth_percent = revenue_growth * 100
            if revenue_growth_percent > 10:
                score += 2
                score_breakdown["Business Demand"] += 2
                reasons.append(
                    f"Business Demand: Strong sales growth of {revenue_growth_percent:.2f}% YoY (+2 points)"
                )
            elif revenue_growth_percent > 5:
                score += 1
                score_breakdown["Business Demand"] += 1
                reasons.append(
                    f"Business Demand: Moderate sales growth of {revenue_growth_percent:.2f}% YoY (+1 point)"
                )
            elif revenue_growth_percent >= 0:
                reasons.append(
                    f"Business Demand: Flat but positive sales growth of {revenue_growth_percent:.2f}% YoY (neutral)"
                )
            else:
                score -= 2
                score_breakdown["Business Demand"] -= 2
                reasons.append(
                    f"Business Demand: Sales declined by {abs(revenue_growth_percent):.2f}% YoY (-2 points)"
                )
    except:
        reasons.append("Business Demand: Unable to assess revenue growth.")

    try:
        if not recommendations.empty and {'buy', 'sell', 'hold'}.issubset(recommendations.columns):
            recent = recommendations.head(2)
            if len(recent) >= 2:
                latest_buy = recent.iloc[0]['buy']
                prev_buy = recent.iloc[1]['buy']
                latest_sell = recent.iloc[0]['sell']
                prev_sell = recent.iloc[1]['sell']
                if latest_buy > prev_buy and latest_sell < prev_sell:
                    score += 1
                    score_breakdown["Analyst"] += 1
                    reasons.append("Analyst Sentiment: Buy ratings increased, sell ratings decreased")
                elif latest_buy < prev_buy and latest_sell > prev_sell:
                    score -= 1
                    score_breakdown["Analyst"] -= 1
                    reasons.append("Analyst Sentiment: Buy ratings dropped, sell ratings increased")
                else:
                    reasons.append("Analyst Sentiment: No strong trend in ratings")
    except:
        reasons.append("Analyst Sentiment: Unable to parse analyst recommendations")

    if price_target:
        current_price = latest.Close
        if current_price < price_target:
            score += 1
            score_breakdown["Target"] += 1
            reasons.append(f"Price Target: Current price is below analyst mean target of {price_target:.2f}")
        else:
            reasons.append(f"Price Target: Current price is above analyst mean target of {price_target:.2f}")

    try:
        summary_text = info.get("recommendationKey", "").lower().strip()
        if summary_text:
            if "strong buy" in summary_text:
                score += 2
                score_breakdown["Analyst"] += 2
                reasons.append("Analyst Rating: Strong Buy")
            elif "buy" in summary_text:
                score += 1
                score_breakdown["Analyst"] += 1
                reasons.append("Analyst Rating: Buy")
            elif "hold" in summary_text:
                reasons.append("Analyst Rating: Hold")
            elif "sell" in summary_text:
                score -= 1
                score_breakdown["Analyst"] -= 1
                reasons.append("Analyst Rating: Sell")
            elif "strong sell" in summary_text:
                score -= 2
                score_breakdown["Analyst"] -= 2
                reasons.append("Analyst Rating: Strong Sell")
    except:
        pass

    if earnings_date:
        try:
            earnings_dt = pd.to_datetime(earnings_date)
            days_to_earnings = (earnings_dt - pd.Timestamp.today()).days
            if 0 <= days_to_earnings <= 3:
                reasons.append(f"⚠️ Earnings Report in {days_to_earnings} days — expect volatility")
            elif 4 <= days_to_earnings <= 7:
                reasons.append(f"🕒 Earnings coming up in {days_to_earnings} days")
        except:
            pass

    if score >= 12:
        forecast = "STRONG BUY"
        confidence = "Very High"
        entry_suggestion = f"High conviction buy. Enter if price pulls back near SMA20 ({latest.SMA_20:.2f}) or shows breakout momentum."
        narrative = "Extremely bullish setup with strong confirmation from fundamentals, technicals, and sentiment."
    elif score >= 8:
        forecast = "BUY"
        confidence = "High"
        entry_suggestion = f"Consider buying if price dips near support (SMA20: {latest.SMA_20:.2f}) and rebounds above SMA50 ({latest.SMA_50:.2f})"
        narrative = "The stock demonstrates bullish technicals, strong fundamentals, and market-positive sentiment. External events are manageable, and analyst momentum is supportive."
    elif score >= 4:
        forecast = "HOLD"
        confidence = "Medium"
        entry_suggestion = f"Watch closely; entry near {latest.SMA_20:.2f} could be favorable if momentum improves"
        narrative = "The situation is balanced with no decisive signal. Market watchers should monitor closely for a directional trend."
    elif score >= 0:
        forecast = "SELL"
        confidence = "Medium"
        entry_suggestion = f"Consider exiting if price drops below {latest.SMA_50:.2f} in the next 3-5 trading days"
        narrative = "The stock exhibits technical and sentiment weakness, potentially amplified by macro headwinds or geopolitical risks."
    else:
        forecast = "STRONG SELL"
        confidence = "High"
        entry_suggestion = f"High risk. Consider exiting or shorting if breakdown below SMA50 ({latest.SMA_50:.2f}) continues"
        narrative = "Severe technical and fundamental weaknesses, combined with negative sentiment and macro headwinds."

    if roe and roe > 0.15:
        final_summary += "The company has strong profitability with solid return on equity. "
    if pe and pe < 20:
        final_summary += "Valuation is attractive based on current PE ratio. "
    if float(info.get('debtToEquity', 0)) > 150:
        final_summary += "Note: The company carries a high level of debt relative to equity. "

    # Determine risk level based on score volatility, sentiment, macro impact
    risk_factors = []
    if geopolitical_impact > 0:
        risk_factors.append("Risk: Market influenced by geopolitical or regulatory developments")
    if sentiment_score < 0:
        risk_factors.append("Risk: Overall public sentiment is negative, may cause downside pressure")
    if latest.Volume < latest.Volume_Avg:
        risk_factors.append("Risk: Current trading volume is lower than average, indicating weak demand")
    if latest.RSI > 70:
        risk_factors.append("Risk: RSI is in overbought territory, suggesting potential pullback")

    risk_level = "Low"
    if len(risk_factors) >= 3:
        risk_level = "High"
    elif len(risk_factors) == 2:
        risk_level = "Moderate"

    reasons.append(f"⚠️ Risk Assessment: {risk_level} risk")
    reasons.extend(risk_factors)

    reasons.append(f"📘 Final Summary: {final_summary.strip()}")


    if debug:
        print("\nSCORE BREAKDOWN:")
        for k, v in score_breakdown.items():
            print(f"- {k}: {v}")
        print(f"Total Score: {score}")

    try:
        highlight_color = '\033[96m'  # Light Cyan
        reset_color = '\033[0m'  # Reset to normal color
        if 'revenueGrowth' in info:
            rev = info['revenueGrowth'] * 100
            rev_qual = "very good" if rev > 30 else "good" if rev > 15 else "not bad" if rev > 5 else "weak"
            reasons.append(
                f"Revenue Growth (YoY): {rev:.2f}% ({rev_qual}) - {highlight_color}shows how much more money the company made this year compared to last year (like your salary hike year-over-year).{reset_color}"
            )
        if 'returnOnEquity' in info:
            roe_val = info['returnOnEquity'] * 100
            roe_qual = "very good" if roe_val > 20 else "good" if roe_val > 10 else "not bad" if roe_val > 5 else "poor"
            roe_earning = (roe_val /
                           100) * 100  # If you invest $100, what is actual earning
            reasons.append(
                f"Return on Equity (ROE): {roe_val:.2f}% ({roe_qual}) - {highlight_color}If investors gave $100, the company would earn approximately ${roe_earning:.2f}. Just like making {roe_val:.2f}% returns from investing your own savings smartly.{reset_color}"
            )
        if 'debtToEquity' in info:
            debt = info['debtToEquity']
            debt_qual = "very good" if debt < 50 else "good" if debt < 100 else "not bad" if debt < 150 else "high"
            reasons.append(
                f"Debt to Equity Ratio: {debt:.2f} ({debt_qual}) -  {highlight_color}shows how much debt the company is using compared to its own money (like how much of your car was bought on loan vs cash){reset_color}"
            )
        if 'grossMargins' in info:
            gm = info['grossMargins'] * 100
            gm_qual = "very good" if gm > 70 else "good" if gm > 50 else "average" if gm > 30 else "low"
            gm_earning = (
                                 gm / 100
                         ) * 100  # Out of $100 sales, how much remains after direct costs
            reasons.append(
                f"Gross Margin: {gm:.2f}% ({gm_qual}) - {highlight_color}Out of every $100 in sales, about ${gm_earning:.2f} remains after covering just the direct production costs (like raw materials and labor), before other expenses like salaries, rent, or ads.{reset_color}"
            )

        if 'profitMargins' in info:
            pm = info['profitMargins'] * 100
            pm_qual = "excellent" if pm > 30 else "good" if pm > 15 else "decent" if pm > 5 else "low"
            reasons.append(
                f"Profit Margin: {pm:.2f}% ({pm_qual}) - {highlight_color}shows how much profit company keeps after all expenses.{reset_color}"
            )

        if 'pegRatio' in info:
            peg = info['pegRatio']
            if peg and peg > 0:
                peg_qual = "undervalued" if peg < 1 else "fairly valued" if peg < 2 else "overvalued"
                reasons.append(
                    f"PEG Ratio: {peg:.2f} ({peg_qual}) - {highlight_color}compares the stock price to earnings growth — under 1 is usually undervalued (like buying a high-potential startup before it explodes).{reset_color}"
                )
        if 'freeCashflow' in info and market_cap:
            fcf_yield = (info['freeCashflow'] / market_cap) * 100
            fcf_qual = "very strong" if fcf_yield > 5 else "healthy" if fcf_yield > 2 else "low"
            reasons.append(
                f"Free Cash Flow Yield: {fcf_yield:.2f}% ({fcf_qual}) - {highlight_color}shows how much cash is left after running the business and paying for assets (like what’s left from your salary after rent, bills, groceries — pure spendable cash).{reset_color}"
            )
        if 'heldPercentInstitutions' in info:
            inst = info['heldPercentInstitutions'] * 100
            inst_qual = "strong backing" if inst > 70 else "moderate" if inst > 30 else "low"
            reasons.append(
                f"Institutional Holdings: {inst:.2f}% ({inst_qual}) - {highlight_color}shows how much of the company is owned by big investors (like mutual funds, banks) — more is usually a vote of confidence.{reset_color}"
            )
        '''if 'dividendYield' in info and info['dividendYield'] is not None:
            raw_div = info['dividendYield']
            if raw_div > 1:  # If Yahoo gave an abnormally high number, treat it correctly
                raw_div = raw_div / 100
        div = raw_div * 100  # Now safe to convert to %
        div_qual = "very good" if div > 4 else "good" if div > 2 else "low"
        reasons.append(
            f"Dividend Yield: {div:.2f}% ({div_qual}) - {highlight_color}shows how much you earn yearly just for holding the stock (like a fixed deposit interest from the company){reset_color}"
        )'''
        # ✅ Dividend Yield logic with MarketBeat fallback

        if 'dividendYield' in info and info['dividendYield'] is not None:
            raw_div = info['dividendYield']

            # Check if Yahoo gave something weird (e.g., >25% usually means bad data)
            if raw_div > 0.25:
                print(f"⚠️ Yahoo dividend yield looks suspicious: {raw_div * 100:.2f}%. Trying MarketBeat...")
                marketbeat_div = fetch_dividend_yield_marketbeat(ticker)
                if marketbeat_div is not None:
                    div = marketbeat_div
                else:
                    div = raw_div * 100  # fallback to Yahoo anyway
            else:
                div = raw_div * 100
        else:
            # Yahoo missing it, try MarketBeat
            marketbeat_div = fetch_dividend_yield_marketbeat(ticker)
            if marketbeat_div is not None:
                div = marketbeat_div
            else:
                div = 0

        div_qual = "very good" if div > 4 else "good" if div > 2 else "low"
        reasons.append(
            f"Dividend Yield: {div:.2f}% ({div_qual}) - {highlight_color}shows how much you earn yearly just for holding the stock (like a fixed deposit interest from the company){reset_color}"
        )


    except:
        reasons.append("Fundamental Metrics: Incomplete or missing")

    # 🔥 SELL Signal Detection
    sell_signals = []

    try:
        # 1. Price below SMA50 (Medium-term Trend Break)
        if latest.Close < latest.SMA_50:
            sell_signals.append(
                f"⚠️ Price is trading below 50-day SMA ({latest.SMA_50:.2f}), suggesting possible trend breakdown."
            )

        # 2. RSI reversal from overbought (>70)
        if latest.RSI < 70 and prev.RSI > 70:
            sell_signals.append(
                f"⚠️ RSI dropped below 70 after being overbought — potential trend reversal."
            )

        # 3. Volume spike + Down close
        if latest.Volume > latest.Volume_Avg and latest.Close < prev.Close:
            sell_signals.append(
                f"⚠️ High volume on a down day — possible distribution happening.")

        # 4. Continuous negative days (3 or more)
        recent_days = df.tail(5)
        negative_days = sum(recent_days['Close'].diff().dropna() < 0)
        if negative_days >= 3:
            sell_signals.append(
                f"⚠️ {negative_days} out of last 5 days were negative — building selling pressure."
            )

    except Exception as e:
        sell_signals.append(
            "⚠️ Unable to fully assess sell signals due to incomplete data.")

    # If any SELL signals detected, add them to Reasons
    if sell_signals:
        reasons.append("🛑 SELL SIGNAL ALERT:")
        reasons.extend(sell_signals)

        # Add SELL Danger Level
        if len(sell_signals) == 1:
            danger_level = "Low"
        elif len(sell_signals) == 2:
            danger_level = "Moderate"
        else:
            danger_level = "High"

        reasons.append(f"🛡️ SELL Danger Level: {danger_level}")

        # 🔥 Added block: Valuation Analysis
    try:
        valuation_status = None
        pe = info.get("trailingPE", None)
        peg = info.get("pegRatio", None)
        fcf_yield = None
        if 'freeCashflow' in info and market_cap:
            fcf_yield = (info['freeCashflow'] / market_cap) * 100

        if peg and peg > 0:
            if peg < 1:
                valuation_status = "undervalued"
            elif peg < 2:
                valuation_status = "fairly valued"
            else:
                valuation_status = "overvalued"
        elif pe:
            if pe < 15:
                valuation_status = "undervalued"
            elif pe <= 25:
                valuation_status = "fairly valued"
            else:
                valuation_status = "overvalued"
        elif fcf_yield is not None:
            if fcf_yield > 5:
                valuation_status = "undervalued"
            elif fcf_yield >= 2:
                valuation_status = "fairly valued"
            else:
                valuation_status = "overvalued"

        if valuation_status:
            reasons.append(
                f"🧠 Valuation Status: Based on financial metrics, this stock appears {valuation_status}."
            )
    except Exception as e:
        reasons.append(
            "Valuation Check: Unable to determine due to missing financial metrics."
        )

    if projected_growth != "Unknown":
        try:
            growth_val = float(projected_growth.strip('%'))
            if growth_val > 0:
                try:
                    past_15 = df.iloc[-15:]
                    start_close = past_15.iloc[0].Close
                    end_close = past_15.iloc[-1].Close
                    change_pct = ((end_close - start_close) / start_close) * 100
                    if change_pct > 0:
                        reasons.append(
                            f"📈 Over the last 15 days, the stock has gained {change_pct:.2f}%, indicating recent buying interest."
                        )
                    elif change_pct < 0:
                        reasons.append(
                            f"📉 Over the last 15 days, the stock has declined {abs(change_pct):.2f}%, indicating potential sell-off pressure."
                        )
                    else:
                        reasons.append(
                            "⏸️ Over the last 15 days, the stock price has remained flat, showing neutral positioning."
                        )
                except:
                    reasons.append(
                        "Unable to compute 15-day movement due to data limitations."
                    )

                final_summary += f"It is showing growth potential with projected 3-month growth of {projected_growth}. "
            else:
                final_summary += f"It may face headwinds, with a projected 3-month decline of {projected_growth}. "
        except:
            pass
            final_summary += f"It may face headwinds, with a projected 3-month decline of {projected_growth}. "

        # 🔥 Summary Table Print
    try:
            summary_forecast = forecast
            summary_danger = danger_level if sell_signals else "None"
            print(f"\n📋 Stock Summary:")
            print(f"| Stock | Forecast | Sell Danger | Suggested Action |")
            print(f"|:------|:---------|:------------|:-----------------|")
            print(
                f"| {company_name} | {summary_forecast} | {summary_danger} | {entry_suggestion} |"
            )
    except Exception as e:
            print(f"⚠️ Unable to print stock summary: {e}")


        # 🚀 Forecast Blast Alert (Big Bold Style)
    blast_color = '\033[0m'
    blast_message = ""

    if forecast == "STRONG BUY":
        blast_color = '\033[1m\033[92m'  # Bold + Bright Green
        blast_message = "🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀\n           STRONG BUY ALERT           \n🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀"
    elif forecast == "BUY":
        blast_color = '\033[1m\033[92m'
        blast_message = "🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢\n             BUY SIGNAL             \n🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢"
    elif forecast == "HOLD":
        blast_color = '\033[1m\033[93m'  # Bold + Yellow
        blast_message = "⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️\n            HOLD ALERT            \n⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️⚖️"
    elif forecast == "SELL":
        blast_color = '\033[1m\033[91m'  # Bold + Light Red
        blast_message = "🛑🛑🛑🛑🛑🛑🛑🛑🛑🛑🛑🛑\n             SELL WARNING             \n🛑🛑🛑🛑🛑🛑🛑🛑🛑🛑🛑🛑"
    elif forecast == "STRONG SELL":
        blast_color = '\033[1m\033[91m'  # Bold + Bright Red
        blast_message = "🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥\n         STRONG SELL ALERT         \n🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥"

    if blast_message:
        print(f"{blast_color}\n{'='*52}\n{blast_message}\n{'='*52}\033[0m\n")

        highlight_color = '\033[96m'
        reset_color = '\033[0m'

        print(
            "\n\033[1m\033[95m================ ENHANCED STOCK INSIGHTS ================\033[0m\n"
        )

        #stock = yf.Ticker(input_symbol)
        #stock = yf.Ticker(ticker_symbol if 'ticker_symbol' in locals() else input_symbol)
# This is inside analyze_trend() function, after the Forecast blast alert.

# ==================== ENHANCED STOCK INSIGHTS =====================

    from datetime import datetime
    import numpy as np

    print(
        "\n\033[1m\033[95m================ ENHANCED STOCK INSIGHTS ================\033[0m\n"
    )

    symbol_to_use = ticker_symbol if 'ticker_symbol' in locals() else input_symbol

    if not symbol_to_use or symbol_to_use.upper() in ['GLOBAL', 'GLOBAL-DESIRED']:
        print(f"⚠️ {symbol_to_use}: Skipping enhanced insights (invalid ticker).")
        buy_date_suggestion = None
        sell_by_suggestion = None

    else:
        try:
            stock = yf.Ticker(symbol_to_use)
            end_date = pd.Timestamp.today()
            start_date = end_date - pd.DateOffset(years=3)
            hist = stock.history(start=start_date.strftime('%Y-%m-%d'),
                                 end=end_date.strftime('%Y-%m-%d'),
                                 interval='1d')

            if hist.empty:
                print(f"⚠️ {symbol_to_use}: No historical data found. Skipping enhanced insights.")
            else:
                # MACD calculation
                exp1 = hist['Close'].ewm(span=12, adjust=False).mean()
                exp2 = hist['Close'].ewm(span=26, adjust=False).mean()
                macd = exp1 - exp2
                signal = macd.ewm(span=9, adjust=False).mean()

                if macd.iloc[-1] > signal.iloc[-1]:
                    print(
                        f"\033[92m🔍 MACD Signal: Bullish Crossover - Like a green traffic light 🚦 (momentum turning upwards)\033[0m"
                    )
                else:
                    print(
                        f"\033[91m🔍 MACD Signal: Bearish Crossover - Like a red traffic light 🚦 (momentum turning downwards)\033[0m"
                    )

                # Earnings Proximity
                try:
                    next_earning_date_str = fetch_next_earnings(stock)  # Pass yf.Ticker object
                    if next_earning_date_str:
                        next_earning_date = pd.to_datetime(next_earning_date_str)
                        days_to_earnings = (next_earning_date - pd.Timestamp.today()).days

                        if 0 <= days_to_earnings <= 15:
                            print(
                                f"\033[93m📅 Earnings Risk: Earnings in {days_to_earnings} days - Like approaching stormy weather 🌩️ (be cautious)\033[0m"
                            )
                        elif days_to_earnings < 0:
                            print(
                                f"\033[91m📅 Earnings Risk: Earnings date might be old or invalid - double check ⚠️\033[0m"
                            )
                        else:
                            print(
                                f"\033[92m📅 Earnings Risk: No earnings in the next 15 days - Smooth sailing 🚤\033[0m"
                            )
                    else:
                        print(
                            f"\033[93m📅 Earnings Risk: No earnings information available - sail carefully 🚤\033[0m"
                        )
                except Exception as e:
                    print(f"\033[91mError calculating earnings risk: {e}\033[0m")

                # Insider Activity Simulation
                insider_buying = np.random.choice([True, False], p=[0.6, 0.4])
                if insider_buying:
                    print(
                        f"\033[92m🕵️‍♂️ Insider Activity: Heavy Insider Buying - Like the chef eating his own cooking 🍽️\033[0m"
                    )
                else:
                    print(
                        f"\033[91m🕵️‍♂️ Insider Activity: Heavy Insider Selling - Like the chef refusing his own dish 🍽️\033[0m"
                    )

                # Sector Strength Simulation
                sector_strong = np.random.choice([True, False], p=[0.7, 0.3])
                if sector_strong:
                    print(
                        f"\033[92m🌎 Sector Strength: Sector is Rising - Rising tide lifts all boats 🌊\033[0m"
                    )
                else:
                    print(
                        f"\033[91m🌎 Sector Strength: Sector is Weak - Even strong ships struggle in low tide 🌊\033[0m"
                    )

                # Danger Level Calculation
                danger_score = 1
                if macd.iloc[-1] < signal.iloc[-1]:
                    danger_score += 1
                if not insider_buying:
                    danger_score += 1
                if not sector_strong:
                    danger_score += 1
                if 'days_to_earnings' in locals() and days_to_earnings <= 15:
                    danger_score += 1

                print(f"\n\033[1m🔥 Danger Level: {danger_score}/5\033[0m")
                if danger_score <= 2:
                    print("🧘 Calm Waters - Low Risk")
                elif danger_score <= 4:
                    print("⚡ Choppy Waves - Moderate Risk")
                else:
                    print("🌋 Red Hot Volcano - High Risk")

                print(
                    "\n\033[1m\033[95m===========================================================\033[0m\n"
                )

        except Exception as e:
            print(f"⚠️ Error processing {symbol_to_use}: {e}")



    # Earnings Proximity using fetch_next_earnings()
        try:
            next_earning_date_str = fetch_next_earnings(stock)  # Pass yf.Ticker object
            if next_earning_date_str:
                next_earning_date = pd.to_datetime(next_earning_date_str)
                days_to_earnings = (next_earning_date - pd.Timestamp.today()).days

                if 0 <= days_to_earnings <= 15:
                    print(
                        f"\033[93m📅 Earnings Risk: Earnings in {days_to_earnings} days - Like approaching stormy weather 🌩️ (be cautious)\033[0m"
                    )
                elif days_to_earnings < 0:
                    print(
                        f"\033[91m📅 Earnings Risk: Earnings date might be old or invalid - double check ⚠️\033[0m"
                    )
                else:
                    print(
                        f"\033[92m📅 Earnings Risk: No earnings in the next 15 days - Smooth sailing 🚤\033[0m"
                    )
            else:
                print(
                    f"\033[93m📅 Earnings Risk: No earnings information available - sail carefully 🚤\033[0m"
                )
        except Exception as e:
            print(f"\033[91mError calculating earnings risk: {e}\033[0m")

        # Insider Activity - Simulated (for now)
        insider_buying = np.random.choice([True, False],
                                          p=[0.6,
                                             0.4])  # Simulate with bias toward buying
        if insider_buying:
            print(
                f"\033[92m🕵️‍♂️ Insider Activity: Heavy Insider Buying - Like the chef eating his own cooking 🍽️ (confidence in company)\033[0m"
            )
        else:
            print(
                f"\033[91m🕵️‍♂️ Insider Activity: Heavy Insider Selling - Like the chef refusing his own dish 🍽️ (possible caution)\033[0m"
            )

        # Sector Strength - Simulated (for now)
        sector_strong = np.random.choice([True, False], p=[0.7, 0.3])
        if sector_strong:
            print(
                f"\033[92m🌎 Sector Strength: Sector is Rising - Rising tide lifts all boats 🌊\033[0m"
            )
        else:
            print(
                f"\033[91m🌎 Sector Strength: Sector is Weak - Even strong ships struggle in low tide 🌊\033[0m"
            )

        # Danger Level Risk Score
        danger_score = 1
        if macd.iloc[-1] < signal.iloc[-1]:
            danger_score += 1
        if not insider_buying:
            danger_score += 1
        if not sector_strong:
            danger_score += 1
        if 'days_to_earnings' in locals() and days_to_earnings <= 15:
            danger_score += 1

        print(f"\n\033[1m🔥 Danger Level: {danger_score}/5\033[0m")
        if danger_score <= 2:
            print("🧘 Calm Waters - Low Risk")
        elif danger_score <= 4:
            print("⚡ Choppy Waves - Moderate Risk")
        else:
            print("🌋 Red Hot Volcano - High Risk")

        print(
            "\n\033[1m\033[95m===========================================================\033[0m\n"
        )

        # =================== ENHANCED NEWS SENTIMENT USING FETCH FUNCTION ====================
        try:
            if 'fetch_news_sentiment' in globals():
                sentiment_score_from_news, geopolitical_impact_from_news, news_headlines = fetch_news_sentiment(
                    stock.ticker)

                positive = 0
                neutral = 0
                negative = 0

                from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
                analyzer = SentimentIntensityAnalyzer()

                for headline in news_headlines:
                    if not headline:
                        continue
                    vs = analyzer.polarity_scores(headline)
                    if vs['compound'] >= 0.05:
                        positive += 1
                    elif vs['compound'] <= -0.05:
                        negative += 1
                    else:
                        neutral += 1

                print(
                    "\n\033[1m\033[94m================ LIVE NEWS SENTIMENT =================\033[0m\n"
                )
                print(f"📰 Total Headlines Analyzed: {len(news_headlines)}")
                print(f"\033[92m✅ Positive: {positive}\033[0m")
                print(f"\033[93m⚠️ Neutral: {neutral}\033[0m")
                print(f"\033[91m❌ Negative: {negative}\033[0m")

                # Calculate sentiment mood
                score_description = ""
                if sentiment_score_from_news >= 5:
                    score_description = "(Very Strong Positive)"
                elif sentiment_score_from_news >= 2:
                    score_description = "(Moderate Positive)"
                elif sentiment_score_from_news <= -5:
                    score_description = "(Very Strong Negative)"
                elif sentiment_score_from_news <= -2:
                    score_description = "(Moderate Negative)"
                else:
                    score_description = "(Neutral)"

                print(
                    f"📊 Net News Sentiment Score: {sentiment_score_from_news} {score_description}"
                )

                # Danger Meter and Decision based on Net Sentiment
                if sentiment_score_from_news >= 5:
                    print(
                        "\033[92m🧘 News Danger Meter: LOW — Positive news dominance. Action: STRONG BUY / BUY\033[0m"
                    )
                elif sentiment_score_from_news >= 2:
                    print(
                        "\033[92m🌤️ News Danger Meter: MILD — Slight bullish bias. Action: BUY / HOLD\033[0m"
                    )
                elif sentiment_score_from_news <= -5:
                    print(
                        "\033[91m🌩️ News Danger Meter: SEVERE — Heavy negative news. Action: STRONG SELL / SELL\033[0m"
                    )
                elif sentiment_score_from_news <= -2:
                    print(
                        "\033[91m⛈️ News Danger Meter: MODERATE — Warning signs in news. Action: SELL / HOLD cautiously\033[0m"
                    )
                else:
                    print(
                        "\033[93m🌫️ News Danger Meter: NEUTRAL — Mixed/low impact news. Action: HOLD\033[0m"
                    )

                print(
                    "\n\033[1m\033[94m========================================================\033[0m\n"
                )

                # Boost or Adjust Main Score based on News Sentiment
                if sentiment_score_from_news >= 5:
                    score += 2
                    reasons.append(
                        "📈 News sentiment extremely positive based on Google News feed."
                    )
                elif sentiment_score_from_news >= 2:
                    score += 1
                    reasons.append(
                        "📈 News sentiment moderately positive based on Google News feed."
                    )
                elif sentiment_score_from_news <= -5:
                    score -= 2
                    reasons.append(
                        "📉 News sentiment extremely negative based on Google News feed."
                    )
                elif sentiment_score_from_news <= -2:
                    score -= 1
                    reasons.append(
                        "📉 News sentiment moderately negative based on Google News feed."
                    )

                # Also consider geopolitical influence separately
                if geopolitical_impact_from_news > 0:
                    reasons.append(
                        f"🌍 Geopolitical Influence Detected: {geopolitical_impact_from_news} mentions — may introduce external risks."
                    )

                else:
                 print(
                    "\033[93m⚠️ fetch_news_sentiment function not available — skipping news sentiment enhancement.\033[0m"
                    )

        except Exception as e:
         print(
            f"\033[91m⚠️ Error during enhanced news sentiment analysis: {e}\033[0m"
            )

        # 🛠️ Suggest Buy and Sell Dates
        from datetime import datetime, timedelta

        buy_date_suggestion = None
        sell_by_suggestion = None

        today = datetime.today().strftime('%Y-%m-%d')

        current_price = latest.Close
        sma20 = latest.SMA_20
        sma50 = latest.SMA_50

        # Suggest Buy Date
        if current_price >= sma20 * 1.02:  # More than 2% above SMA20
            buy_date_suggestion = (
                f"Already in breakout zone. Consider buying on dips or consolidation near ${sma20:.2f}."
            )
        elif abs(current_price - sma20) / sma20 <= 0.02:  # Within 2% of SMA20
            buy_date_suggestion = (
                f"Buy on pullback near SMA20 (${sma20:.2f})."
            )
        else:
            buy_date_suggestion = (
                f"Monitor closely — buy signal will strengthen if price rebounds above SMA20 (${sma20:.2f})."
            )

    from pandas.tseries.offsets import BDay  # Make sure you import this at the top

        # Suggest Sell Date
    if earnings_date:
            try:
                earnings_dt = pd.to_datetime(earnings_date)
                sell_by_date = earnings_dt - pd.Timedelta(days=7)
                sell_by_suggestion = (
                    f"Sell by {sell_by_date.strftime('%Y-%m-%d')} (before earnings on {earnings_dt.strftime('%Y-%m-%d')})."
                )
            except:
                # If earnings parsing fails, fallback
                estimated_sell_date = (pd.Timestamp.today() + BDay(25)).strftime("%Y-%m-%d")
                sell_by_suggestion = f"Sell by {estimated_sell_date} unless trend weakens."
    else:
            # No earnings date available, suggest estimated sell date
            estimated_sell_date = (pd.Timestamp.today() + BDay(25)).strftime("%Y-%m-%d")
            sell_by_suggestion = f"Sell by {estimated_sell_date} unless trend weakens."


    # 🚀 FINAL RETURN #

    return StockForecast(
                forecast=forecast,
                confidence=confidence,
                entry_suggestion=entry_suggestion,
                narrative=narrative,
                reasons=reasons,
                score_breakdown=score_breakdown,
                buy_date_suggestion=buy_date_suggestion,
                sell_by_suggestion=sell_by_suggestion
        )



# ---------------------
# Main Analysis Logic
# ---------------------
def analyze_trend_yahoo(ticker):
    score = 0
    reasons = []

    if check_earnings_coming(ticker):
        score += 1
        reasons.append("📆 Earnings within window")

    earnings = get_historical_earnings(ticker)
    if earnings is not None and (earnings['Earnings']
                                 > earnings['Estimate']).sum() >= 3:
        score += 1
        reasons.append("📊 History of beating EPS")

    momentum = get_momentum(ticker)
    if momentum > 0.05:
        score += 1
        reasons.append(f"📈 Positive price momentum ({momentum:.2%})")

    rating = get_analyst_rating(ticker)
    if rating >= 2:
        score += 1
        reasons.append(f"🗣️ Analyst Buy Ratings (last 10: {rating})")

    if get_fundamentals(ticker):
        score += 1
        reasons.append("💰 Healthy net profit margins (>10%)")

    forecast = get_forecast_outlook(ticker)

    return {
        'ticker': ticker,
        'score': score,
        'reasons': reasons,
        'forecast': forecast
    }


# Utility function to load a list of US stocks (example: S&P 500)
def load_sp500_symbols():
    url = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"
    df = pd.read_csv(url)
    return df['Symbol'].tolist()[:20]  # limit to top 50 for performance demo


if __name__ == "__main__":
    import sys
    import os
    import time
    import yfinance as yf

    if len(sys.argv) > 1:
        input_symbol = sys.argv[1].upper()
        top_n = int(sys.argv[2]) if len(sys.argv) > 2 else 30

        if input_symbol in ["GLOBAL", "GLOBAL-DESIRED"]:
            print(f"🌎 {input_symbol} scan mode triggered...")

            project_root = "C:/Users/pramo/IdeaProjects/stock_analysis/downloads"
            csv_files = [f for f in os.listdir(project_root) if f.endswith(".csv")]

            if not csv_files:
                print(f"⚠️ No CSV files found in downloads folder: {project_root}")
                sys.exit(1)

            csv_path = os.path.join(project_root, csv_files[0])
            csv_path = os.path.normpath(csv_path)
            print(f"📄 Reading tickers from: {csv_path.replace(os.sep, '/')}")

            if not os.path.exists(csv_path):
                print(f"⚠️ CSV not found at path: {csv_path}")
                sys.exit(1)

            symbols_list = read_symbols_from_csv(csv_path)
            print(f"✅ Found {len(symbols_list)} symbols to analyze.")

            if input_symbol == "GLOBAL":
                symbols_to_process = symbols_list[:top_n]
            else:  # GLOBAL-DESIRED
                import random
                symbols_to_process = random.sample(symbols_list, min(top_n, len(symbols_list)))

            top_stocks = scan_stocks_and_find_strong_buys(symbols_to_process, top_n)

            print("\n🏆 Top Strong Buy Stocks:")
            print(top_stocks)

        else:
            try:
                ticker = yf.Ticker(input_symbol)
                info = ticker.get_info()
            except Exception as e:
                print(f"❗ Failed to fetch data for {input_symbol}: {e}")
                sys.exit(1)

            quote_type = info.get("quoteType", "").lower()

            if quote_type == "etf":
                etf_analysis = analyze_etf(input_symbol)
                print("\n📌 SUMMARY:")
                print(etf_analysis["summary"])
                #print("\n📋 REASONS:")
                #for reason in etf_analysis["reasons"]:
                    #print(reason)

            elif quote_type in ["equity", "stock"]:
                df, info, recommendations, price_target, ticker = fetch_stock_data(input_symbol)
                sentiment_score, geopolitical_impact, headlines = fetch_news_sentiment(input_symbol)
                earnings_date = fetch_next_earnings(ticker)

                decision = analyze_trend(df, info, sentiment_score, geopolitical_impact,
                                         recommendations, price_target, earnings_date, input_symbol)

                print(f"\nStock: {input_symbol}")
                print("Recent Headlines:")
                sorted_headlines = sorted(headlines, key=lambda x: x['published'], reverse=True)
                for h in sorted_headlines:
                    print(f"- [{h['published']}] {h['title']}")

                print(f"\nForecast: {decision.forecast} ({decision.confidence})")
                print(f"Suggested Action: {decision.entry_suggestion}")
                print(f"\nAI Summary: {decision.narrative}")
                print("Reasons:")
                for reason in decision.reasons:
                    print("-", reason)

                print(f"\n📅 Buy Suggestion: {decision.buy_date_suggestion or 'Not Available'}")
                print(f"📅 Sell Suggestion: {decision.sell_by_suggestion or 'Not Available'}")
                print(f"\nNext Earnings Date: {earnings_date if earnings_date else 'Not available'}")

            else:
                print("❗ Unsupported asset type or missing data.")

    else:
        print("📊 Scanning top 50 S&P 500 stocks for best buy signals...")
        symbols = load_sp500_symbols()
        results = []

        for symbol in symbols:
            try:
                df, info, recommendations, price_target, ticker = fetch_stock_data(symbol)
                sentiment_score, geopolitical_impact, headlines = fetch_news_sentiment(symbol)
                earnings_date = fetch_next_earnings(ticker)

                input_symbol = symbol

                decision = analyze_trend(df, info, sentiment_score, geopolitical_impact,
                                         recommendations, price_target, earnings_date, input_symbol)

                if decision.forecast == 'BUY':
                    results.append({
                        'symbol': symbol,
                        'score': decision.confidence,
                        'summary': decision.narrative,
                        'action': decision.entry_suggestion,
                        'reasons': decision.reasons[:3],
                        'earnings': earnings_date
                    })

            except Exception as e:
                print(f"Error processing {symbol}: {e}")
            time.sleep(1)

        if results:
            print("🏆 Top 5 BUY Signals Today:")
            for i, res in enumerate(results[:5], 1):
                print(f"{i}. 🔹 {res['symbol']}")
                print(f"   Forecast: {res['summary']}")
                print(f"   Suggested Entry: {res['action']}")
                print(f"   Earnings Date: {res['earnings'] or 'Unknown'}")
                print("   Key Reasons:")
                for reason in res['reasons']:
                    print(f"   - {reason}")
        else:
            print("No strong BUY signals found today.")



