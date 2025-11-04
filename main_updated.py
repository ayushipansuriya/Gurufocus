import asyncio
import pathlib
import numpy as np
from dotenv import load_dotenv
from playwright.async_api import async_playwright
import pandas as pd
from pathlib import Path
from collections import Counter
import os, math
import logging
from xlsxwriter.utility import xl_col_to_name


# Load environment variables
load_dotenv()
GF_USER = os.getenv("GF_USER")
GF_PASS = os.getenv("GF_PASS")
TARGET_URL = "https://www.gurufocus.com/guru/top-holdings"

#--------------downloaded and cleaning row file save in this folder------------
DOWNLOADS_DIR = pathlib.Path("downloads")
DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)

#----------------Final file save in this folder-------------
FINAL_FILE_DIR= pathlib.Path("Final_File")
FINAL_FILE_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------Configure Logging --------------------------------
log_file = "Opration_logging.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

#------------------------ Login into Gurufocus---------------------------
async def login_and_go_to_top_holdings(context):
    """Login into gurufocus.com."""

    page = await context.new_page()

    # Go to login page
    await page.goto("https://www.gurufocus.com/login")

    # Fill username and password
    await page.fill("input[name='username'], input#login-dialog-name-input", GF_USER)
    await page.fill("input[name='password'], input#login-dialog-pass-input", GF_PASS)

    # Click login button
    await page.click("button[type='submit'], button.el-button--submit", force=True)

    # Wait for login button to disappear (indicating login success)
    await page.wait_for_selector("button[type='submit'], button.el-button--submit", state="detached", timeout=30000)

    # Wait a short time to ensure homepage loads fully
    await page.wait_for_timeout(3000)

    # Navigate to Top Holdings page
    await page.goto(TARGET_URL, wait_until="networkidle")
    return page

#------------------------ Cleaning Operation----------------------------
def delete(file_path):
    """Perform Cleanning Operation."""
    # Read Excel File
    try:
        logger.info("🔹 Reading Excel file...")
        df = pd.read_excel(file_path, header=None)
        logger.info("✅ File read successfully.")
    except Exception as e:
        logger.error(f"❌ Error reading Excel file: {e}")
        return None

    # 1. Dropping the first 4 columns (A–D)
    try:
        logger.info("🔹 Dropping first 4 columns (A–D)...")
        df = df.iloc[:, 4:]
        logger.info("✅ Columns dropped.")
    except Exception as e:
        logger.error(f"❌ Error dropping columns: {e}")
        return None

    # 2. Dropping the first 5 rows (1–5)
    try:
        logger.info("🔹 Dropping first 5 rows (1–5)...")
        df = df.iloc[5:, :].reset_index(drop=True)
        logger.info("✅ Rows dropped.")
    except Exception as e:
        logger.error(f"❌ Error dropping rows: {e}")
        return None

    # 3. Removing all text inside parentheses () including the brackets
    try:
        logger.info("🔹 Removing all text inside ( ) including brackets...")
        df = df.apply(lambda col: col.astype(str).str.replace(r'\([^\)]*\)', '', regex=True))
        logger.info("✅ Brackets content removed.")
    except Exception as e:
        logger.error(f"❌ Error removing brackets content: {e}")
        return None

    #  4. Replacing all zeros (0, 0.0, "0", "0.00") with empty cells
    try:
        logger.info("🔹 Replacing all 0.00 values with empty cells...")
        df = df.replace({0: "", 0.0: "", "0": "", "0.00": ""})
        logger.info("✅ Zero values removed.")
    except Exception as e:
        logger.error(f"❌ Error removing zero values: {e}")
        return None

    # 5. Saving the cleaned file as *_cleaned.xlsx
    try:
        logger.info("🔹 Saving cleaned file...")
        p = Path(file_path)
        new_path = p.with_name(p.stem + "_cleaned.xlsx")
        df.to_excel(new_path, index=False, header=False, engine="openpyxl")
        logger.info(f"✅ Cleaned file saved: {new_path}")
        return new_path
    except Exception as e:
        logger.error(f"❌ Error saving cleaned file: {e}")
        return None


def merge_or_rename(file_path: str) -> str | None:
    """Rename Goog and BRK.A."""
    try:
        logger.info("🔹 Loading cleaned Excel file...")
        df = pd.read_excel(file_path)
        logger.info("✅ File loaded successfully.")

        # Replace GOOG with GOOGL
        try:
            logger.info("🔹 Replacing GOOG with GOOGL...")
            df = df.replace("GOOG", "GOOGL")
            logger.info("✅ Replacement done for GOOG → GOOGL.")
        except Exception as e:
            logger.error(f"❌ Error replacing GOOG: {e}")

        # Replace BRK.A with BRK.B
        try:
            logger.info("🔹 Replacing BRK.A with BRK.B...")
            df = df.replace("BRK.A", "BRK.B")
            logger.info("✅ Replacement done for BRK.A → BRK.B.")
        except Exception as e:
            logger.error(f"❌ Error replacing BRK.A: {e}")

        # Save result
        p = Path(file_path)
        new_path = p.with_name(p.stem + "_rename.xlsx")  # ensures proper extension
        df.to_excel(new_path, index=False, header=True, engine="openpyxl")
        logger.info(f"✅ Updated file saved: {new_path}")
        return new_path

    except Exception as e:
        logger.error(f"❌ Error processing on Merge and Rename: {e}")
        return None


def filter_adr_file(adr_file_path, output_path=None):
    """
    Filters an ADR file to include only NYSE and NASDAQ listings.
    Loads a CSV or Excel ADR file, ensures a 'Ticker' column, replaces '%2F' with '/',
    and removes non-NYSE/NASDAQ entries. Saves the cleaned result as a CSV.
    """
    # Load ADR file
    if adr_file_path.endswith(".csv"):
        df_adr = pd.read_csv(adr_file_path)
    else:
        df_adr = pd.read_excel(adr_file_path)

    # Normalize columns
    if "Ticker" not in df_adr.columns:
        if "Symbol" in df_adr.columns:
            df_adr["Ticker"] = df_adr["Symbol"].astype(str).str.upper()
        else:
            raise ValueError("No 'Ticker' or 'Symbol' column in ADR file")

    # Replace %2F → / in place (only if the column exists)
    if "Ticker" in df_adr.columns:
        df_adr["Ticker"] = df_adr["Ticker"].astype(str).str.replace("%2F", "/", regex=False)
    if "Symbol" in df_adr.columns:
        df_adr["Symbol"] = df_adr["Symbol"].astype(str).str.replace("%2F", "/", regex=False)

    # Filter exchanges: only NYSE or NASDAQ
    df_filtered = df_adr[df_adr["Exchange"].isin(["NYSE", "NASDAQ"])].copy()

    # Keep only Ticker (drop Symbol column)
    df_filtered = df_filtered.drop(columns=["Symbol"], errors="ignore")

    # Optional: save filtered ADR file
    if not output_path:
        output_path = Path(adr_file_path).with_name(Path(adr_file_path).stem + "_NYSE_NASDAQ.csv")
    df_filtered.to_csv(output_path, index=False)
    logger.info(f"✅ Filtered ADR file saved → {output_path}")
    return output_path


# ------------------------------------------------------------------------------------------------
def split_tickers_flat(file_path: Path):
    """Split tickers into two lists: with colon and without colon"""
    df = pd.read_excel(file_path, header=None, dtype=str).fillna('')

    # Flatten all tickers into a single Series
    tickers = df.astype(str).stack().str.strip()

    list1 = tickers[~tickers.str.contains(":")].reset_index(drop=True)
    list2 = tickers[tickers.str.contains(":")].reset_index(drop=True)

    list1_path = Path(file_path).with_name("list1_no_colon.xlsx")
    list2_path = Path(file_path).with_name("list2_with_colon.xlsx")

    list1.to_frame(name="Ticker").to_excel(list1_path, index=False)
    list2.to_frame(name="Ticker").to_excel(list2_path, index=False)

    return list1_path, list2_path


def count_frequency(file_path: Path) -> Path:
    """Count how many times each ticker appears in the file."""
    df = pd.read_excel(file_path, dtype=str).fillna('')
    tickers = df.stack().astype(str).str.upper().tolist()

    # Remove empty strings
    tickers = [t for t in tickers if t.strip() != '']

    counts = Counter(tickers)
    df_counts = pd.DataFrame(counts.items(), columns=["Ticker", "Count"]).sort_values(by="Count", ascending=False)

    output_path = Path(file_path).with_name(Path(file_path).stem + "_frequency.xlsx")
    df_counts.to_excel(output_path, index=False)
    logger.info(f"✅ Frequency saved → {output_path}")
    return output_path


async def match_tickers_without_colon(user_file, adr_file, freq_file=None, use_before_colon=False):
    """ Matching /filtering without colon ticker with this source file """
    df_user = pd.read_excel(user_file)
    if adr_file.endswith(".csv"):
        df_adr = pd.read_csv(adr_file)
    else:
        df_adr = pd.read_excel(adr_file)

    # Normalize ADR/TradingView column
    if "Symbol" in df_adr.columns:
        df_adr["Ticker"] = df_adr["Symbol"].astype(str).str.upper()
    elif "Ticker" in df_adr.columns:
        df_adr["Ticker"] = df_adr["Ticker"].astype(str).str.upper()
    else:
        raise ValueError("No Symbol or Ticker column found in ADR/TradingView file")

    tickers = df_user["Ticker"].astype(str).str.upper()

    if use_before_colon:
        tickers = tickers.str.split(":").str[-1]

    user_tickers = tickers.tolist()

    matched = df_adr[df_adr["Ticker"].astype(str).str.upper().isin(user_tickers)]

    # Keep only the Ticker column in excel file
    matched = matched[["Ticker"]]

    # If frequency file is provided, merge count
    if freq_file:
        df_freq = pd.read_excel(freq_file)
        if use_before_colon:
            df_freq["Ticker"] = df_freq["Ticker"].astype(str).str.split(":").str[-1].str.upper()
        matched = matched.merge(df_freq, how="left", left_on="Ticker", right_on="Ticker")

        # Drop rows where Ticker is NaN after merge
        matched = matched.dropna(subset=["Ticker"])

    matched_path = Path(user_file).with_name(Path(user_file).stem + "_adr_matched.xlsx")
    matched.to_excel(matched_path, index=False)
    logger.info(f"✅ Matched tickers saved → {matched_path}")
    return matched_path


async def match_tickers_with_colon(user_file, other_file, freq_file=None):
    """ Matching /filtering with colon ticker with this source file """
    # Load user file
    df_user = pd.read_excel(user_file)
    user_tickers = df_user["Ticker"].astype(str).tolist()  # keep as-is

    # Load other file (CSV or Excel)
    if other_file.endswith(".csv"):
        df_other = pd.read_csv(other_file)
    else:
        df_other = pd.read_excel(other_file)

    # Ensure ticker column exists
    if "Ticker" not in df_other.columns:
        if "Symbol" in df_other.columns:
            df_other["Ticker"] = df_other["Symbol"].astype(str)
        else:
            raise ValueError("No Ticker or Symbol column found in other file")

    # Keep only tickers present in user file
    matched = df_other[df_other["Ticker"].isin(user_tickers)].copy()

    # Filter out rows where 'NYSE/NASDAQ' column is '-'
    if "NYSE/NASDAQ" not in matched.columns:
        raise ValueError("No 'NYSE/NASDAQ' column found in other file")

    matched = matched[matched["NYSE/NASDAQ"] != "-"]


    # Keep both old and new tickers
    matched = matched.rename(columns={"Ticker": "OriginalTicker"})
    matched["Ticker"] = matched["NYSE/NASDAQ"]  # renamed column

    # Remove duplicates based on final Ticker
    matched = matched.drop_duplicates(subset=["Ticker"])

    # Select relevant columns
    matched = matched[["OriginalTicker", "Ticker"]]

    # Merge frequency if provided (using OriginalTicker for lookup)
    if freq_file:
        df_freq = pd.read_excel(freq_file)
        df_freq["Ticker"] = df_freq["Ticker"].astype(str)
        matched = matched.merge(df_freq, how="left", left_on="OriginalTicker", right_on="Ticker")
        if "Ticker_y" in matched.columns:
            matched = matched.drop(columns=["Ticker_y"])
        if "Ticker_x" in matched.columns:
            matched = matched.rename(columns={"Ticker_x": "Ticker"})

    # Save result
    matched_path = Path(user_file).with_name(Path(user_file).stem + "_matched_filtered.xlsx")
    matched.to_excel(matched_path, index=False)
    logger.info(f"✅ Matched tickers saved → {matched_path}")
    return matched_path


def merge_final_lists(list1_file, list2_file):
    """ Merge with colon and without colon ticker in one file"""
    df1 = pd.read_excel(list1_file)[["Ticker", "Count"]]
    df2 = pd.read_excel(list2_file)[["Ticker", "Count"]]

    merged = pd.concat([df1, df2], ignore_index=True)

    # Remove duplicates based on 'Ticker' column
    if "Ticker" in merged.columns:
        merged = merged.drop_duplicates(subset="Ticker", keep="first")
    else:
        # If no column name, remove fully identical rows
        merged = merged.drop_duplicates(keep="first")

    final_path = Path(list1_file).with_name("final_merged_list.xlsx")
    merged.to_excel(final_path, index=False)
    logger.info(f"✅ Final merged list saved → {final_path}")
    return final_path


def split_tickers_by_count_csv(final_file):
    """
    Splits tickers into repeated (count >=2) and single (count=1) groups.
    Saves two CSV files with only the 'Ticker' column.
    """
    # Load the final merged file
    df = pd.read_excel(final_file)

    if "Count" not in df.columns or "Ticker" not in df.columns:
        raise ValueError("Final file must contain 'Ticker' and 'Count' columns")

    # Repeated tickers: count >= 2
    repeated = df[df["Count"] >= 2][["Ticker"]]
    repeated_path = Path(final_file).with_name("tickers_repeated.csv")
    repeated.to_csv(repeated_path, index=False)

    # Single occurrence: count = 1
    single = df[df["Count"] == 1][["Ticker"]]
    single_path = Path(final_file).with_name("tickers_single.csv")
    single.to_csv(single_path, index=False)

    logger.info(f"✅ Repeated tickers saved → {repeated_path}")
    logger.info(f"✅ Single tickers saved → {single_path}")

    return repeated_path, single_path


# ------------------------------Create and Download portfolio------------------------------------

async def create_guru_portfolio_api(page, csv_path: str, portfolio_name: str, desc: str = "", private: int = 1):

    """ Create portfolio in gurufocus through the gurufocus and return the created portfolio Id"""
    df = pd.read_csv(csv_path)

    # Detect the column containing tickers
    ticker_col = None
    for col in df.columns:
        if "ticker" in col.lower():
            ticker_col = col
            break
    if not ticker_col:
        raise ValueError("No column named like 'Ticker' found in CSV.")

    # Extract non-empty tickers
    tickers = [t.strip().upper() for t in df[ticker_col].dropna() if str(t).strip()]
    if not tickers:
        raise ValueError(f"No valid tickers found in {csv_path}.")

    logger.info(f"📈 Creating portfolio '{portfolio_name}' with {len(tickers)} tickers...")

    payload = {
        "name": portfolio_name,
        "private": private,
        "desc": desc,
        "stocks": tickers,
    }

    result = await page.evaluate(
        """async (payload) => {
            try {
                const res = await fetch("https://www.gurufocus.com/reader/_api/v2/portfolios?v=1.7.52", {
                    method: "POST",
                    headers: {
                        "accept": "application/json, text/plain, */*",
                        "content-type": "application/json"
                    },
                    body: JSON.stringify(payload)
                });
                const text = await res.text();
                try { return JSON.parse(text); } 
                catch { return { raw: text }; }
            } catch (err) {
                return { error: err.toString() };
            }
        }""",
        payload,
    )

    logger.info(f"Portfolio '{portfolio_name}' creation response:", result)
    return result


async def download_gurufocus_portfolio_api(page, portfolio_id: int, save_as: str = None, ranking: bool = False):
    """
    Fetches portfolio from GuruFocus and writes a FINAL analysis Excel with:
      - Full analysis table (A..AK) with 4-decimal precision
      - TOP/WORST 25 for Group 1 (1W,1M,YTD,3M) and Group 2 (1W,1M,YTD,3M,6M,1Y)
      - 4 embedded charts inside Excel (TOP/WORST per group)
    """
    if not save_as:
        save_as = f"portfolio_{portfolio_id}.xlsx"

    save_path = FINAL_FILE_DIR / save_as
    logger.info(f"Downloading portfolio {portfolio_id} ...")

    url = f"https://www.gurufocus.com/reader/_api/v2/portfolios/{portfolio_id}?v=1.7.52"

    # Fetch via the authenticated browser context
    result = await page.evaluate(
        """async (url) => {
            try {
                const res = await fetch(url, {
                    method: "POST",
                    headers: {
                        "accept": "application/json, text/plain, */*",
                        "content-type": "application/json"
                    },
                    body: JSON.stringify({
                        fields: [
                            "symbol","mktcap","stockid","display_symbol","pe","beta","currency_symbol","delisted",
                            "company","exchange","price","currency","morn_secr_id","p_pct_change","mktcap_norm",
                            "volume","volume_3m","pchange_1w","pchange_mtd","pchange_4w","pchange_12w","pchange_24w",
                            "pchange_52w","pchange_ytd","pchange_3y","pchange_5y","pchange_10y","pchange_15y",
                            "pchange_20y","pchangeSP_1w","pchangeSP_mtd","pchangeSP_4w","pchangeSP_12w","pchangeSP_24w",
                            "pchangeSP_52w","pchangeSP_ytd","pchangeSP_3y","pchangeSP_5y","pchangeSP_10y",
                            "pchangeSP_15y","pchangeSP_20y","sector","industry","country","ttm_dividend","ttm_eps",
                            "price52whigh","price52wlow","in_price","original_in_price","rsi_14"
                        ],
                        per_page: 1000,
                        page: 1
                    })
                });

                const json = await res.json();
                return json;

            } catch (err) {
                return { error: err.toString() };
            }
        }""",
        url
    )

    if isinstance(result, dict) and "error" in result:
        logger.error("❌ Error fetching portfolio:", result["error"])
        return None

    stocks = (result.get("stocks") or result.get("data") or [])
    if not stocks:
        logger.info(f"No stocks found for portfolio {portfolio_id}")
        return None

    rows = []
    for s in stocks:
        stock = s.get("stock", s)
        rows.append({
            # Required core
            "Ticker": stock.get("display_symbol") or stock.get("symbol"),
            "Company": stock.get("company"),
            "Sector": stock.get("sector"),
            "RSI": stock.get("rsi_14"),
            # "RSI": np.nan,  # not provided; left blank as per spec
            "Price": stock.get("price"),
            "MTD": stock.get("pchange_mtd"),
            # Map periods
            "1D": stock.get("p_pct_change"),
            "1W": stock.get("pchange_1w"),
            # Choose MTD as 1M (change to pchange_4w if desired)
            "1M": stock.get("pchange_4w"),
            "3M": stock.get("pchange_12w"),
            "6M": stock.get("pchange_24w"),
            "YTD": stock.get("pchange_ytd"),
            "1Y": stock.get("pchange_52w"),
            "3Y": stock.get("pchange_3y"),
            "5Y": stock.get("pchange_5y"),
            "10Y": stock.get("pchange_10y"),
        })

    df = pd.DataFrame(rows)

    # --- Calculate Total Performance from Annualized Returns----------------------

    for col, years in [
        ("3Y", 3),
        ("5Y", 5),
        ("10Y", 10)
    ]:
        if col in df.columns:
            df[f"Total {years}Y Return %"] = ((1 + df[col] / 100) ** years - 1) * 100

    # ------------------------- Exclude companies with no performance (YTD=0 and 1Y=0) ---------
    if "YTD" in df.columns and "1Y" in df.columns:
        before = len(df)
        df = df[~((df["YTD"] == 0) & (df["1Y"] == 0))].copy()
        after = len(df)
        logger.info(f"Excluded {before - after} stocks with YTD=0 and 1Y=0.")



    # ---------- Conditional Ranking & Export ----------
    if ranking:
        df_analysis = _compute_analysis_from_raw(df)
        _export_analysis_with_charts(df_analysis, save_path)
        logger.info(f"✅ Final analysis Excel with ranking saved to {save_path.resolve()}")
    else:
        df.to_excel(save_path, index=False)
        logger.info(f"✅ Raw portfolio Excel (no ranking) saved to {save_path.resolve()}")

    return save_path


# -------------------------Ranking --- charts -- Combined Portfolio ----------------------------


REQUIRED_COL_ORDER = [
    "No", "Ticker", "Company", "Sector", "RSI", "Price", "MTD",
    "1D", "1W", "1M", "3M", "6M", "YTD", "1Y", "3Y", "5Y", "10Y",
    "Total 3Y Return %", "Total 5Y Return %", "Total 10Y Return %",
    "WR 1 1W 1M 3M 6M YTD", "WR 2 1M 3M 6M YTD", "WR 3 1W 1M 3M", "WR 4 3M 6M YTD 1Y","WR 5 TOTAL",
    "Rank 1W", "Rank 1M", "Rank 3M", "Rank 6M", "Rank YTD", "Rank 1Y",
    "Total Rank",
    "RWR1 1W 1M 3M 6M YTD", "RWR2 1M 3M 6M YTD", "RWR3 1W 1M 3M", "RWR4 3M 6M YTD 1Y",
    "Total Score", "R1 Score", "R2 Score", "R3 Score", "R4 Score", "All Ranks"
]


def _to_num(s):
    """Convert values to numeric, treating '-', '' and spaces as NaN."""
    if s is None:
        return np.nan
    if isinstance(s, str) and s.strip() in {"", "-"}:
        return np.nan
    return pd.to_numeric(s, errors="coerce")


def _mean_available(row, cols):
    vals = pd.to_numeric(row[cols], errors="coerce")
    vals = vals.dropna()
    return vals.mean() if len(vals) else np.nan


def _rank_series(s):
    # Higher is better -> rank 1 is highest value
    return s.rank(ascending=False, method="min")


def _read_analysis_sheet(path: Path) -> pd.DataFrame:
    """Read 'Analysis' if present, else first sheet, and normalize core columns."""

    xl = pd.ExcelFile(path)
    sheet = "Analysis" if "Analysis" in xl.sheet_names else xl.sheet_names[0]
    df = pd.read_excel(path, sheet_name=sheet ,header=1)
    core = ["Ticker", "Company", "Sector", "RSI", "Price", "MTD", "1D", "1W", "1M", "3M", "6M", "YTD", "1Y", "3Y", "5Y",
            "10Y", "Total 3Y Return %", "Total 5Y Return %", "Total 10Y Return %"]
    for c in core:
        if c not in df.columns:
            df[c] = np.nan
    return df[core]


def _compute_analysis_from_raw(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build the 'Analysis' table using the exact same WR + Rank + Score formulas
    used for individual portfolio exports.
    """
    df = df.copy()

    # "No" column
    df.insert(0, "No", range(1, len(df) + 1))

    # Coerce numeric cols
    num_cols = ["RSI", "Price", "1D", "1W", "1M", "3M", "6M", "YTD", "1Y", "3Y", "5Y", "10Y"]
    for c in num_cols:
        if c in df.columns:
            df[c] = df[c].apply(_to_num)

    # ---- WR calculations (mean of available values only) ----
    wr1_cols = ["1W", "1M", "3M", "6M", "YTD"]
    wr2_cols = ["1M", "3M", "6M", "YTD"]
    wr3_cols = ["1W", "1M", "3M"]
    wr4_cols = ["3M", "6M", "YTD", "1Y"]
    wr5_cols = ["1W", "1M", "3M", "6M", "YTD","1Y"]  #------------


    df["WR 1 1W 1M 3M 6M YTD"] = df.apply(lambda r: _mean_available(r, wr1_cols), axis=1)
    df["WR 2 1M 3M 6M YTD"] = df.apply(lambda r: _mean_available(r, wr2_cols), axis=1)
    df["WR 3 1W 1M 3M"] = df.apply(lambda r: _mean_available(r, wr3_cols), axis=1)
    df["WR 4 3M 6M YTD 1Y"] = df.apply(lambda r: _mean_available(r, wr4_cols), axis=1)
    df["WR 5 TOTAL"] = df.apply(lambda r: _mean_available(r, wr5_cols), axis=1)  #---------

    # ---- Ranks for each single period ----
    df["Rank 1W"] = _rank_series(df["1W"])
    df["Rank 1M"] = _rank_series(df["1M"])
    df["Rank 3M"] = _rank_series(df["3M"])
    df["Rank 6M"] = _rank_series(df["6M"])
    df["Rank YTD"] = _rank_series(df["YTD"])
    df["Rank 1Y"] = _rank_series(df["1Y"])

    # Total Rank (sum of period ranks)
    df["Total Rank"] = df[["Rank 1W", "Rank 1M", "Rank 3M", "Rank 6M", "Rank YTD", "Rank 1Y"]].sum(axis=1, skipna=True)

    # ---- Ranks for WRs ----
    df["RWR1 1W 1M 3M 6M YTD"] = _rank_series(df["WR 1 1W 1M 3M 6M YTD"])
    df["RWR2 1M 3M 6M YTD"] = _rank_series(df["WR 2 1M 3M 6M YTD"])
    df["RWR3 1W 1M 3M"] = _rank_series(df["WR 3 1W 1M 3M"])
    df["RWR4 3M 6M YTD 1Y"] = _rank_series(df["WR 4 3M 6M YTD 1Y"])

    # Total Score
    df["Total Score"] = (
            df["Rank 1W"] + df["Rank 1M"] + df["Rank 3M"] + df["Rank 6M"] + df["Rank YTD"] + df["Rank 1Y"]
            + df["RWR1 1W 1M 3M 6M YTD"] + df["RWR2 1M 3M 6M YTD"] + df["RWR3 1W 1M 3M"] + df["RWR4 3M 6M YTD 1Y"]
    )

    # R1..R4 Scores
    df["R1 Score"] = df["Rank 1W"] + df["Rank 1M"] + df["Rank 3M"] + df["Rank 6M"] + df["Rank YTD"] + df[
        "RWR1 1W 1M 3M 6M YTD"]
    df["R2 Score"] = df["Rank 1M"] + df["Rank 3M"] + df["Rank 6M"] + df["Rank YTD"] + df["RWR2 1M 3M 6M YTD"]
    df["R3 Score"] = df["Rank 1W"] + df["Rank 1M"] + df["Rank 3M"] + df["RWR3 1W 1M 3M"]
    df["R4 Score"] = df["Rank 3M"] + df["Rank 6M"] + df["Rank YTD"] + df["Rank 1Y"] + df["RWR4 3M 6M YTD 1Y"]

    # All Ranks
    df["All Ranks"] = (
            df["Rank 1W"] + df["Rank 1M"] + df["Rank 3M"] + df["Rank 6M"] + df["Rank YTD"] + df["Rank 1Y"]
            + df["RWR1 1W 1M 3M 6M YTD"] + df["RWR2 1M 3M 6M YTD"] + df["RWR3 1W 1M 3M"] + df["RWR4 3M 6M YTD 1Y"]
             + df["R1 Score"] + df["R2 Score"] + df["R3 Score"] + df["R4 Score"]
    )

    # Ensure all required columns exist & order them
    for col in REQUIRED_COL_ORDER:
        if col not in df.columns:
            df[col] = np.nan
    df = df[REQUIRED_COL_ORDER]

    # Round numeric to 4 decimals
    df = df.apply(lambda c: c.round(4) if pd.api.types.is_numeric_dtype(c) else c)
    return df


def _export_analysis_with_charts(df: pd.DataFrame, out_path: Path) -> Path:
    """Write Analysis + TOP/WORST sheets and charts (same as individual exports)."""
    df = df.copy()

    # Build TOP/WORST helper sets
    grp1_cols = ["1W", "1M", "YTD", "3M"]
    grp2_cols = ["1W", "1M", "YTD", "3M", "6M", "1Y"]

    def avg_available(frame, cols):
        return frame.apply(lambda r: _mean_available(r, cols), axis=1)

    df["_Group1Avg"] = avg_available(df, grp1_cols)
    df["_Group2Avg"] = avg_available(df, grp2_cols)

    n = min(25, len(df))
    top25_g1 = df.sort_values("_Group1Avg", ascending=False).head(n)
    worst25_g1 = df.sort_values("_Group1Avg", ascending=True).head(n)
    top25_g2 = df.sort_values("_Group2Avg", ascending=False).head(n)
    worst25_g2 = df.sort_values("_Group2Avg", ascending=True).head(n)

    df_main = df.drop(columns=["_Group1Avg", "_Group2Avg"])



    with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:

        # Set default font for the whole workbook
        writer.book.formats[0].set_font_name("Aptos Light")
        writer.book.formats[0].set_font_size(12)

        rank_cols = ["No", "Rank 1W", "Rank 1M", "Rank 3M", "Rank 6M", "Rank YTD", "Rank 1Y", "Total Rank"]
        performance_cols = [
            "MTD", "1D", "1W", "1M", "3M", "6M", "YTD", "1Y", "3Y", "5Y", "10Y",
            "Total 3Y Return %", "Total 5Y Return %", "Total 10Y Return %",
            "WR 1 1W 1M 3M 6M YTD", "WR 2 1M 3M 6M YTD", "WR 3 1W 1M 3M",
            "WR 4 3M 6M YTD 1Y", "WR 5 TOTAL",
        ]
        # Only divide by 100 for non-combined files
        if "Combined" not in str(out_path):
            for col in performance_cols:
                if col in df_main.columns:
                    df_main[col] = pd.to_numeric(df_main[col], errors="coerce") / 100


        # Replace 0 values with "-" before writing
        for c in ["3Y", "5Y", "10Y", "Total 3Y Return %", "Total 5Y Return %", "Total 10Y Return %"]:
            if c in df_main.columns:
                df_main[c] = df_main[c].apply(lambda x: "-" if pd.isna(x) or x == 0 else x)


        df_main.to_excel(writer, sheet_name="Analysis", index=False, startrow=3, startcol=1,   header=False)

        top25_g1_out = top25_g1[["Ticker", "Company", "_Group1Avg"]].rename(columns={"_Group1Avg": "Group1Avg"})
        worst25_g1_out = worst25_g1[["Ticker", "Company", "_Group1Avg"]].rename(columns={"_Group1Avg": "Group1Avg"})
        top25_g2_out = top25_g2[["Ticker", "Company", "_Group2Avg"]].rename(columns={"_Group2Avg": "Group2Avg"})
        worst25_g2_out = worst25_g2[["Ticker", "Company", "_Group2Avg"]].rename(columns={"_Group2Avg": "Group2Avg"})

        top25_g1_out.to_excel(writer, sheet_name="TOP25_G1", index=False)
        worst25_g1_out.to_excel(writer, sheet_name="WORST25_G1", index=False)
        top25_g2_out.to_excel(writer, sheet_name="TOP25_G2", index=False)
        worst25_g2_out.to_excel(writer, sheet_name="WORST25_G2", index=False)

        workbook = writer.book

        def add_bar_chart(sheet_name, title):
            ws = writer.sheets[sheet_name]
            nrows = ws.dim_rowmax + 1  # includes header
            chart = workbook.add_chart({'type': 'column'})

            # Put the category axis labels on top for WORST sheets
            if "WORST" in sheet_name.upper():
                chart.set_x_axis({
                    'name': 'Ticker',
                    'label_position': 'high',  # move category labels to top
                    'num_font': {'rotation': -45},  # keep them horizontal
                    'interval_unit': 1  # attempt to show every label
                })
            else:
                chart.set_x_axis({'name': 'Ticker', 'num_font': {'rotation': -45}, 'interval_unit': 1})

            chart.set_y_axis({'name': 'Average %'})

            chart.add_series({
                'name': title,
                'categories': f"='{sheet_name}'!$A$2:$A${nrows}",
                'values': f"='{sheet_name}'!$C$2:$C${nrows}",
            })

            chart.set_legend({'none': True})
            ws.insert_chart('E2', chart, {'x_scale': 1.4, 'y_scale': 1.2})

        add_bar_chart("TOP25_G1", "TOP 25 - Group 1 (1W,1M,YTD,3M)")
        add_bar_chart("WORST25_G1", "WORST 25 - Group 1 (1W,1M,YTD,3M)")
        add_bar_chart("TOP25_G2", "TOP 25 - Group 2 (1W,1M,YTD,3M,6M,1Y)")
        add_bar_chart("WORST25_G2", "WORST 25 - Group 2 (1W,1M,YTD,3M,6M,1Y)")

        #--------------------------FORMATTING---------------------------------------


        ws_analysis = writer.sheets["Analysis"]


        for ci, col in enumerate(df_main.columns):

            # Force integer format for rank columns
            if col in rank_cols:
                fmt = workbook.add_format({'num_format': '0','font_name': 'Aptos Light', 'font_size': 12})

            # Format performance columns as percentages with 2 decimals
            elif col in performance_cols:
                fmt = workbook.add_format({'num_format':  '0.00%;[Red]-0.00%','font_name': 'Aptos Light', 'font_size': 12})

            # Force float format for performance columns (optional)
            elif col in ["RSI", "Price"]:
                fmt = workbook.add_format({'num_format': '0.00','font_name': 'Aptos Light', 'font_size': 12})

            else:
                fmt = workbook.add_format({'font_name': 'Aptos Light', 'font_size': 12})

            if fmt is not None:
                ws_analysis.set_column(ci + 1, ci + 1, 12, fmt)

        ws_analysis.set_column(0, 0, 3)  # Reduce size of column A
        center_fmt = workbook.add_format(
            {'align': 'center', 'valign': 'vcenter', 'font_name': 'Aptos Light', 'font_size': 12})
        ws_analysis.set_column(1, 1, 6,center_fmt)  # Reduce size of column No
        ws_analysis.set_column(3, 3, 28)  # Increase size of column company
        ws_analysis.set_column(4, 4, 20)  # Increase size of column sector

        # ✅ Header format (Row 2)
        fmt_header = workbook.add_format({
            'bold': True,
            'align': 'center',
            'valign': 'vcenter',
            'text_wrap': True,
            'bg_color': '#E6E0EC',  # light purple
            'border': 1,
            'font_name': 'Aptos Light',
            'font_size': 12
        })

        # ✅ Wrapped header text map (for line breaks)
        header_wrap_map = {
            # "Total 3Y Return %": "Total\n3Y Return %",
            # "Total 5Y Return %": "Total\n5Y Return %",
            # "Total 10Y Return %": "Total\n10Y Return %",
            "WR 1 1W 1M 3M 6M YTD": "WR1\n1W 1M 3M 6M YTD",
            "WR 2 1M 3M 6M YTD": "WR2\n1M 3M 6M YTD",
            "WR 3 1W 1M 3M": "WR3\n1W 1M 3M",
            "WR 4 3M 6M YTD 1Y": "WR4\n3M 6M YTD 1Y",
            "WR 5 TOTAL": "WR5\nTOTAL",
            "Rank 1W": "Rank\n1W",
            "Rank 1M": "Rank\n1M",
            "Rank 3M": "Rank\n3M",
            "Rank 6M": "Rank\n6M",
            "Rank YTD": "Rank\nYTD",
            "Rank 1Y": "Rank\n1Y",
            "Total Rank": "Total\nRank",
            "RWR1 1W 1M 3M 6M YTD": "RWR1\n1W 1M\n3M 6M\nYTD",
            "RWR2 1M 3M 6M YTD": "RWR2\n1M 3M\n6M YTD",
            "RWR3 1W 1M 3M": "RWR3\n1W 1M\n3M",
            "RWR4 3M 6M YTD 1Y": "RWR4\n3M 6M\nYTD 1Y",
            "Total Score":"Total\nScore",
            "R1 Score": "R1\nScore",
            "R2 Score": "R2\nScore",
            "R3 Score": "R3\nScore",
            "R4 Score": "R4\nScore",
            "All Ranks": "All\nRanks",
        }

        # ✅ Write wrapped headers to row 2 (Excel row index 1)
        for ci, col in enumerate(df_main.columns):
            header_text = header_wrap_map.get(col, col)
            ws_analysis.write(1, ci + 1, header_text, fmt_header)

        # ✅ Add autofilter (Row 3 in Excel, i.e. index 2)
        nrows, ncols = df_main.shape
        ws_analysis.autofilter(2, 2, nrows + 2, ncols )

        # ✅ Set row heights
        ws_analysis.set_row(0, 20)  # Row 1 empty small
        ws_analysis.set_row(1, 62)  # Header row
        ws_analysis.set_row(2, 20)  # Filter row

        ws_analysis.freeze_panes(3, 3)  #freeze_panes

    return out_path


def combine_and_rank_portfolios_reuse(main_path: Path,
                                      repeated_path: Path,
                                      single_path: Path,
                                      out_path: Path,
                                      ranking: bool = False) -> Path:
    """
    Combine three portfolio workbooks and recompute analysis using the exact
    same WR + ranking logic. Duplicate tickers are resolved by priority:
    Main > Repeated > Single (change concat order to tweak priority).
    """
    # Read core columns from each file
    df_main = _read_analysis_sheet(main_path)
    df_repeated = _read_analysis_sheet(repeated_path)
    df_single = _read_analysis_sheet(single_path)


    # Priority order: Main first (keeps first occurrence)
    combined = pd.concat([df_main, df_repeated, df_single], ignore_index=True)
    combined = combined.dropna(subset=["Ticker"])
    combined["Ticker"] = combined["Ticker"].astype(str).str.upper().str.strip()
    combined = combined.drop_duplicates(subset=["Ticker"], keep="first").reset_index(drop=True)

    # ---------- Conditional Ranking & Export ----------
    if ranking:
        df_analysis = _compute_analysis_from_raw(combined)
        _export_analysis_with_charts(df_analysis, out_path)
        logger.info(f"✅ Final analysis Excel with ranking saved to {out_path.resolve()}")

    else:
        combined.to_excel(out_path, index=False)
        logger.info(f"✅ Raw portfolio Excel (no ranking) saved to {out_path.resolve()}")

    return out_path


# -------------------------------MOMENTUM RANKING ----------------------------

PARAMS = {
    "SpecVersion": "v2.8_FULL_RevAB",
    "WinsorPct": 0.01,        # cross-sectional 1%/99%
    "LambdaMin": 0.30,        # λ_min
    "Alpha": 0.25,            # Score_total = Score_z + Alpha*Score_cat
    # Category cuts (decimals). Grades map: {-2,-1,0,1,2,3} = Awful..Fantastic
    "cuts_1M":  [-0.01, 0.00, 0.01, 0.02, 0.10],
    "cuts_3M":  [ 0.015,0.03, 0.06, 0.12, 0.20],
    "cuts_6M":  [ 0.03, 0.06, 0.12, 0.18, 0.30],
    "cuts_12M": [ 0.06, 0.12, 0.24, 0.36, 0.55],
    "grade_points": [-2, -1, 0, 1, 2, 3],
    # Rank Guard & Trend multipliers (driven by categorical grades of 1M and 3M)
    "RG_by_Grade1M": { -2: 0.60, -1: 0.90, 0: 1.00, 1: 1.00, 2: 1.00, 3: 1.00 },
    "TrendMult_rules": { "both_below_neutral": 0.85, "mixed": 0.93, "both_ge_good": 1.00 },
    # Weights (w12, w6, w3, w1)
    "Weights_base":   (0.40, 0.30, 0.20, 0.10),
    "Weights_medium": (0.36, 0.26, 0.24, 0.14),  # if (1M>=5% & 3M>=12%)
    "Weights_strong": (0.33, 0.24, 0.26, 0.17),  # if (1M>=10% & 3M>=20%)
    # Output options
    "ExportTopWorst": True,
    "Charts": True,  # requires matplotlib; set False on servers without display
}

def pct_to_dec_series(s):
    """Auto-detect % vs decimal. If median(|x|)>0.5 assume % and divide by 100."""
    s = pd.to_numeric(s, errors="coerce")
    med_abs = np.nanmedian(np.abs(s))
    if med_abs is not np.nan and med_abs > 0.5:
        return s / 100.0
    return s

def winsorize(s, lim=0.01):
    s = pd.Series(s)
    if s.dropna().empty: return s
    lo, hi = s.quantile(lim), s.quantile(1 - lim)
    return s.clip(lower=lo, upper=hi)

def zscore(s, winsor_pct=0.01):
    s = pd.Series(s)
    sw = winsorize(s, winsor_pct)
    mu = sw.mean()
    sd = sw.std(ddof=1)
    if pd.isna(sd) or sd == 0:
        # revB: Zero variance -> set z=0 for all rows and log
        return pd.Series(0.0, index=s.index)
    return (s - mu) / sd

def lambda_from_r1m(r1m, lam_min=0.30):
    if pd.isna(r1m): return np.nan
    return min(1.0, max(lam_min, 1.0 + float(r1m)))

def safe_log1p(x):
    try: return math.log1p(x)
    except Exception: return np.nan

def gated_mom(rk, r1, lam):
    if any(pd.isna(x) for x in (rk, r1, lam)): return np.nan
    Lk, L1 = safe_log1p(rk), safe_log1p(r1)
    if pd.isna(Lk) or pd.isna(L1): return np.nan
    return math.exp(Lk - (1 - lam) * L1) - 1.0

def grade_from_cuts(x, cuts, points=(-2,-1,0,1,2,3)):
    if pd.isna(x): return np.nan
    # bins: (-inf, c0], (c0,c1], ... (c4, +inf)
    if x <  cuts[0]: return points[0]
    if x <  cuts[1]: return points[1]
    if x <  cuts[2]: return points[2]
    if x <  cuts[3]: return points[3]
    if x <  cuts[4]: return points[4]
    return points[5]

def trend_multiplier(g3, g1, rules):
    if (g3 < 0) and (g1 < 0): return rules["both_below_neutral"]
    if (g3 >= 1) and (g1 >= 1): return rules["both_ge_good"]
    return rules["mixed"]

def choose_weights(r1, r3, base, med, strong):
    if (not pd.isna(r1)) and (not pd.isna(r3)):
        if (r1 >= 0.10) and (r3 >= 0.20): return strong, "STRONG"
        if (r1 >= 0.05) and (r3 >= 0.12): return med,    "MEDIUM"
    return base, "NONE"

def run_momentum_ranking(in_xls, out_xls, sheet="Analysis"):
    """ Run a cross-sectional momentum ranking on a dataset and save results to Excel."""
    df = pd.read_excel(in_xls, sheet_name=sheet,header=1)
    cols = {c.lower(): c for c in df.columns}

    def pick(*names):
        for n in names:
            if n.lower() in cols: return cols[n.lower()]
        return None

    col_tkr = pick("Ticker", "SYMBOL", "Symbol")
    c1 = pick("1M", "1 m", "1mo", "1 mo", "oneM")
    c3 = pick("3M", "3 m", "3mo", "3 mo", "threeM")
    c6 = pick("6M", "6 m", "6mo", "6 mo", "sixM")
    c12 = pick("12M", "12 m", "1Y", "1 y", "1yr", "twelveM")

    if not all([col_tkr, c1, c3, c6, c12]):
        raise ValueError("Need columns: Ticker, 1M, 3M, 6M, 12M (or 1Y).")

    tickers = df[col_tkr].astype(str).str.upper()
    R1M = pct_to_dec_series(df[c1])
    R3M = pct_to_dec_series(df[c3])
    R6M = pct_to_dec_series(df[c6])
    R12M = pct_to_dec_series(df[c12])
    idx = pd.Index(tickers, name="Ticker")
    R1M, R3M, R6M, R12M = [pd.Series(s.values, index=idx) for s in (R1M, R3M, R6M, R12M)]

    # ---------- Base metrics (v2.8) ----------
    LAM = R1M.apply(lambda x: lambda_from_r1m(x, PARAMS["LambdaMin"]))
    MOM6L = pd.Series([gated_mom(R6M[t], R1M[t], LAM[t]) for t in idx], index=idx)
    MOM12L = pd.Series([gated_mom(R12M[t], R1M[t], LAM[t]) for t in idx], index=idx)

    Z12 = zscore(MOM12L, PARAMS["WinsorPct"])
    Z6 = zscore(MOM6L, PARAMS["WinsorPct"])
    Z3 = zscore(R3M, PARAMS["WinsorPct"])
    Z1 = zscore(R1M, PARAMS["WinsorPct"])

    # ---------- Categorical grades ----------
    g1 = R1M.apply(lambda x: grade_from_cuts(x, PARAMS["cuts_1M"], PARAMS["grade_points"]))
    g3 = R3M.apply(lambda x: grade_from_cuts(x, PARAMS["cuts_3M"], PARAMS["grade_points"]))
    g6 = R6M.apply(lambda x: grade_from_cuts(x, PARAMS["cuts_6M"], PARAMS["grade_points"]))
    g12 = R12M.apply(lambda x: grade_from_cuts(x, PARAMS["cuts_12M"], PARAMS["grade_points"]))

    # Rank Guard and Trend
    RG = g1.map(PARAMS["RG_by_Grade1M"])
    TrendMult = pd.Series([trend_multiplier(g3[t], g1[t], PARAMS["TrendMult_rules"]) for t in idx], index=idx)

    # Apply RG only to 12M & 6M z; then global TrendMult
    Z12_t = TrendMult * (RG * Z12)
    Z6_t = TrendMult * (RG * Z6)
    Z3_t = TrendMult * Z3
    Z1_t = TrendMult * Z1

    # Boosted weights (two levels)
    weights = pd.DataFrame(index=idx, columns=["W12", "W6", "W3", "W1"])
    boost_flag = pd.Series(index=idx, dtype=object)
    for t in idx:
        w, flag = choose_weights(R1M[t], R3M[t],
                                 PARAMS["Weights_base"],
                                 PARAMS["Weights_medium"],
                                 PARAMS["Weights_strong"])
        weights.loc[t, ["W12", "W6", "W3", "W1"]] = w
        boost_flag[t] = flag

    Score_z = (weights["W12"] * Z12_t +
               weights["W6"] * Z6_t +
               weights["W3"] * Z3_t +
               weights["W1"] * Z1_t)

    Score_cat = 0.40 * g12 + 0.30 * g6 + 0.20 * g3 + 0.10 * g1
    Score_total = Score_z + PARAMS["Alpha"] * Score_cat

    tbl = pd.DataFrame({
        "R1M": R1M, "R3M": R3M, "R6M": R6M, "R12M": R12M,
        "Lambda": LAM, "MOM6λ": MOM6L, "MOM12λ": MOM12L,
        "Z1": Z1, "Z3": Z3, "Z6": Z6, "Z12": Z12,
        "Grade_1M": g1, "Grade_3M": g3, "Grade_6M": g6, "Grade_12M": g12,
        "RG": RG, "TrendMult": TrendMult,
        "W12": weights["W12"], "W6": weights["W6"], "W3": weights["W3"], "W1": weights["W1"],
        "BoostFlag": boost_flag,
        "Score_z": Score_z, "Score_cat": Score_cat, "Score_total": Score_total
    })

    # Exclusions: any missing required windows -> drop from ranking
    required = ["R1M", "R3M", "R6M", "R12M"]
    mask_ok = tbl[required].notna().all(axis=1)
    exclusions = (~mask_ok).sum()

    rankable = tbl[mask_ok].copy()
    rankable["Ticker_sortkey"] = rankable.index.str.casefold()
    rankable.sort_values(
        by=["Score_total", "R1M", "R3M", "Ticker_sortkey"],
        ascending=[False, False, False, True],
        inplace=True
    )
    rankable["Rank"] = np.arange(1, len(rankable) + 1)
    rankable.drop(columns=["Ticker_sortkey"], inplace=True)

    with pd.ExcelWriter(out_xls, engine="xlsxwriter") as xw:

        workbook = xw.book

        # Set default font for the entire workbook
        workbook.formats[0].set_font_name("Aptos Light")
        workbook.formats[0].set_font_size(12)


        # 1️⃣ Remove any existing "No" column and add it freshly before Ticker
        df_out = rankable.reset_index()
        df_out.insert(0, "No", range(1, len(df_out) + 1))
        df_out.to_excel(xw, sheet_name="FullExport", index=False, startrow=3, startcol=1,header=False)

        top25 = rankable.head(25).copy()
        bot25 = rankable.sort_values("Score_total", ascending=True).head(25).copy()
        top25_out = top25.reset_index()[
            ["Rank", "Ticker", "Score_total", "Score_z", "Score_cat", "R1M", "R3M", "R6M", "R12M", "RG", "TrendMult",
             "BoostFlag"]]
        bot25_out = bot25.reset_index()[
            ["Rank", "Ticker", "Score_total", "Score_z", "Score_cat", "R1M", "R3M", "R6M", "R12M", "RG", "TrendMult",
             "BoostFlag"]]


        top25_out.to_excel(excel_writer=xw, sheet_name="Top25", index=False)
        bot25_out.to_excel(excel_writer=xw, sheet_name="Worst25", index=False)

        # ---------------------- Add Bar Charts for Top25 and Worst25 ------------------------

        def add_bar_chart(sheet_name, title):
            """Add a bar chart plotting Score_total vs. Ticker."""
            ws = xw.sheets[sheet_name]
            nrows = len(rankable) if sheet_name == "FullExport" else 26

            chart = workbook.add_chart({'type': 'column'})

            # Configure chart axes
            if "WORST" in sheet_name.upper():
                chart.set_x_axis({
                    'name': 'Ticker',
                    'label_position': 'high',  # show labels on top
                    'num_font': {'rotation': -45},
                    'interval_unit': 1
                })
            else:
                chart.set_x_axis({'name': 'Ticker', 'num_font': {'rotation': -45}, 'interval_unit': 1})

            chart.set_y_axis({'name': 'Score_total', 'major_gridlines': {'visible': False}})

            # Data references
            chart.add_series({
                'name': title,
                'categories': f"='{sheet_name}'!$B$2:$B${nrows}",  # Ticker column
                'values': f"='{sheet_name}'!$C$2:$C${nrows}",      # Score_total column
                'data_labels': {'value': False}
            })

            chart.set_legend({'none': True})
            chart.set_title({'name': title})
            ws.insert_chart('N2', chart, {'x_scale': 1.4, 'y_scale': 1.2})

        # Add charts for both Top25 and Worst25
        add_bar_chart("Top25", "Top 25 Momentum Scores")
        add_bar_chart("Worst25", "Worst 25 Momentum Scores")


        # Workbook
        params_df = pd.DataFrame.from_dict(PARAMS, orient="index", columns=["Value"])
        diag = pd.DataFrame([
            ["Rows_total", len(tbl)],
            ["Rows_rankable", len(rankable)],
            ["Exclusions_missing_windows", int(exclusions)],
            ["WinsorPct", PARAMS["WinsorPct"]],
            ["LambdaMin", PARAMS["LambdaMin"]],
            ["Alpha", PARAMS["Alpha"]],
            ["Weights_base", PARAMS["Weights_base"]],
            ["Weights_medium", PARAMS["Weights_medium"]],
            ["Weights_strong", PARAMS["Weights_strong"]],
        ], columns=["Key", "Value"])

        rankable.reset_index().to_excel(xw, sheet_name="Metrics", index=False)
        diag.to_excel(xw, sheet_name="Diagnostics", index=False)
        params_df.to_excel(xw, sheet_name="Parameters")

        # ====================== FORMATTING RULES ======================

        ws = xw.sheets["FullExport"]

        # ---------- Define formats ----------
        pct_fmt = workbook.add_format({'num_format': '0.00%', 'font_color': 'black','font_name': 'Aptos Light', 'font_size': 12})
        pct_fmt_red = workbook.add_format({'num_format': '0.00%', 'font_color': 'red','font_name': 'Aptos Light', 'font_size': 12})

        num_fmt = workbook.add_format({'num_format': '0.00', 'font_color': 'black','font_name': 'Aptos Light', 'font_size': 12})
        num_fmt_red = workbook.add_format({'num_format': '0.00', 'font_color': 'red','font_name': 'Aptos Light', 'font_size': 12})

        header_fmt = workbook.add_format({
            'bold': True,
            'align': 'center',
            'valign': 'vcenter',
            'text_wrap': True,
            'bg_color': '#cbebf5',  # light blue
            'border': 1,
            'font_name': 'Aptos Light',
            'font_size': 11
        })

        # ---------- Apply header highlight (No → Rank columns) ----------
        # Find columns dynamically
        header_row = 1
        ncols = len(df_out.columns)
        headers = df_out.columns.tolist()
        start_col = 1 +headers.index("No")
        end_col = 1 +headers.index("Rank")
        ws.set_row(header_row, None, None)  # ensure default height

        header_wrap_map = {
            "Grade_1M": "Grade\n1M",
            "Grade_3M": "Grade\n3M",
            "Grade_6M": "Grade\n6M",
            "Grade_12M": "Grade\n12M",
            "TrendMult": "Trend\nMult",
            "BoostFlag": "Boost\nFlag",
            "Score_z": "Score\nz",
            "Score_cat": "Score\ncat",
            "Score_total": "Score\ntotal",
        }

        # Write headers with wrapping on row 3 (Excel row index 2)
        for ci, col in enumerate(headers):
            header_text = header_wrap_map.get(col,col)
            ws.write(header_row, ci + 1, header_text, header_fmt)

        # for col in range(start_col, end_col + 1):
        #     ws.write(header_row, col, headers[col - 1], header_fmt)

        # ---------- Add AutoFilter ----------
        nrows, ncols = df_out.shape
        ws.autofilter(2, 2, nrows +2, ncols)

        # ✅ Set row heights
        ws.set_row(0, 15)  # Row 1 empty small
        ws.set_row(1, 30)  # Header row
        ws.set_row(2, 15)  # Filter row

        ws.freeze_panes(3, 3) # freeze panes

        ws.set_column("O:X", 6)  # Grade 1M to Grade 12M (columns O to R)
        ws.set_column("Z:Z", 6)  # Score_z
        ws.set_column("AA:AA", 6)  # Score_cat
        ws.set_column("AB:AB", 8)  # Score_total
        ws.set_column("AC:AC", 5)  # Rank
        ws.set_column(0, 0, 3)  # Reduce size of column A
        center_fmt = workbook.add_format({'align': 'center', 'valign': 'vcenter','font_name': 'Aptos Light', 'font_size': 12})
        ws.set_column(1, 1, 6,center_fmt)  # Reduce size of column No


        # ---------- Conditional formatting ----------
        def apply_color_conditional(ws, col_letter):
            # red if negative
            ws.conditional_format(f"{col_letter}3:{col_letter}{len(df_out) + 2}", {
                'type': 'cell',
                'criteria': '<',
                'value': 0,
                'format': pct_fmt_red
            })

        # Columns
        cols_pct = ["R1M", "R3M", "R6M", "R12M", "MOM6λ", "MOM12λ"]
        cols_num = ["Z1", "Z3", "Z6", "Z12", "Score_z", "Score_total"]

        for col_name in cols_pct:
            if col_name in headers:
                col_idx =1 + headers.index(col_name)
                col_letter = xl_col_to_name(col_idx)
                ws.set_column(col_idx, col_idx, 10, pct_fmt)
                apply_color_conditional(ws, col_letter)

        for col_name in cols_num:
            if col_name in headers:
                col_idx = 1 +headers.index(col_name)
                col_letter = xl_col_to_name(col_idx)
                ws.set_column(col_idx, col_idx, 10, num_fmt)
                ws.conditional_format(f"{col_letter}3:{col_letter}{len(df_out) + 2}", {
                    'type': 'cell',
                    'criteria': '<',
                    'value': 0,
                    'format': num_fmt_red
                })

        logger.info(f"✅ Momentum Ranking Done. Wrote: {out_xls}")




# ----------------------------1: Create portfolios--------------------------------------------

async def create_portfolios():
    """
    Automates the creation of GuruFocus portfolios from top holdings.
    1. Launches a Chromium browser via Playwright.
    2. Logs in and navigates to the Top Holdings page.
    3. Downloads the dynamic Excel file of top holdings.
    4. Cleans and splits tickers into two lists.
    5. Filters ADR tickers and matches them to each list.
    6. Renames/merges tickers and splits them by frequency.
    7. Creates two portfolios via the Guru API:
       - "Repeated Ticker"
       - "Single Ticker"
    8. Returns the portfolio IDs along with a main reference ID.
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()

        open("Opration_logging.log", "w").close()

        # Log in and go to Top Holdings
        page = await login_and_go_to_top_holdings(context)

        # Click the dropdown button
        await page.click("button.el-dropdown-selfdefine")
        await page.wait_for_timeout(500)

        # Wait for the dynamic Excel link to appear
        excel_button = page.locator("text='Download to Excel(.xls)'")
        await excel_button.wait_for(state="visible", timeout=5000)

        # Trigger download
        async with page.expect_download() as download_info:
            await excel_button.click(force=True)

        download = await download_info.value
        save_path = DOWNLOADS_DIR / "guru_top_holdings.xls"
        await download.save_as(str(save_path))
        logger.info(f"✅ Saved: {save_path.resolve()}")

        # Clean & process tickers
        cleaned_path = delete(save_path)
        list1_path, list2_path = split_tickers_flat(cleaned_path)

        adr_file = "downloads/tradingview_symbols.csv"
        filtered_adr_file = str(filter_adr_file(adr_file))

        list1_freq = count_frequency(list1_path)
        list2_freq = count_frequency(list2_path)

        list1_matched = await match_tickers_without_colon(list1_path, filtered_adr_file, freq_file=list1_freq)
        other_file_path = "downloads/tickers with colon.xlsx"
        list2_matched = await match_tickers_with_colon(list2_path, other_file_path, freq_file=list2_freq)

        renamed_path_1 = merge_or_rename(list1_matched)
        renamed_path_2 = merge_or_rename(list2_matched)

        final_file = merge_final_lists(renamed_path_1, renamed_path_2)
        repeated_csv, single_csv = split_tickers_by_count_csv(final_file)

        # ✅ Create portfolios
        Repeated_portfolio = await create_guru_portfolio_api(page, repeated_csv, portfolio_name="Repeated Ticker")
        Single_portfolio = await create_guru_portfolio_api(page, single_csv, portfolio_name="Single Ticker")

        repeated_id = Repeated_portfolio.get("id")
        single_id = Single_portfolio.get("id")
        main_id = 296771

        logger.info(f"✅ Portfolio IDs created: {repeated_id}, {single_id}, {main_id}")

        return repeated_id, single_id, main_id


#---------------------------- 2: Download portfolios------------------------------------------

async def download_portfolios(repeated_id, single_id, main_id):
    """
     Downloads GuruFocus portfolios, combines them, and runs momentum ranking.
    1. Launches a Chromium browser via Playwright and logs in.
    2. Downloads the three portfolios individually from GuruFocus.
    3. Combines all three portfolios into a single workbook with ranking.
    4. Runs the momentum ranking algorithm on each individual portfolio and the combined workbook.
    5. Saves output files to the FINAL_FILE_DIR with descriptive names.
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await login_and_go_to_top_holdings(context)

        # Download portfolios
        repeated_path=await download_gurufocus_portfolio_api(page, repeated_id, "Repeated_Ticker_guru_portfolio.xlsx",ranking=True)
        single_path=await download_gurufocus_portfolio_api(page, single_id, "Single_Ticker_guru_portfolio.xlsx",ranking=True)
        main_path=await download_gurufocus_portfolio_api(page, main_id, "Main_guru_portfolio.xlsx",ranking=True)

        # Combine all three into a single workbook
        combine_path=combine_and_rank_portfolios_reuse(
            main_path=FINAL_FILE_DIR / "Main_guru_portfolio.xlsx",
            repeated_path=FINAL_FILE_DIR / "Repeated_Ticker_guru_portfolio.xlsx",
            single_path=FINAL_FILE_DIR / "Single_Ticker_guru_portfolio.xlsx",
            out_path=FINAL_FILE_DIR / "Combined_guru_portfolio.xlsx",
            ranking = True
        )
        logger.info(f"✅ Combined portfolio saved → {(FINAL_FILE_DIR / 'Combined_guru_portfolio.xlsx').resolve()}")

        return repeated_path, single_path, main_path,combine_path

#---------------------------- 3: Momentum Ranking------------------------------------------
def run_all_momentum_rankings(repeated_path, single_path, main_path, combined_path):
    """Runs momentum ranking for each portfolio and saves the results."""

    # Dictionary to map input files to output names
    portfolio_mapping = {
        repeated_path: "Repeated_Portfolio_Momentum_Ranking.xlsx",
        single_path: "Single_Portfolio_Momentum_Ranking.xlsx",
        main_path: "Main_Portfolio_Momentum_Ranking.xlsx",
        combined_path: "Combined_Portfolio_Momentum_Ranking.xlsx",
    }

    # Run momentum ranking for all portfolios
    for in_xls, out_name in portfolio_mapping.items():
        run_momentum_ranking(
            in_xls=in_xls,
            out_xls=FINAL_FILE_DIR / out_name,
            sheet="Analysis"
        )

#------------------------------------ RUN BOTH FUNCTION-----------------------------------

async def main():
    repeated_id, single_id, main_id = await create_portfolios()
    repeated_path, single_path, main_path, combined_path = await download_portfolios(repeated_id, single_id, main_id)
    run_all_momentum_rankings(repeated_path, single_path, main_path, combined_path)


#-------------------------------------------------------------------------------------------
async def test_main():
    repeated_path, single_path, main_path, combined_path =await download_portfolios(repeated_id=300594, single_id=300595, main_id=296771)
    #
    # # repeated_path = "Final_File/Repeated_Ticker_guru_portfolio.xlsx"
    # # single_path = "Final_File/Single_Ticker_guru_portfolio.xlsx"
    # # main_path = "Final_File/Main_guru_portfolio.xlsx"
    # # combined_path = "Final_File/Combined_guru_portfolio.xlsx"
    #
    # run_all_momentum_rankings(repeated_path, single_path, main_path, combined_path)

if __name__ == "__main__":
    # asyncio.run(main())
    asyncio.run(test_main())