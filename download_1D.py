import os
from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path
from time import time
from zipfile import ZipFile

import requests

BASES = [
    "1INCH",
    "AAVE",
    "ADA",
    "BAT",
    "BLZ",
    "BUSD",
    # "C98",
    # "DAI",
    "ENJ",
    "KNC",
    "LINK",
    "LRC",
    "LTC",
    "MANA",
    "MATIC",
    "OGN",
    "OMG",
    # "SNT",
    "SNX",
    "USDC",
    "USDT",
    "BTC",  # WBTC
    "XMR",
    "ZRX",
]

QUOTES = ["ETH", "USDT"]

FOLDER = "data/"
BASE_URL = "https://data.binance.vision/data/spot/"
DAILY = f"{BASE_URL}daily/klines/"


def download_zip_file(url, filename=None, extract=False):
    """Download and extract zip file"""
    local_filename = filename if filename else url.split("/")[-1]
    if extract:
        local_path = Path(filename).parent if filename else None
        try:
            rq = requests.get(url)
            rq.raise_for_status()
            with ZipFile(BytesIO(rq.content)) as zf:
                zf.extractall(path=local_path)
        except requests.exceptions.RequestException as ex:
            print(ex)
            return True
        except Exception as ex:
            print(ex)
            pass
    else:
        with requests.get(url, stream=True) as rq:
            rq.raise_for_status()
            with open(local_filename, "wb") as fh:
                for chunk in rq.iter_content(chunk_size=8192):
                    # If you have chunk encoded response uncomment if
                    # and set chunk_size parameter to None.
                    # if chunk:
                    fh.write(chunk)
    return False


def csv_zip_exists(filename) -> bool:
    """Check if csv or zip file exists."""
    is_csv = Path(filename + ".csv").exists()
    is_zip = Path(filename + ".zip").exists()
    return is_csv or is_zip


def binance_1d_hist_daily(
    symbols, from_d, to_d, extract=False, skipexisting=True, verbose=False
):
    """Download Binance 1D daily data."""
    missing = []
    for symbol in symbols:
        if verbose:
            print(f"Getting data for {symbol}")
        for i in range((to_d - from_d).days):
            day = from_d + timedelta(days=i)
            if verbose:
                print(f"Downloading day: {day}")
            mo = str(day.month).zfill(2)
            dy = str(day.day).zfill(2)
            file = f"{symbol}-1d-{day.year}-{mo}-{dy}"
            pathname = FOLDER + f"{day.year}/daily/{symbol}/{file}"
            Path(pathname).parent.mkdir(parents=True, exist_ok=True)
            url = DAILY + f"{symbol}/1d/{file}.zip"
            if csv_zip_exists(pathname) and skipexisting:
                continue
            not_found = download_zip_file(url, pathname + ".zip", extract)
            if not_found:
                missing.append(Path(pathname).name.split("-")[0])
    return list(set(missing))


def main():
    """Run the programm."""
    token_usdt = [i + QUOTES[1] for i in BASES if i not in QUOTES]

    start = time()
    # -- DAILY --
    start_d = date(2021, 6, 1)
    today = datetime.utcnow().date()
    print(f"Getting data for {today - start_d} days:")
    missing = binance_1d_hist_daily(
        token_usdt, start_d, today, extract=True, verbose=False
    )
    print(missing)

    end = time()
    print(f"\nTotal time taken: {end - start}sec")

    # Clean up
    try:
        for p in Path(FOLDER).glob("**/*"):
            if p.is_dir() and len(list(p.iterdir())) == 0:
                os.removedirs(p)
    except Exception as ex:
        print("Clean up could not be completed: ", ex)


if __name__ == "__main__":
    main()
