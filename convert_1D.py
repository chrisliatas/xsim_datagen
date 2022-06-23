from functools import partial
from pathlib import Path

import pandas as pd

YEAR = "2022"
data_dir = f"data/{YEAR}/daily/"

col_names = [
    "Open time",
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
    "Close time",
    "Quote asset volume",
    "Number of trades",
    "Taker buy base asset volume",
    "Taker buy quote asset volume",
    # "Ignore",
]

BASES = [
    "1INCH",
    "AAVE",
    "BAT",
    "BLZ",
    "BUSD",
    # "C98",
    # "DAI",
    "ENJ",
    "KNC",
    "LINK",
    "LRC",
    "MANA",
    "MATIC",
    "OGN",
    "OMG",
    # "SNT",
    "SNX",
    "USDC",
    "USDT",
    "BTC",  # WBTC
    "ZRX",
]

QUOTES = ["ETH", "USDT"]

token_usdt = [i + QUOTES[1] for i in BASES if i not in QUOTES]


def write_merged_pairs(symbols, col_names, origin_path, to_pathname):
    pair_d = [sorted(list(Path(origin_path + p).glob("*.csv"))) for p in token_usdt]
    all_pair = [list(_) for _ in zip(*pair_d)]
    pairs_names = [i.name.split("-")[0] for i in all_pair[0]]
    for csv_list in all_pair:
        outfile = csv_list[0].name.rstrip(".csv")[-10:].replace("-", "")
        print(outfile)
        conc_pairs = pd.concat(
            map(
                partial(
                    pd.read_csv,
                    names=col_names,
                    index_col=col_names[0],
                    usecols=list(range(11)),
                    parse_dates=[0, 6],
                    date_parser=partial(pd.to_datetime, unit="ms"),
                ),
                csv_list,
            )
        )
        conc_pairs["Pair"] = pairs_names
        if Path(to_pathname + outfile).exists():
            continue
        conc_pairs.to_csv(to_pathname + outfile, sep="|")


years = [2021, 2022]
outdir = "data/XSim/"

for year in years:
    data_dir = f"data/{year}/daily/"
    print(f"Merging days for {year}:")
    write_merged_pairs(token_usdt, col_names, data_dir, outdir)
