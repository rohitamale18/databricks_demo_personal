import pandas as pd
import os
import glob
from typing import List, Tuple

def get_all_files(path: str, extension: str) -> List:
    all_files = glob.glob(os.path.join(path, extension))

    return all_files

def read_file(file: str) -> (pd.DataFrame, str):
    df = pd.read_csv(file)
    df['ticker'] = ticker
        
    return (df, ticker)

def save_file(path: str, df: pd.DataFrame, file_name: str = None) -> bool:
    
    path = path + "/" + file_name + ".csv"
    try:
        print(f"Saving data for file: {file_name}")
        df.to_csv(
            path,
            sep=",",
            index=False
        )
        print(f"Data for file: {file_name} saved at {path}")
        return True
    except Exception as e:
        return False
    
def extract_data(all_files: List) -> pd.DataFrame:

    dfs = []
    for file in all_files:
        df = pd.read_csv(file)
        dfs.append(df)

    output = pd.concat(dfs, ignore_index=True)

    return output

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    output = df.groupby("ticker").aggregate(
        {
            "Open": 'mean'
        }
    ).reset_index()
    return output

    
if __name__ == "__main__":
    BRONZE_PATH = "/Users/rohitamale/Documents/Coding/repo/sample_data/stocks/Stocks"
    SILVER_PATH = "/Users/rohitamale/Documents/Coding/repo/sample_data/stocks/bronze"
    GOLD_PATH = "/Users/rohitamale/Documents/Coding/repo/sample_data/stocks/gold"

    # Read raw data
    raw_files = get_all_files(BRONZE_PATH, ".csv")
    for file in raw_files:
        ticker = file.split("/")[-1].split(".")[0]
        print(f"Processing file for ticker: {ticker}")

        (df, ticker) = read_file(file)

        flag = save_file(SILVER_PATH, df, ticker)

        if not flag:
            print(f"Error processing data for {ticker}")
    
    # Read silver data
    silver_files = get_all_files(SILVER_PATH, "*.csv")
    stock_data = extract_data(silver_files)
    print(stock_data.head())

    # Transform silver data
    stock_data_agg = transform_data(stock_data)
    print(stock_data_agg.head())

    # Load gold data
    flag = save_file(GOLD_PATH, stock_data_agg, "avg_open_price")
    if not flag:
        print("ETL succeeded")