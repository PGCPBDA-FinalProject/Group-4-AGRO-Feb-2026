import zipfile
import pandas as pd
import os

ZIP_PATH = "/home/hadoop/archive.zip"
OUTPUT_PATH = "/home/hadoop/parquet/city_master"

os.makedirs(OUTPUT_PATH, exist_ok=True)

with zipfile.ZipFile(ZIP_PATH, "r") as z:

    with z.open(
        "Weather_Data_Scraping_and_Analysis/weather.csv"
    ) as f:

        df = pd.read_csv(f)

df.drop(columns=["Unnamed: 0"], inplace=True)

output_file = os.path.join(
    OUTPUT_PATH,
    "city_master.parquet"
)

df.to_parquet(
    output_file,
    engine="pyarrow",
    compression="snappy",
    index=False
)

print("city_master.parquet created successfully")