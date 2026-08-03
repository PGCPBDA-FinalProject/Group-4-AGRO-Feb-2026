import zipfile
import pandas as pd
import os
import time

ZIP_PATH = "/home/hadoop/archive.zip"
OUTPUT_PATH = "/home/hadoop/parquet/w_d_2"

os.makedirs(OUTPUT_PATH, exist_ok=True)

start = time.time()

with zipfile.ZipFile(ZIP_PATH, "r") as z:

    csv_files = [
        f for f in z.namelist()
        if f.startswith("w_d_2/")
        and f.endswith(".csv")
    ]

    print("Total Files:", len(csv_files))

    for i, file in enumerate(csv_files, start=1):

        city = os.path.basename(file).replace(".csv", "")

        print(f"[{i}/{len(csv_files)}] {city}")

        with z.open(file) as f:
            df = pd.read_csv(f, encoding="utf-8-sig")
        if "Unnamed: 0" in df.columns:
            df.drop(columns=["Unnamed: 0"], inplace=True)

        df["city"] = city

        output_file = os.path.join(
            OUTPUT_PATH,
            city + ".parquet"
        )

        df.to_parquet(
            output_file,
            engine="pyarrow",
            compression="snappy",
            index=False
        )

print("WD2 Completed")
print("Time:", round((time.time()-start)/60,2), "minutes")
