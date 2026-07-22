import sys
import zipfile
import pandas as pd
import os
import time


extract_path = r"D:\Weather\Extracted"

os.makedirs(extract_path, exist_ok=True)


zip_path = r"D:\Weather\archive.zip"

output_path = r"D:\Weather\parquet\w_d_1"

os.makedirs(output_path, exist_ok=True)


with zipfile.ZipFile(zip_path, 'r') as z:

    # get only w_d_1 csv files
    csv_files = [
        f for f in z.namelist()
        if f.startswith("w_d_1/")
        and f.endswith(".csv")
    ]

    print("Total w_d_1 files:", len(csv_files))


    start = time.time()


    for i, file in enumerate(csv_files):

        try:

            city = os.path.basename(file).replace(".csv", "")

            print(
                f"\n[{i+1}/{len(csv_files)}] Processing {city}"
            )


            # read csv from zip
            with z.open(file) as f:

                df = pd.read_csv(
                    f,
                    encoding="utf-8-sig"
                )


            # add city column
            df["city"] = city


            # parquet output path
            output_file = os.path.join(
                output_path,
                city + ".parquet"
            )


            # save parquet
            df.to_parquet(
                output_file,
                engine="pyarrow",
                index=False
            )


            print(
                "Saved:",
                output_file,
                "Rows:",
                len(df)
            )


        except Exception as e:

            print(
                "FAILED:",
                city,
                e
            )


    print("\nCompleted")
    print(
        "Total time:",
        round((time.time()-start)/60,2),
        "minutes"
    )

    ip_path = r"D:\Weather\archive.zip"

output_path = r"D:\Weather\parquet\w_d_2"

os.makedirs(output_path, exist_ok=True)


with zipfile.ZipFile(zip_path, 'r') as z:

    # get only w_d_2 csv files
    csv_files = [
        f for f in z.namelist()
        if f.startswith("w_d_2/")
        and f.endswith(".csv")
    ]

    print("Total w_d_2 files:", len(csv_files))


    start = time.time()


    for i, file in enumerate(csv_files):

        try:

            city = os.path.basename(file).replace(".csv", "")

            print(
                f"\n[{i+1}/{len(csv_files)}] Processing {city}"
            )


            # read csv directly from zip
            with z.open(file) as f:

                df = pd.read_csv(
                    f,
                    encoding="utf-8-sig"
                )


            # add city column
            df["city"] = city


            # parquet file path
            output_file = os.path.join(
                output_path,
                city + ".parquet"
            )


            # convert to parquet
            df.to_parquet(
                output_file,
                engine="pyarrow",
                index=False
            )


            print(
                "Saved:",
                city,
                "Rows:",
                len(df)
            )


        except Exception as e:

            print(
                "FAILED:",
                city,
                e
            )


    print("\nW_D_2 Completed")

    print(
        "Total time:",
        round((time.time()-start)/60,2),
        "minutes"
    )