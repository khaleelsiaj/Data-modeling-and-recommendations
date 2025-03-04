import pandas as pd
import logging

# configuring a logger
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)

# File handler
fh = logging.FileHandler("logs/etl_pipeline.log")
fh.setLevel(logging.DEBUG)

# set formatter and assign it to the handlers
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
fh.setFormatter(formatter)

# add handler to logging
logging.getLogger().addHandler(fh)


df = pd.read_csv("data/Online Retail.csv")
logging.info("Dataset read")


# check and remove corrupted rows (where Description is Null and UnitPrice is 0)
missing_values = df.isnull().sum()
logging.info(f"Missing values per column:\n{missing_values}.")

df_null = df[df["Description"].isnull() & (df["UnitPrice"] == 0.0)].shape[0]
logging.info(f"Identified {df_null} corrupted rows.")


# checking if all columns associated with customer 15287 are corrupted
# customer_rows = df[df["CustomerID"] == 15287]
# customer_rows[~corrupt_condition]

# remove rows with corrupt condition
corrupt_condition = (df["Description"].isnull()) & (df["UnitPrice"] == 0.0)
df_cleaned = df.drop(df[corrupt_condition].index)
logging.info(f"corrupted rows removed, New dataset size is {df_cleaned.shape}.")


# check and duplicates
# df_cleaned.duplicated().any()
duplicates_rows_num = df_cleaned.duplicated().sum()
df_cleaned.drop_duplicates(inplace=True)
logging.info(f"Identified {duplicates_rows_num} duplicate rows.")
logging.info(f"duplicate rows removed, New dataset size is {df_cleaned.shape}.")


# check data types
# df_cleaned.dtypes
df_cleaned["InvoiceDate"] = df_cleaned["InvoiceDate"].astype("datetime64[ns]")
df_cleaned["Country"] = df_cleaned["Country"].astype("string")
logging.info("Converted 'InvoiceDate' to datetime and 'Country' to string.")


# partitioning data into different Data frames
customer_df = df_cleaned[["CustomerID", "Country"]]
invoice_df = df_cleaned[["InvoiceNo", "CustomerID", "InvoiceDate"]]
invoice_details_df = df_cleaned[["InvoiceNo", "StockCode", "Quantity"]]
product_df = df_cleaned[["StockCode", "UnitPrice", "Description"]]
logging.info("Dataset successfully partitioned into Data frames.")
