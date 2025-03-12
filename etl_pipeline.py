import io
import psycopg2
import pandas as pd
import logging
from db_setup import connect_to_db

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
duplicates_rows_num = df_cleaned.duplicated().sum()
df_cleaned.drop_duplicates(inplace=True)
logging.info(f"Identified {duplicates_rows_num} duplicate rows.")
logging.info(f"duplicate rows removed, New dataset size is {df_cleaned.shape}.")


# check data types
df_cleaned.dtypes
df_cleaned["InvoiceDate"] = df_cleaned["InvoiceDate"].astype("datetime64[ns]")
df_cleaned["Country"] = df_cleaned["Country"].astype("string")
logging.info("Converted 'InvoiceDate' to datetime and 'Country' to string.")


# partitioning data into different Data frames
customer_df = df_cleaned[["CustomerID", "Country"]]
invoice_df = df_cleaned[["InvoiceNo", "CustomerID", "InvoiceDate"]]
invoice_details_df = df_cleaned[["InvoiceNo", "StockCode", "Quantity"]]
product_df = df_cleaned[["StockCode", "UnitPrice", "Description"]]
logging.info("Dataset successfully partitioned into Data frames.")


# drop duplicates for different data frames
customer_df = customer_df.drop_duplicates(subset=["CustomerID"])
invoice_df = invoice_df.drop_duplicates(subset=["InvoiceNo"])
invoice_details_df = invoice_details_df.drop_duplicates(
    subset=["InvoiceNo", "StockCode"]
)
product_df = product_df.drop_duplicates(subset=["StockCode"])

# fix numbers values in quantity column in invoice_details table
invoice_details_df["Quantity"] = abs(invoice_details_df["Quantity"])

# connect to the database
conn = connect_to_db()
if conn:
    try:
        logging.debug(
            "Data is being inserted into the tables, this might take a while..."
        )
        cur = conn.cursor()

        # insert into customer table
        try:
            customer_data = [tuple(row) for row in customer_df.itertuples(index=False)]
            cur.executemany(
                "INSERT INTO customer (customerid, country) VALUES(%s, %s)",
                customer_data,
            )
            logging.info("Data inserted correctly into customer table")
        except psycopg2.Error as e:
            logging.error(f"Error inserting data into customer table: {e}")

        # insert into invoice table
        try:

            invoice_data = [tuple(row) for row in invoice_df.itertuples(index=False)]
            cur.executemany(
                "INSERT INTO invoice (invoiceno, customerid, invoicedate) VALUES(%s, %s, %s)",
                invoice_data,
            )
            logging.info("Data inserted correctly into invoice table")
        except psycopg2.Error as e:
            logging.error(f"Error inserting data into invoice table: {e}")

        # insert into product table
        try:

            product_data = [tuple(row) for row in product_df.itertuples(index=False)]
            cur.executemany(
                "INSERT INTO product (stockcode, unitprice, description) VALUES(%s, %s, %s)",
                product_data,
            )
            logging.info("Data inserted correctly into product table")

        except psycopg2.Error as e:
            logging.error(f"Error inserting data into product table: {e}")

        # insert into invoice_details table
        try:
            buffer = io.StringIO()
            invoice_details_df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            cur.copy_from(
                buffer,
                "invoice_details",
                sep=",",
                columns=["invoiceno", "stockcode", "quantity"],
            )
            logging.info("Data inserted correctly into invoice_details table")

        except psycopg2.Error as e:
            logging.error(f"Errors inserting data into invoice_details table: {e}")

    except psycopg2.Error as e:
        logging.error(f"Unexpected Error: {e}")
