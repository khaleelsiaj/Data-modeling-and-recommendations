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


def load_data():
    df = pd.read_csv("data/Online Retail.csv")
    logging.info("Dataset read")
    return df


def transform_data(df):

    # check and remove corrupted rows (where Description is Null and UnitPrice is 0)
    missing_values = df.isnull().sum()
    logging.info(f"Missing values per column:\n{missing_values}.")

    df_null = df[df["Description"].isnull() & (df["UnitPrice"] == 0.0)].shape[0]
    logging.info(f"Identified {df_null} corrupted rows.")

    # remove rows with corrupt condition
    corrupt_condition = (df["Description"].isnull()) & (df["UnitPrice"] == 0.0)
    df_cleaned = df.drop(df[corrupt_condition].index)
    logging.info(f"corrupted rows removed, New dataset size is {df_cleaned.shape}.")

    # check duplicates
    duplicates_rows_num = df_cleaned.duplicated().sum()
    df_cleaned.drop_duplicates(inplace=True)
    logging.info(f"Identified {duplicates_rows_num} duplicate rows.")
    logging.info(f"duplicate rows removed, New dataset size is {df_cleaned.shape}.")

    # correct data types
    df_cleaned["InvoiceDate"] = df_cleaned["InvoiceDate"].astype("datetime64[ns]")
    df_cleaned["Country"] = df_cleaned["Country"].astype("string")
    logging.info("Converted 'InvoiceDate' to datetime and 'Country' to string.")

    # transform quantity values
    df_cleaned["Quantity"] = abs(df_cleaned["Quantity"])

    return df_cleaned


def partition_data(df_cleaned):

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

    return customer_df, invoice_df, invoice_details_df, product_df


def insert_with_copy(cur, df, table_name, columns):
    buffer = io.StringIO()
    df.to_csv(buffer, sep="|", index=False, header=False)
    buffer.seek(0)
    cur.copy_from(buffer, table_name, sep="|", columns=columns)
    cur.execute(f"SELECT COUNT(*) from {table_name}")
    rows_count = cur.fetchone()[0]
    logging.info(f"Inserted {rows_count} rows into {table_name}")
    return rows_count


def insert_data(dfs):
    conn = connect_to_db()
    if conn:
        try:
            customer_df = dfs[0]
            invoice_df = dfs[1]
            invoice_details_df = dfs[2]
            product_df = dfs[3]
            logging.debug(
                "Data is being inserted into the tables, this might take a while..."
            )
            cur = conn.cursor()

            # insert customer
            rows_count = insert_with_copy(
                cur, customer_df, "customer", ["customerid", "country"]
            )
            if rows_count != len(customer_df):
                logging.warning(f"Expected {len(customer_df)} rows, got {rows_count}")

            # insert invoice
            rows_count = insert_with_copy(
                cur, invoice_df, "invoice", ["invoiceno", "customerid", "invoicedate"]
            )
            if rows_count != len(invoice_df):
                logging.warning(f"Expected {len(invoice_df)} rows, got {rows_count}")

            # insert product
            rows_count = insert_with_copy(
                cur, product_df, "product", ["stockcode", "unitprice", "description"]
            )
            if rows_count != len(product_df):
                logging.warning(f"Expected {len(product_df)} rows, got {rows_count}")

            # insert invoice_details
            rows_count = insert_with_copy(
                cur,
                invoice_details_df,
                "invoice_details",
                ["invoiceno", "stockcode", "quantity"],
            )
            if rows_count != len(invoice_details_df):
                logging.warning(f"Expected {len(product_df)} rows, got {rows_count}")

            conn.commit()
            logging.info("All data committed successfully")

        except psycopg2.Error as e:
            conn.rollback()
            logging.error(f"Unexpected Error: {e}")

        finally:
            cur.close()
            conn.close()


if __name__ == "__main__":
    df = load_data()
    df_cleand = transform_data(df)
    dfs = partition_data(df_cleand)
    insert_data(dfs)
