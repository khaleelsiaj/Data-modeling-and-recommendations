import sys
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


def load_data(filepath="data/Online Retail.csv"):
    """Load data from a CSV file.

    Returns:
        pd.DataFrame: A DataFrame containing the loaded data.
    """
    df = pd.read_csv(filepath)
    logging.info("Dataset read")
    return df


def transform_data(df):
    """Transform the DataFrame by cleaning and preparing the data.

    Args:
        df (pd.DataFrame): The DataFrame to be transformed.

    Returns:
        pd.DataFrame: A cleaned and transformed DataFrame.
    """
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
    """Partition the cleaned DataFrame into separate DataFrames.

    Args:
        df_cleaned (pd.DataFrame): The cleaned DataFrame to be partitioned.

    Returns:
        tuple: A tuple containing DataFrames for customers, invoices, products, and invoice details.
    """
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

    return customer_df, invoice_df, product_df, invoice_details_df


def insert_with_copy(cur, df, table_name, columns):
    """Insert data into the database using the COPY command.

    Args:
        cur: A cursor object for executing database commands.
        df (pd.DataFrame): The DataFrame containing data to insert.
        table_name (str): The name of the table to insert data into.
        columns (list): The list of columns in the table.

    Returns:
        int: The number of rows inserted into the table.
    """
    buffer = io.StringIO()
    df.to_csv(buffer, sep="|", index=False, header=False)
    buffer.seek(0)
    cur.copy_from(buffer, table_name, sep="|", columns=columns)
    cur.execute(f"SELECT COUNT(*) from {table_name}")
    rows_count = cur.fetchone()[0]
    logging.info(f"Inserted {rows_count} rows into {table_name}")
    return rows_count


def insert_data(conn, dfs, table_configs):
    """Insert multiple DataFrames into the database.

    Args:
        conn: A connection object to the PostgreSQL database.
        dfs (list): A list of DataFrames to insert.
        table_configs (list): A list of tuples containing table names and column names.

    Returns:
        None
    """
    cur = conn.cursor()
    try:
        for df, (table_name, columns) in zip(dfs, table_configs):
            rows_count = insert_with_copy(cur, df, table_name, columns)
            if rows_count != len(df):
                logging.warning(f"Expected {len(df)} rows, got {rows_count}")
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        logging.error(f"Insert failed for table {table_name}: {e}")
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":

    table_configs = [
        ("customer", ["customerid", "country"]),
        ("invoice", ["invoiceno", "customerid", "invoicedate"]),
        ("product", ["stockcode", "unitprice", "description"]),
        ("invoice_details", ["invoiceno", "stockcode", "quantity"]),
    ]
    conn = connect_to_db()

    df = load_data()
    df_cleand = transform_data(df)
    dfs = partition_data(df_cleand)
    insert_data(conn, dfs, table_configs)
