from dotenv import load_dotenv
import os
import sys
import psycopg2
from psycopg2 import sql
import logging

# configuring a logger
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)

# File handler
fh = logging.FileHandler("logs/db_setup.log")
fh.setLevel(logging.DEBUG)

# formatter and assign it to the handlers
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
fh.setFormatter(formatter)

# add handler to logging
logging.getLogger().addHandler(fh)

# fetch Database configurations
load_dotenv(".env")

DB_CONFIG = {
    "dbname": os.environ["DBNAME"],
    "user": os.environ["USER"],
    "password": os.environ["PASSWORD"],
    "host": os.environ["HOST"],
}

DEFAULT_DB = os.environ["DEFAULT_DB"]


def connect_to_db(dbname_override=None):
    """Connect to the PostgreSQL database.

    Args:
        dbname_override (str, optional): The name of the database to connect to.
                                          If not provided, the default database is used.

    Returns:
        conn: A connection object to the PostgreSQL database.
    """
    try:
        config = DB_CONFIG.copy()
        logging.debug(f"Attempting to connect to {config['dbname']}")
        if dbname_override:
            config["dbname"] = dbname_override
        conn = psycopg2.connect(**config)
        logging.debug(f"Connected successfully to {config['dbname']}")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Connection error: {e}")
        sys.exit(1)


def database_exists(conn, db_name):
    """Check if a database exists.

    Args:
        conn: A connection object to the PostgreSQL database.
        db_name (str): The name of the database to check.

    Returns:
        bool: True if the database exists, False otherwise.
    """
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
    exists = cur.fetchone()
    cur.close()
    return True if exists else False


def create_db():
    """Create a new database if it does not already exist.

    Returns:
        bool: True if the database was created, False if it already exists.
    """
    conn = connect_to_db(DEFAULT_DB)
    conn.autocommit = True

    if not conn:
        return False

    if database_exists(conn, DB_CONFIG["dbname"]):
        logging.error(f"Database {DB_CONFIG['dbname']} already exists")

    else:
        cur = conn.cursor()
        cur.execute(
            sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_CONFIG["dbname"]))
        )
        logging.info(f"Database {DB_CONFIG['dbname']} created!")
        cur.close()
    conn.close()
    return True


def create_tables():
    """Create tables in the database for customer, invoice, product, and invoice details.

    Returns:
        None
    """
    conn = None
    cur = None
    try:
        conn = connect_to_db()
        if not conn:
            return
        cur = conn.cursor()

        create_tables_sql = """
        CREATE TABLE IF NOT EXISTS customer (
                    CustomerID INTEGER PRIMARY KEY NOT NULL,
                    Country VARCHAR(50) NOT NULL
                    );

        CREATE TABLE IF NOT EXISTS invoice (
                    InvoiceNo VARCHAR(7) PRIMARY KEY NOT NULL,
                    CustomerID INTEGER NOT NULL,
                    InvoiceDate TIMESTAMP NOT NULL,
                    FOREIGN KEY (CustomerID) REFERENCES customer(CustomerID) ON DELETE CASCADE
                    );

        CREATE TABLE IF NOT EXISTS product (
                    StockCode VARCHAR(50) PRIMARY KEY NOT NULL,
                    UnitPrice NUMERIC(10,2) NOT NULL,
                    Description Text NOT NULL
                    );

        CREATE TABLE IF NOT EXISTS invoice_details (
                    InvoiceNO VARCHAR(7) NOT NULL,
                    StockCode VARCHAR(50) NOT NULL,
                    Quantity INTEGER NOT NULL,
                    PRIMARY KEY(InvoiceNo, StockCode),
                    FOREIGN KEY (InvoiceNO) REFERENCES invoice(InvoiceNo) ON DELETE CASCADE,
                    FOREIGN KEY (StockCode) REFERENCES product(StockCode) ON DELETE CASCADE
                    );

        """
        logging.debug("Executing tables creation query")
        cur.execute(create_tables_sql)
        conn.commit()
        logging.info("Tables created successfully!")

    except psycopg2.Error as e:
        cur.rollback()
        logging.error(f"Tables creation failed {e}")
        print(e)
        sys.exit(1)

    finally:
        if conn:
            conn.close()
        if cur:
            cur.close()
        logging.debug("Database connection closed")


if __name__ == "__main__":
    if create_db():
        create_tables()
    else:
        logging.info("Skipped table creation as database was not created")
