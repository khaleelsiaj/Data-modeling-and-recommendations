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

# set formatter and assign it to the handlers
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
fh.setFormatter(formatter)


# add handler to logging
logging.getLogger().addHandler(fh)

# defining credintials

DB_NAME = "retail_db"
DB_USER = "postgres"
DB_PASSWORD = "123"
DB_HOST = "localhost"
DEFAULT_DB = "postgres"


def connect_to_db(db_name):
    try:
        logging.debug(f"Attempting to connect to {db_name} ...")
        conn = psycopg2.connect(
            dbname=db_name, user=DB_USER, host=DB_HOST, password=DB_PASSWORD
        )
        logging.debug(f"Connected successfully to {db_name}")
        conn.autocommit = True
        return conn
    except psycopg2.Error as e:
        logging.error(f"Connection error: {e}")


def database_exists(conn, db_name):
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
    exists = cur.fetchone()
    cur.close()
    return True if exists else False


# establish a connection
def create_db():
    conn = connect_to_db(DEFAULT_DB)

    if not conn:
        return

    if database_exists(conn, DB_NAME):
        logging.error(f"Database {DB_NAME} already exists")

    else:
        cur = conn.cursor()
        cur.execute(sql.SQL("CREATE TABLE {}").format(sql.Identifier(DB_NAME)))
        logging.info(f"Database {DB_NAME} created!")
        cur.close()
    conn.close()


# Establish a new connection to the created database and create the tables
def create_tables():
    try:
        conn = connect_to_db(DB_NAME)
        if not conn:
            return
        cur = conn.cursor()

        create_tables_sql = """
        CREATE TABLE IF NOT EXISTS customer (
                    CustomerID INTEGER PRIMARY KEY,
                    Country VARCHAR(50)
                    );

        CREATE TABLE IF NOT EXISTS invoice (
                    InvoiceNo INTEGER PRIMARY KEY,
                    CustomerID INTEGER,
                    InvoiceDate TIMESTAMP,
                    FOREIGN KEY (CustomerID) REFERENCES customer(CustomerID) ON DELETE CASCADE
                    );

        CREATE TABLE IF NOT EXISTS product (
                    StockCode VARCHAR(6) PRIMARY KEY,
                    UnitPrice NUMERIC(10,2),
                    Description Text
                    );

        CREATE TABLE IF NOT EXISTS invoice_details (
                    InvoiceNO INTEGER,
                    StockCode VARCHAR(6),
                    Quantity SMALLINT,
                    PRIMARY KEY(InvoiceNo, StockCode),
                    FOREIGN KEY (InvoiceNO) REFERENCES invoice(InvoiceNo) ON DELETE CASCADE,
                    FOREIGN KEY (StockCode) REFERENCES product(StockCode) ON DELETE CASCADE
                    );

        """
        logging.debug("Executing tables creation query")
        cur.execute(create_tables_sql)
        logging.info("Tables created successfully!")

    except psycopg2.Error as e:
        logging.error(f"Tables creation failed {e}")
        print(e)

    finally:
        if conn:
            conn.close()
        if cur:
            cur.close()
        logging.debug("Database conncetion closed")


create_db()
create_tables()
