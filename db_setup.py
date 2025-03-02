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


# establish a connection
logging.debug("Attempting to connect to postgres database")
try:
    conn = psycopg2.connect(
        dbname=DEFAULT_DB, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
    )
    conn.autocommit = True
    cur = conn.cursor()

    logging.info("Database connected successfully, cursor was obtained!")

    # Create the Database
    cur.execute("SELECT 1 FROM pg_database where datname = %s;", (DB_NAME,))
    exist = cur.fetchone()
    if exist:
        logging.warning("Database already exists")
    else:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_NAME)))
        logging.info("Database Created!")

except psycopg2.Error as e:
    logging.error(f"Database connection failed: {e}")

finally:
    if conn:
        conn.close()
    if cur:
        cur.close()
    logging.debug("Database connection closed")


# Establish a new connection to the created database and create the tables
try:
    logging.debug(f"Attempting to connect to {DB_NAME}...")
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, host=DB_HOST, password=DB_PASSWORD
    )
    conn.autocommit = True
    cur = conn.cursor()
    logging.info(f"{DB_NAME} connected successfully!")

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
# make the connections

# close connection
