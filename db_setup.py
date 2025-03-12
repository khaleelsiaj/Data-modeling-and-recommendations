import psycopg2
from psycopg2 import sql
import logging
from credentials import DB_CONFIG

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


DEFAULT_DB = "postgres"


def connect_to_db(dbname_override=None):
    try:
        config = DB_CONFIG.copy()
        logging.debug(f"Attempting to connect to {config['dbname']}")
        if dbname_override:
            config["dbname"] = dbname_override
        conn = psycopg2.connect(**config)
        logging.debug(f"Connected successfully to {config['dbname']}")
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


# Establish a new connection to the created database and create the tables
def create_tables():
    conn = None
    cur = None
    try:
        conn = connect_to_db()
        if not conn:
            return
        cur = conn.cursor()

        create_tables_sql = """
        CREATE TABLE IF NOT EXISTS customer (
                    CustomerID INTEGER PRIMARY KEY,
                    Country VARCHAR(50)
                    );

        CREATE TABLE IF NOT EXISTS invoice (
                    InvoiceNo VARCHAR(7) PRIMARY KEY,
                    CustomerID INTEGER,
                    InvoiceDate TIMESTAMP,
                    FOREIGN KEY (CustomerID) REFERENCES customer(CustomerID) ON DELETE CASCADE
                    );

        CREATE TABLE IF NOT EXISTS product (
                    StockCode VARCHAR(50) PRIMARY KEY,
                    UnitPrice NUMERIC(10,2),
                    Description Text
                    );

        CREATE TABLE IF NOT EXISTS invoice_details (
                    InvoiceNO VARCHAR(7),
                    StockCode VARCHAR(50),
                    Quantity INTEGER,
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


if __name__ == "__main__":
    create_db()
    create_tables()
