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

DB_NAME = "sexy"
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

# create the database

# create the tables

# make the connections

# close connection
