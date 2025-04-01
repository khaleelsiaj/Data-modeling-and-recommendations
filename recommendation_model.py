import pandas as pd
from db_setup import connect_to_db
from sklearn.metrics.pairwise import cosine_similarity
import logging
import argparse

# configuring a logger
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)

# File handler
fh = logging.FileHandler("logs/recommendation_model.log")
fh.setLevel(logging.DEBUG)

# set formatter and assign it to the handlers
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
fh.setFormatter(formatter)

# add handler to logging
logging.getLogger().addHandler(fh)


def load_data(conn):
    """Fetch the required data from the database.

    Args:
        conn: A connection object to the PostgreSQL database.

    Returns:
        pd.DataFrame: A DataFrame containing customer IDs, stock codes, quantities, and a purchased feature.
    """
    # establish a connection with the db
    cur = conn.cursor()

    # fetch the required data
    cur.execute(
        """
        SELECT i.customerid, id.stockcode, id.quantity
        FROM invoice i
        JOIN invoice_details id
        ON i.invoiceno = id.invoiceno;
        """
    )
    data = cur.fetchall()

    # load the data into dataframe
    df = pd.DataFrame(data, columns=["customer_id", "stock_code", "quantity"])

    # add a new purchased feature
    df["purchased"] = 1

    return df


def get_matrix(df):
    """Create a user-item matrix from the DataFrame.

    Args:
        df (pd.DataFrame): A DataFrame containing customer IDs, stock codes, and purchased features.

    Returns:
        pd.DataFrame: A pivot table representing the user-item interactions.
    """
    user_item_matrix = df.pivot_table(
        index="customer_id", columns="stock_code", values="purchased", fill_value=0
    )
    user_item_matrix.duplicated().sum()
    logging.debug("Created user_item_matrix df")
    return user_item_matrix


def compute_cosine_similarity(user_item_matrix):
    """Compute cosine similarity between items.

    Args:
        user_item_matrix (pd.DataFrame): A pivot table representing the user-item interactions.

    Returns:
        pd.DataFrame: A DataFrame containing cosine similarity scores between items.
    """
    user_item_matrix_T = user_item_matrix.transpose()

    # compute cosine similarity
    item_similarity = cosine_similarity(user_item_matrix_T)

    item_similarity_df = pd.DataFrame(
        item_similarity,
        index=user_item_matrix.columns,
        columns=user_item_matrix.columns,
    )

    logging.debug("Calculated cosine similarity and created item_similarity_df")

    return item_similarity_df


def compute_recommendations(customer_id, user_item_matrix, item_similarity_df, top_n=5):
    """Generate recommendations for a given customer based on purchased items.

    Args:
        customer_id (int): The ID of the customer for whom recommendations are generated.
        user_item_matrix (pd.DataFrame): A pivot table representing the user-item interactions.
        item_similarity_df (pd.DataFrame): A DataFrame containing cosine similarity scores between items.
        top_n (int, optional): The number of top recommendations to return. Defaults to 5.

    Returns:
        tuple: A tuple containing a list of recommended items and a list of purchased items.
    """
    if customer_id not in user_item_matrix.index:
        logging.error("Customer ID not found, try different one")
        return

    purchased_items = user_item_matrix.loc[customer_id][
        user_item_matrix.loc[customer_id] > 0
    ].index

    recommendations = {}

    for item in purchased_items:
        similar_items = (
            item_similarity_df.loc[item]
            .sort_values(ascending=False)[1 : top_n + 1]
            .index
        )
        for sim_item in similar_items:
            if sim_item not in purchased_items:
                if sim_item not in recommendations:
                    recommendations[sim_item] = item_similarity_df[item][sim_item]
                else:
                    recommendations[sim_item] += item_similarity_df[item][sim_item]

    sorted_rec = sorted(recommendations.items(), key=lambda x: x[1], reverse=True)[
        :top_n
    ]
    recommended_items = [item for item, _ in sorted_rec]
    logging.debug(f"Computed top {top_n} recommended items for customer: {customer_id}")

    return recommended_items, purchased_items


def show_recommendations(conn, customer_id, recommended_items, purchased_items):
    """Display the recommended items and items purchased by the customer.

    Args:
        conn: A connection object to the PostgreSQL database.
        customer_id (int): The ID of the customer.
        recommended_items (list): A list of recommended items.
        purchased_items (list): A list of items purchased by the customer.

    Returns:
        None
    """
    cur = conn.cursor()

    logging.info(f"Items bought by customer: {customer_id}")
    for item in purchased_items:
        cur.execute("SELECT DESCRIPTION FROM product WHERE stockcode = %s;", (item,))
        product_name = cur.fetchone()[0]
        logging.info(product_name)

    logging.info(f"Recommended items:")
    for item in recommended_items:
        cur.execute("SELECT description FROM product WHERE stockcode = %s;", (item,))
        product_name = cur.fetchone()[0]
        logging.info(product_name)


if __name__ == "__main__":
    # essential variables
    conn = connect_to_db()
    customer_id = 12346

    df = load_data(conn)
    user_item_matrix = get_matrix(df)
    item_similary_df = compute_cosine_similarity(user_item_matrix)
    recommended_items, purchased_items = compute_recommendations(
        customer_id,
        user_item_matrix=user_item_matrix,
        item_similarity_df=item_similary_df,
    )
    show_recommendations(conn, customer_id, recommended_items, purchased_items)
