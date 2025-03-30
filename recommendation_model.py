import pandas as pd
from db_setup import connect_to_db
from sklearn.metrics.pairwise import cosine_similarity
# fetch the required data


conn = connect_to_db()
cur = conn.cursor()
def load_data():
    cur.execute("""
        SELECT i.customerid, id.stockcode, id.quantity
        FROM invoice i
        JOIN invoice_details id
        ON i.invoiceno = id.invoiceno;
        """)
    data = cur.fetchall()
    df = pd.DataFrame(data, columns=['customer_id', 'stock_code', 'quantity'])

    df['purchased'] = 1

# add purchased column and create the pivot table
# df['purchased'] = (df['quantity'] > 0).astype(int)

user_item_matrix = df.pivot_table(
    index='customer_id',
    columns='stock_code',
    values='purchased',
    fill_value=0
)
user_item_matrix.duplicated().sum()
user_item_matrix_T = user_item_matrix.transpose()

# compute consine similarity
item_similarity = cosine_similarity(user_item_matrix_T)

item_similarity_df = pd.DataFrame(
    item_similarity,
    index=user_item_matrix.columns,
    columns=user_item_matrix.columns
)

# similar_items = item_similarity_df["10080"].sort_values(ascending=False).index[1:5]
# d =user_item_matrix.loc[18283]
# pd.DataFrame(d)



# recommendation function
def get_recommendations(customer_id, user_item_matrix, item_similarity, top_n=5 ):
    
    purchased_items = user_item_matrix.loc[customer_id][user_item_matrix.loc[customer_id] > 0].index

    recommendations = {}

    for item in purchased_items:
        similar_items = item_similarity_df.loc[item].sort_values(ascending=False)[1:top_n+1].index
        for sim_item in similar_items:
            if sim_item not in purchased_items:
                if sim_item not in recommendations:
                    recommendations[sim_item] = item_similarity_df[item][sim_item]
                else:
                    recommendations[sim_item] += item_similarity_df[item][sim_item]
    
    sorted_rec = sorted(recommendations.items(), key= lambda x:x[1], reverse=True)[:top_n]
    return [item for item, _ in sorted_rec]


recommendations = get_recommendations(18283, user_item_matrix, item_similarity)



purchased_items = user_item_matrix.loc[18283][user_item_matrix.loc[18283] > 0].index
for item in purchased_items:
    cur.execute('SELECT DESCRIPTION FROM product WHERE stockcode = %s;', (item,))
    r = cur.fetchone()
    print(r)
for item in recommendations:
    cur.execute('SELECT DESCRIPTION FROM product WHERE stockcode = %s;', (item,))
    r = cur.fetchone()
    print(r)