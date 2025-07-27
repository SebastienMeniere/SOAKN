# ingest.py
import pandas as pd
from pathlib import Path
from neo4j import GraphDatabase

BATCH = 500          # tune up/down per memory & latency
CSV_DIR = Path("../../../data/")
NEO4J_URI = "bolt://localhost:7687"
AUTH     = ("neo4j", "Meniere19")

driver = GraphDatabase.driver(NEO4J_URI, auth=AUTH, max_connection_pool_size=4)

def chunk(df, size):
    for i in range(0, len(df), size):
        yield df.iloc[i:i+size]

# ────────────────────────────────────────────────────────────────
#  1. Households  →  (:Household)
# ────────────────────────────────────────────────────────────────
def load_consumers():
    df = pd.read_csv(CSV_DIR / "hh_demographic.csv")
    # column names to upper case for easy dot-notation access
    df.columns = df.columns.str.upper()

    for batch in chunk(df, BATCH):
        rows = batch.to_dict("records")
        q = """
        UNWIND $rows AS row
        MERGE (h:Household {household_key: toInteger(row.HOUSEHOLD_KEY)})
          ON CREATE SET
            h.age               = row.AGE_DESC,         // ret:age
            h.maritalStatus     = row.MARITAL_STATUS_CODE,
            h.incomeBracket     = row.INCOME_DESC,       // ret:incomeBracket
            h.homeOwnership     = row.HOMEOWNER_DESC,
            h.hhComposition     = row.HH_COMP_DESC,
            h.householdSize     = row.HOUSEHOLD_SIZE_DESC,
            h.kidCategory       = row.KID_CATEGORY_DESC ;
        """
        driver.execute_query(q, rows=rows)

# ────────────────────────────────────────────────────────────────
#  2. Products  →  (:Product) plus manufacturer (:Seller)
# ────────────────────────────────────────────────────────────────
def load_products():
    df = pd.read_csv(CSV_DIR / "product.csv")
    df.columns = df.columns.str.upper()

    for batch in chunk(df, BATCH):
        rows = batch.to_dict("records")
        q = """
        UNWIND $rows AS row
        MERGE (p:Product {product_id: toInteger(row.PRODUCT_ID)})
          ON CREATE SET
            p.department      = row.DEPARTMENT,
            p.brand           = row.BRAND,
            p.commodity       = row.COMMODITY_DESC,
            p.subCommodity    = row.SUB_COMMODITY_DESC,
            p.size            = row.CURR_SIZE_OF_PRODUCT
        // ---------- Manufacturer ----------
        MERGE (m:Seller {seller_id: toInteger(row.MANUFACTURER)})
          ON CREATE SET m.sellerType = 'Manufacturer'
        // ---------- Relationship ----------
        MERGE (p)-[:manufacturedBy]->(m)
        """
        driver.execute_query(q, rows=rows)

# ────────────────────────────────────────────────────────────────
#  3. Transactions, Basket, BasketItems
# ────────────────────────────────────────────────────────────────
def load_transactions():
    df = pd.read_csv(CSV_DIR / "transaction_data.csv")
    df.columns = df.columns.str.upper()

    for batch in chunk(df, BATCH):
        rows = batch.to_dict("records")
        q = """
        UNWIND $rows AS row
        MERGE (c:Household {household_key: toInteger(row.HOUSEHOLD_KEY)})
        MERGE (s:Store     {store_id:     toInteger(row.STORE_ID)})
          ON CREATE SET s.sellerType = 'Store' 
        MERGE (t:Transaction {basket_id: toInteger(row.BASKET_ID)})
          ON CREATE SET
            t.day       = toInteger(row.DAY),
            t.transTime = toInteger(row.TRANS_TIME) 
        MERGE (b:Basket {basket_id: toInteger(row.BASKET_ID)})
        MERGE (c)-[:madeTransaction]->(t)
        MERGE (t)-[:atSeller]->(s)
        MERGE (t)-[:hasBasket]->(b)
        MERGE (p:Product {product_id: toInteger(row.PRODUCT_ID)})
        MERGE (bi:BasketItem {
                basket_id: toInteger(row.BASKET_ID),
                product_id: toInteger(row.PRODUCT_ID)
              })
          ON CREATE SET
            bi.quantity          = toInteger(row.QUANTITY),
            bi.salesValue        = toFloat(row.SALES_VALUE),
            bi.retailDisc        = toFloat(row.RETAIL_DISC),
            bi.couponDisc        = toFloat(row.COUPON_DISC),
            bi.couponMatchDisc   = toFloat(row.COUPON_MATCH_DISC),
            bi.weekNo            = toInteger(row.WEEK_NO) 

        MERGE (b)-[:hasLine]->(bi)
        MERGE (bi)-[:ofProduct]->(p) 
        """
        driver.execute_query(q, rows=rows)

# ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # load_consumers()
    # load_products()
    load_transactions()
    driver.close()