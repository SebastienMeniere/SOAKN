UNWIND $rows AS row

// 1. hub --------------------------------------------------------------------
MERGE (t:Transaction {id: row.txn_id})
  ON CREATE SET t.ts      = row.date,
                t.items   = row.items,
                t.cost    = row.cost,
                t.payment = row.payment,
                t.discount= row.discount

// 2. dimensional nodes ------------------------------------------------------
MERGE (c:Customer {name: row.customer})
  ON CREATE SET c.category = row.cust_cat
MERGE (c)-[:MADE]->(t)

MERGE (city:City {name: row.city})
MERGE (t)-[:AT_CITY]->(city)

MERGE (st:StoreType {type: row.store_type})
MERGE (t)-[:STORE_TYPE]->(st)

MERGE (season:Season {name: row.season})
MERGE (t)-[:IN_SEASON]->(season)

// 3. optional promotion edge  ----------------------------------------------
FOREACH (_ IN CASE
             WHEN row.promo IS NULL OR row.promo = 'None' THEN []
             ELSE [1]
           END |
  MERGE (pr:Promotion {name: row.promo})
  MERGE (t)-[:USED_PROMO]->(pr)
)

// ----  ADD A WITH HERE  ----
WITH row, t   // pass the data you still need forward

// 4. products ---------------------------------------------------------------
UNWIND row.products AS prod
  MERGE (p:Product {name: prod})
  MERGE (t)-[:CONTAINS {qty: 1}]->(p)
