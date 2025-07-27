import pandas as pd
import rdflib as rdf
import pathlib
import datetime
import yaml
from urllib.parse import quote_plus
from neo4j import GraphDatabase, basic_auth
import time
import glob

# --- Load config ---
CFG = yaml.safe_load(open("config.yaml"))

# --- Constants and Namespace binding ---
BASE = CFG["base_uri"]
NS = {pfx: rdf.Namespace(uri) for pfx, uri in CFG["prefixes"].items()}
rdf_graph = rdf.Graph()
for pfx, ns in NS.items():
    rdf_graph.bind(pfx, ns)

def uri(entity_type: str, key) -> str:
    return f"{BASE}{entity_type}/{quote_plus(str(key))}"

def new_graph():
    g = rdf.Graph()
    for pfx, ns in NS.items():
        g.bind(pfx, ns)
    return g

def day_time_to_iso(day, trans_time):
    date = datetime.date(2017, 1, 1) + datetime.timedelta(int(day))
    hh, mm = divmod(int(trans_time), 100)
    return datetime.datetime.combine(date, datetime.time(hh, mm)).isoformat()

def parse_household_size(size_text: str):
    if not size_text:
        return None
    if "-" in size_text:
        return int(size_text.split("-")[-1])
    if size_text.endswith("+"):
        return int(size_text[:-1])
    return int(size_text)

# --- Export folder ---
export = pathlib.Path("export")
export.mkdir(exist_ok=True)

# --- Transactions RDF ---
chunksize = 10000
for chunk in pd.read_csv("../../../data/transactions_small.csv", chunksize=chunksize):
    g = new_graph()
    for row in chunk.itertuples():
        txn = rdf.URIRef(uri("txn", row.BASKET_ID))
        g.add((txn, rdf.RDF.type, NS["rio"].Transaction))
        g.add((txn, NS["rio"].hasCustomer, rdf.URIRef(uri("household", row.household_key))))
        g.add((txn, NS["time"].inXSDDateTime, rdf.Literal(day_time_to_iso(row.DAY, row.TRANS_TIME), datatype=rdf.XSD.dateTime)))
        g.add((txn, NS["rio"].occurredAt, rdf.URIRef(uri("store", row.STORE_ID))))

        line = rdf.BNode()
        g.add((line, rdf.RDF.type, NS["rio"].TransactionLine))
        g.add((line, NS["rio"].lineProduct, rdf.URIRef(uri("product", row.PRODUCT_ID))))
        g.add((line, NS["rio"].quantity, rdf.Literal(int(row.QUANTITY))))
        g.add((line, NS["rio"].lineTotal, rdf.Literal(float(row.SALES_VALUE))))
        g.add((txn, NS["rio"].containsLine, line))

    ttl_file = export / f"transactions_{chunk.index.start}.ttl"
    g.serialize(destination=ttl_file, format="turtle")

# --- Products RDF ---
df = pd.read_csv("../../../data/product.csv", dtype=str).fillna("")
g = new_graph()
for row in df.itertuples(index=False):
    prd = rdf.URIRef(uri("product", row.PRODUCT_ID))
    g.add((prd, rdf.RDF.type, NS["gr"].ProductOrServiceModel))
    g.add((prd, NS["gr"].name, rdf.Literal(row.SUB_COMMODITY_DESC)))
    g.add((prd, NS["schema"].brand, rdf.Literal(row.BRAND)))

    if row.MANUFACTURER:
        m_uri = rdf.URIRef(uri("manufacturer", row.MANUFACTURER))
        g.add((m_uri, rdf.RDF.type, NS["schema"].Organization))
        g.add((prd, NS["schema"].manufacturer, m_uri))

    if row.CURR_SIZE_OF_PRODUCT:
        g.add((prd, NS["schema"].packageQuantity, rdf.Literal(row.CURR_SIZE_OF_PRODUCT)))

    dept_uri = rdf.URIRef(uri("dept", row.DEPARTMENT))
    commod_uri = rdf.URIRef(uri("commodity", row.COMMODITY_DESC))
    subcommod_uri = rdf.URIRef(uri("subcomm", row.SUB_COMMODITY_DESC))

    g.add((dept_uri, rdf.RDF.type, NS["skos"].Concept))
    g.add((commod_uri, rdf.RDF.type, NS["skos"].Concept))
    g.add((subcommod_uri, rdf.RDF.type, NS["skos"].Concept))

    g.add((commod_uri, NS["skos"].broader, dept_uri))
    g.add((subcommod_uri, NS["skos"].broader, commod_uri))
    g.add((prd, NS["skos"].broader, subcommod_uri))

g.serialize(destination=export / "products.ttl", format="turtle")
print(f"✓ products.ttl written ({len(df)} products)")

# --- Households RDF ---
df = pd.read_csv("../../../data/hh_demographic.csv", dtype=str).fillna("")
g = new_graph()
for row in df.itertuples(index=False):
    hh = rdf.URIRef(uri("household", row.household_key))
    g.add((hh, rdf.RDF.type, NS["foaf"].Group))

    if row.AGE_DESC:
        g.add((hh, NS["rupo"].ageBand, rdf.Literal(row.AGE_DESC)))
    if row.INCOME_DESC:
        g.add((hh, NS["rupo"].incomeBand, rdf.Literal(row.INCOME_DESC)))
    if row.HOMEOWNER_DESC:
        g.add((hh, NS["rupo"].homeownerStatus, rdf.Literal(row.HOMEOWNER_DESC)))
    if row.MARITAL_STATUS_CODE:
        g.add((hh, NS["rupo"].maritalStatus, rdf.Literal(row.MARITAL_STATUS_CODE)))

    size_val = parse_household_size(row.HOUSEHOLD_SIZE_DESC)
    if size_val:
        g.add((hh, NS["rupo"].householdSize, rdf.Literal(size_val)))

    if row.KID_CATEGORY_DESC:
        g.add((hh, NS["rupo"].kidCategory, rdf.Literal(row.KID_CATEGORY_DESC)))

g.serialize(destination=export / "households.ttl", format="turtle")
print(f"✓ households.ttl written ({len(df)} households)")

# --- Generate schema.ttl ---
g = new_graph()
for tname, table in CFG["tables"].items():
    g.add((rdf.URIRef(table["class"]), rdf.RDF.type, rdf.OWL.Class))
    for col, prop in table.get("props", {}).items():
        g.add((rdf.URIRef(prop), rdf.RDF.type, rdf.OWL.DatatypeProperty))
g.serialize(destination=export / "schema.ttl", format="turtle")
print("✓ schema.ttl written.")

# --- Neo4j Loading ---
URI, AUTH = "bolt://localhost:7687", basic_auth("neo4j", "sebastienM19")
driver = GraphDatabase.driver(URI, auth=AUTH)

def register_prefixes():
    with driver.session() as s:
        for pfx, ns in CFG["prefixes"].items():
            s.run("CALL n10s.nsprefixes.add($pfx, $uri)", pfx=pfx, uri=ns)
register_prefixes()
print("✓ Neo4j prefixes registered")

def import_ttl(path):
    with driver.session() as s:
        s.run("""
            CALL n10s.rdf.import.fetch($url,'Turtle',{
                handleVocabUris: "SHORTEN",
                typesToLabels: true
            })
        """, url=f"file:///{pathlib.Path(path).resolve()}")
        print("✓ Imported:", pathlib.Path(path).name)

import_ttl(export / "schema.ttl")
for ttl in glob.glob("export/*.ttl"):
    if "schema.ttl" not in ttl:
        import_ttl(ttl)
        time.sleep(0.1)
