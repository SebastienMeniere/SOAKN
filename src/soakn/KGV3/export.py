#!/usr/bin/env python
"""
Export object-property triples from Neo4j (+n10s) to TSV files that
PyKEEN/-DGL-KE can digest   ––  ERROR-FREE VERSION.
"""
from neo4j import GraphDatabase
from collections import OrderedDict
from pathlib import Path
import csv

# ---------- CONFIG --------------------------------------------------------
NEO4J_URI  = "bolt://localhost:7687"     # single-instance = bolt://
NEO4J_USER = "neo4j"
NEO4J_PWD  = "Meniere19"
DATABASE   = "neo4j"                     # CHANGE if yours is not called neo4j
OUT_DIR    = Path("kge_export")
OUT_DIR.mkdir(exist_ok=True)

PREDICATES = [
    "http://example.com/retail#madeTransaction",
    "http://example.com/retail#hasBasket",
    "http://example.com/retail#atSeller",
    "http://example.com/retail#hasLine",
    "http://example.com/retail#ofProduct",
]

BATCH_SIZE = 1000      # pull this many triples per round-trip to the DB
# -------------------------------------------------------------------------


def fetch_batch(tx, preds, skip_, limit_):
    """
    Grab a slice of triples inside one *live* read-transaction,
    return them as a Python list – prevents 'Transaction closed'.
    """
    cypher = """
    CALL n10s.export.csv(
         { stream:true,
           predicates:$preds,
           skip:$skip_,
           limit:$limit_ })
    YIELD subject, predicate, object
    RETURN subject, predicate, object;
    """
    return [
        (r["subject"], r["predicate"], r["object"])
        for r in tx.run(cypher, preds=preds, skip_=skip_, limit_=limit_)
    ]


def main():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PWD))
    entity2id, relation2id = OrderedDict(), OrderedDict()
    triples_file = OUT_DIR / "triples.tsv"

    with driver.session(database=DATABASE) as sess, \
         open(triples_file, "w", newline="") as fout:

        writer = csv.writer(fout, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
        skip = 0
        while True:
            batch = sess.execute_read(
                fetch_batch, PREDICATES, skip, BATCH_SIZE
            )
            if not batch:
                break

            for s, p, o in batch:
                # entity mapping
                for n in (s, o):
                    if n not in entity2id:
                        entity2id[n] = len(entity2id)
                # relation mapping
                if p not in relation2id:
                    relation2id[p] = len(relation2id)

                writer.writerow([s, p, o])

            skip += BATCH_SIZE
            print(f"🟢  exported {skip:,} triples …")

    # -------- lookup tables -------------
    (OUT_DIR / "entity2id.txt").write_text(
        "\n".join(f"{uri}\t{idx}" for uri, idx in entity2id.items())
    )
    (OUT_DIR / "relation2id.txt").write_text(
        "\n".join(f"{uri}\t{idx}" for uri, idx in relation2id.items())
    )

    print(f"✅  Done: {len(entity2id):,} entities • {len(relation2id):,} "
          f"relations • triples → {triples_file}")


if __name__ == "__main__":
    main()
