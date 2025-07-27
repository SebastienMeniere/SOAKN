from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "Meniere19"))

def create_mappings():
    with driver.session() as session:
        # Set namespace prefix (once)
        session.run('CALL n10s.nsprefixes.add("ret", "http://example.com/retail#")')

        # Get all labels and map them as classes
        labels = session.run("MATCH (n) UNWIND labels(n) AS label RETURN DISTINCT label")
        for record in labels:
            label = record["label"]
            uri = f"http://example.com/retail#{label}"
            session.run("CALL n10s.mapping.add($uri, $label)", uri=uri, label=label)
            print(f"Mapped class: {label} -> {uri}")

        # Get all relationship types and map them
        rels = session.run("MATCH ()-[r]->() RETURN DISTINCT type(r) AS rel")
        for record in rels:
            rel = record["rel"]
            uri = f"http://example.com/retail#{rel}"
            session.run("CALL n10s.mapping.add($uri, $rel)", uri=uri, rel=rel)
            print(f"Mapped relationship: {rel} -> {uri}")

create_mappings()
