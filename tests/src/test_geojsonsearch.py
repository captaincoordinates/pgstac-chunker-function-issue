from os import environ
import json
import psycopg2
from time import sleep


def _get_connection():
    host = environ["PGHOST"]
    try_count = 0
    while try_count < 3:
        try:
            return psycopg2.connect(
                dbname=environ["PGDATABASE"],
                user=environ["PGUSER"],
                password=environ["PGPASSWORD"],
                port=5432,
                host=host,
                options="-c search_path=pgstac,public -c application_name=pgstac",
            )
        except Exception:
            sleep(1)
            try_count += 1
    raise Exception(f"all {host} connection attempts exhausted")

def load_json(filepath: str):
    """yield JSON file content."""
    with open(filepath, "r") as f:
        return "".join(f.readlines()).strip()

def setup_module() -> None:
    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM pgstac.create_collection(%s::jsonb);", (load_json("/data/collection.json"),)
            )
            for item_name in ("item1", "item2"):
                cur.execute("SELECT * FROM pgstac.create_item(%s::jsonb);", (load_json(f"/data/{item_name}.json"),))

def test_verify() -> None:
    with _get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT id FROM collections")
            rows = cursor.fetchall()
            assert len(rows) == 1, "incorrect number of collections"
            assert rows[0][0] == "joplin", "incorrect collection data"
            cursor.execute("SELECT id FROM items")
            rows = cursor.fetchall()
            assert len(rows) == 2, "incorrect number of items"


def test_geojsonsearch_no_ordering() -> None:
    with _get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""SELECT hash FROM search_query('{"collections": ["joplin"]}')""")
            hash = cursor.fetchone()[0]
            assert len(hash) == 32, f"hash not as expected: {hash}"
            cursor.execute("SELECT * FROM geojsonsearch(%s, %s, %s, %s, %s, %s, %s, %s)", (
                json.dumps({
                    "type": "Point",
                    "coordinates": [-94.6, 37.06],
                }),
                hash,
                json.dumps(["assets", "id", "bbox", "collection"]),
                10000,
                100,
                "5 seconds",
                True,
                True,
            ))
            rows = cursor.fetchall()
            assert len(rows) == 1, f"wrong number of rows ({len(rows)})"
            assert len(rows[0][0]) == 2, f"wrong number of features ({len(rows[0][0])})"


def test_geojsonsearch_datetime_ordering() -> None:
    with _get_connection() as connection:
        with connection.cursor() as cursor:
            cursor = connection.cursor()
            cursor.execute("""SELECT hash FROM search_query('{"sortby": [{"field": "datetime", "direction": "desc"}], "collections": ["joplin"]}')""")
            hash = cursor.fetchone()[0]
            assert len(hash) == 32, f"hash not as expected: {hash}"
            cursor.execute("SELECT * FROM geojsonsearch(%s, %s, %s, %s, %s, %s, %s, %s)", (
                json.dumps({
                    "type": "Point",
                    "coordinates": [-94.6, 37.06],
                }),
                hash,
                json.dumps(["assets", "id", "bbox", "collection"]),
                10000,
                100,
                "5 seconds",
                True,
                True,
            ))
            rows = cursor.fetchall()
            assert len(rows) == 1, f"wrong number of rows ({len(rows)})"
            assert len(rows[0][0]) == 2, f"wrong number of features ({len(rows[0][0])})"
