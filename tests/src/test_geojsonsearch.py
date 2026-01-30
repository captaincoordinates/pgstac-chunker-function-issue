from os import environ
import json
import psycopg2


_connection = None

def setup_function() -> None:
    global _connection
    _connection = psycopg2.connect(
        dbname=environ["PGDATABASE"],
        user=environ["PGUSER"],
        password=environ["PGPASSWORD"],
        port=5432,
        host=environ["PGHOST"],
        options="-c search_path=pgstac,public -c application_name=pgstac",
    )


def teardown_function() -> None:
    global _connection
    _connection.close()


def test_verify() -> None:
    cursor = _connection.cursor()
    cursor.execute("SELECT id FROM collections")
    rows = cursor.fetchall()
    assert len(rows) == 1, "incorrect number of collections"
    assert rows[0][0] == "joplin", "incorrect collection data"
    cursor.execute("SELECT id FROM items")
    rows = cursor.fetchall()
    assert len(rows) == 2, "incorrect number of items"


def test_geojsonsearch_no_ordering() -> None:
    cursor = _connection.cursor()
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
    cursor = _connection.cursor()
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
