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
    assert len(rows) == 1, "incorrect number of items"
    assert rows[0][0] == "item-1", "incorrect item data"


def test_geojsonsearch_no_ordering() -> None:
    cursor = _connection.cursor()
    cursor.execute("SELECT hash FROM search_query()")
    hash = cursor.fetchone()[0]
    assert len(hash) == 32, f"hash not as expected: {hash}"
    cursor.execute("SELECT * FROM geojsonsearch(%s, %s)", (
        json.dumps({
            "type": "Polygon",
            "coordinates": [[[
                -180, -90
            ], [
                180, -90
            ], [
                180, 90
            ], [
                -180, 90
            ], [
                -180, -90
            ]]]
        }),
        hash,
    ))
    rows = cursor.fetchall()
    assert len(rows) == 1, f"wrong number of rows ({len(rows)})"
    result = rows[0][0]
    assert result["features"][0]["id"] == "item-1", f"incorrect item returned {result}"


def test_geojsonsearch_datetime_ordering() -> None:
    cursor = _connection.cursor()
    cursor.execute("""SELECT hash FROM search_query('{"sortby": [{"field": "datetime", "direction": "desc"}]}')""")
    hash = cursor.fetchone()[0]
    assert len(hash) == 32, f"hash not as expected: {hash}"
    cursor.execute("SELECT * FROM geojsonsearch(%s, %s)", (
        json.dumps({
            "type": "Polygon",
            "coordinates": [[[
                -180, -90
            ], [
                180, -90
            ], [
                180, 90
            ], [
                -180, 90
            ], [
                -180, -90
            ]]]
        }),
        hash,
    ))
    rows = cursor.fetchall()
    assert len(rows) == 1, f"wrong number of rows ({len(rows)})"
    result = rows[0][0]
    assert result["features"][0]["id"] == "item-1", f"incorrect item returned {result}"
