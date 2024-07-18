import sqlite3
import json

def new_ship(ship):
    with sqlite3.connect("./shipping.db") as conn:
        db_cursor = conn.cursor()

        db_cursor.execute(
            """
            INSERT INTO Ship (name, hauler_id)
            VALUES(?,?)
            """,
            (ship['name'], ship['hauler_id'])
        )

        rows_affected = db_cursor.rowcount
        
    return True if rows_affected > 0 else False

def update_ship(id, ship_data):
    with sqlite3.connect("./shipping.db") as conn:
        db_cursor = conn.cursor()

        db_cursor.execute(
            """
            UPDATE Ship
                SET
                    name = ?,
                    hauler_id = ?
            WHERE id = ?
            """,
            (ship_data['name'], ship_data['hauler_id'], id)
        )

        rows_affected = db_cursor.rowcount

    return True if rows_affected > 0 else False

def delete_ship(pk):
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        # Write the SQL query to get the information you want
        db_cursor.execute("""
        DELETE FROM Ship WHERE id = ?
        """, (pk,)
        )
        number_of_rows_deleted = db_cursor.rowcount

    return True if number_of_rows_deleted > 0 else False




def list_ships(url):
    # Open a connection to the database
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        ships = []

        if '_expand' in url['query_params']:
            # Write the SQL query to get the expanded information
            db_cursor.execute("""
            SELECT
                s.id,
                s.name,
                s.hauler_id,
                h.id AS haulerId,
                h.name AS haulerName,
                h.dock_id
            FROM Ship s
            JOIN Hauler h
                ON h.id = s.hauler_id
            """)
            query_results = db_cursor.fetchall()
            for row in query_results:
                hauler = {
                    "id": row['haulerId'],
                    "name": row['haulerName'],
                    "dock_id": row["dock_id"]
                }
                ship = {
                    "id": row['id'],
                    "name": row['name'],
                    "hauler_id": row["hauler_id"],
                    "hauler": hauler
                }
                ships.append(ship)
        else:
            # Write the SQL query to get the basic information
            db_cursor.execute("""
            SELECT
                s.id,
                s.name,
                s.hauler_id
            FROM Ship s
            """)
            query_results = db_cursor.fetchall()
            for row in query_results:
                ships.append(dict(row))

        # Serialize Python list to JSON encoded string
        serialized_ships = json.dumps(ships)

    return serialized_ships


def retrieve_ship(pk, url):
    # Open a connection to the database
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        if '_expand' in url['query_params']:
            # Write the SQL query to get the expanded information
            db_cursor.execute("""
            SELECT
                s.id,
                s.name,
                s.hauler_id,
                h.id AS haulerId,
                h.name AS haulerName,
                h.dock_id
            FROM Ship s
            JOIN Hauler h
                ON h.id = s.hauler_id
            WHERE s.id = ?
            """, (pk,))
            row = db_cursor.fetchone()
            if row:
                hauler = {
                    "id": row['haulerId'],
                    "name": row['haulerName'],
                    "dock_id": row["dock_id"]
                }
                ship = {
                    "id": row['id'],
                    "name": row['name'],
                    "hauler_id": row["hauler_id"],
                    "hauler": hauler
                }
            else:
                ship = None
        else:
            # Write the SQL query to get the basic information
            db_cursor.execute("""
            SELECT
                s.id,
                s.name,
                s.hauler_id
            FROM Ship s
            WHERE s.id = ?
            """, (pk,))
            row = db_cursor.fetchone()
            ship = dict(row) if row else None

        # Serialize Python dictionary to JSON encoded string
        serialized_ship = json.dumps(ship) if ship else json.dumps({})

    return serialized_ship

