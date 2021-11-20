import importlib
build_mysql_connection = importlib.import_module("build-mysql-connection")
O_DB, O_DB_CURSOR = None, None
C_DB, C_DB_CURSOR = None, None

def get_fields(db, db_cursor):
    db_cursor.execute("select count(*) from `field`")
    res = db_cursor.fetchone()
    db.commit()
    return res[0]

def get_spans(db, db_cursor):
    db_cursor.execute("select count(*) from `span`")
    res = db_cursor.fetchone()
    db.commit()
    return res[0]

def get_size(db, db_cursor, db_name):
    db_cursor.execute("SELECT * from \
        (SELECT table_schema AS 'Database', \
        CAST(ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS CHAR) AS 'Size (MB)' \
        FROM information_schema.TABLES \
        GROUP BY table_schema) as res \
        WHERE res.Database = '"+ db_name +"'"
    )
    res = db_cursor.fetchone()
    db.commit()
    return res[1]

def compare(original, compressed):
    res = {
        "fields": {
            "o": 0,
            "c": 0
        },
        "spans": {
            "o": 0,
            "c": 0
        },
        "size": {
            "o": 0,
            "c": 0
        }
    }

    res["fields"]["o"] = get_fields(O_DB, O_DB_CURSOR)
    res["fields"]["c"] = get_fields(C_DB, C_DB_CURSOR)
    res["spans"]["o"] = get_spans(O_DB, O_DB_CURSOR)
    res["spans"]["c"] = get_spans(C_DB, C_DB_CURSOR)
    res["size"]["o"] = float(get_size(O_DB, O_DB_CURSOR, original))
    res["size"]["c"] = float(get_size(C_DB, C_DB_CURSOR, compressed))
    return res

def main(original_db, compressed_db):
    global O_DB
    global C_DB
    global O_DB_CURSOR
    global C_DB_CURSOR

    O_DB, O_DB_CURSOR = build_mysql_connection.main(original_db)
    C_DB, C_DB_CURSOR = build_mysql_connection.main(compressed_db)
    return compare(original_db, compressed_db)

if __name__ == "__main__":
	main("original")