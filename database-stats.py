import importlib
build_mysql_connection = importlib.import_module("build-mysql-connection")
DB, DB_CURSOR = None, None

def compare(original, compressed):
    return 0

def main(db_type):
    global DB
    global DB_CURSOR
    DB, DB_CURSOR = build_mysql_connection.main(db_type)
    return {}

if __name__ == "__main__":
	main("original")