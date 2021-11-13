import importlib
import json
build_mysql_connection = importlib.import_module("build-mysql-connection")
DB, DB_CURSOR = build_mysql_connection.main("original")

def get_create_table_query(table_name, fields):
    # TODO create foriegn keys
    query = "CREATE TABLE IF NOT EXISTS " + table_name + " ( "
    for field in fields:
        query += ("`" + field + "` ") 
        if field == "id":
            query += "INT NOT NULL AUTO_INCREMENT "
        else:
            query += "VARCHAR(255) "
        query += ", "
    if len(fields) > 1 and fields[0] == "id": 
        query += "PRIMARY KEY (`id`) "
    if query.endswith(", "):
        query = query[:-2]
    query = query + " )"
    return query

def create_tables():
    with open("db-schema.json") as f:
        schema = json.load(f)
        table_names = schema["tableNames"]
        for table_name in table_names:
            fields = schema["tableData"][table_name]
            query = get_create_table_query(table_name, fields)
            print("Executing: " + query)
            DB_CURSOR.execute(query)

def populate_tables():
    
    pass

def main():
    create_tables()
    populate_tables()

if __name__ == "__main__":
	main()