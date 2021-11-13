import importlib
import json
import glob
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

def insert_field_in_db(field) -> int:
    pass
    return 0

def insert_log_in_db(log) -> int:
    inserted_fields = []
    for field in log["fields"]:
        inserted_fields += [insert_field_in_db(field)]
    return 0

def insert_span_in_db(span) -> int:
    inserted_logs = []
    for log in span["logs"]:
        inserted_logs += [insert_log_in_db(log)]
    return 0

def insert_trace_in_db(trace) -> int:
    inserted_spans = []
    for span in trace["spans"]:
        inserted_spans += [insert_span_in_db(span)]
    return 0

def populate_tables(traces_dir):
    files = glob.glob(traces_dir + "/*.json")
    for file in files:
        with open(file) as f:
            traces = json.load(f)
            for trace in traces["data"]:
                id  = insert_trace_in_db(trace)
                print("Inserted trace with id " + str(id))
        break
def main():
    # create_tables()
    populate_tables("./traces")

if __name__ == "__main__":
	main()