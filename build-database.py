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

def get_last_insert_id():
    return DB_CURSOR.lastrowid

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
    DB_CURSOR.execute(
        "INSERT INTO `field` (`key`, `value`) VALUES (%s, %s)",
        (field["key"], field["value"])
    )
    return get_last_insert_id()

def insert_log_in_db(log) -> int:
    inserted_fields = []
    for field in log["fields"]:
        inserted_fields += [insert_field_in_db(field)]
    DB_CURSOR.execute(
        "INSERT INTO `log` (`totalFields`) VALUES (%s)",
        (len(inserted_fields),)
    )
    id = get_last_insert_id()
    for fieldId in inserted_fields:
        DB_CURSOR.execute(
            "INSERT INTO `logHasField` (`logId`, `fieldId`) VALUES (%s, %s)",
            (id, fieldId)
        )
    return id

def insert_span_in_db(span) -> int:
    inserted_logs = []
    for log in span["logs"]:
        inserted_logs += [insert_log_in_db(log)]
    DB_CURSOR.execute(
        "INSERT INTO `span` (`operationName`, `totalLogs`) VALUES (%s, %s)",
        (span["operationName"], len(inserted_logs))
    )
    id = get_last_insert_id()
    for logId in inserted_logs:
        DB_CURSOR.execute(
            "INSERT INTO `spanHasLog` (`spanId`, `logId`) VALUES (%s, %s)",
            (id, logId)
        )
    return id

def insert_trace_in_db(trace) -> int:
    inserted_spans = []
    for span in trace["spans"]:
        inserted_spans += [insert_span_in_db(span)]
    DB_CURSOR.execute(
        "INSERT INTO `trace` (`totalSpans`) VALUES (%s)",
        (len(inserted_spans),)
    )
    id = get_last_insert_id()
    for spanId in inserted_spans:
        DB_CURSOR.execute(
            "INSERT INTO `traceHasSpan` (`traceId`, `spanId`) VALUES (%s, %s)",
            (id, spanId)
        )
    return id

def populate_tables(traces_dir):
    files = glob.glob(traces_dir + "/*.json")
    for count, file in enumerate(files):
        with open(file) as f:
            traces = json.load(f)
            for trace in traces["data"]:
                id  = insert_trace_in_db(trace)
        if ((count % 10 == 0) or (count == len(files) - 1)):
            #DB.commit()
            print("Committing " + str(count) + " files.")

def main():
    create_tables()
    populate_tables("./traces")

if __name__ == "__main__":
	main()