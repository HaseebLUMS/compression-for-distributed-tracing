import importlib
import glob
import json
from time import perf_counter
from typing import Tuple, Union


TRACE_DIR = "./traces"
compressed_db = "compressed"
original_db = "original"
db = compressed_db

build_mysql_connection = importlib.import_module("build-mysql-connection")
DB, DB_CURSOR = (None, None)

def get_create_table_query(table_name, fields):
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
            DB_CURSOR.execute(query)

def get_id_if_field_already_inserted(field) -> Tuple[Union[int, None], bool]:
    if db != compressed_db:
        return None, False
    DB_CURSOR.execute(
        "SELECT `id` FROM `field` WHERE `key` = %s and `value` = %s",
        (field["key"], field["value"])
    )
    res = DB_CURSOR.fetchone()
    if res is not None:
        return res[0], True
    return None, False

def get_id_if_log_already_inserted(inserted_fields) -> Tuple[Union[int, None], bool]:
    if db != compressed_db:
        return None, False
    inserted_fields = ",".join(sorted([str(x) for x in inserted_fields]))

    DB_CURSOR.execute(
        "SELECT `logId` from `logHasField` GROUP BY `logId` HAVING GROUP_CONCAT(DISTINCT fieldId ORDER BY fieldId) = %s",
        (inserted_fields,)
    )

    res = DB_CURSOR.fetchone()
    if res is not None:
        return res[0], True
    return None, False

def get_id_if_span_already_inserted(inserted_logs) -> Tuple[Union[int, None], bool]:
    if db != compressed_db:
        return None, False
    inserted_logs = ",".join(sorted([str(x) for x in inserted_logs]))

    DB_CURSOR.execute(
        "SELECT `spanId` from `spanHasLog` GROUP BY `spanId` HAVING GROUP_CONCAT(DISTINCT `logId` ORDER BY `logId`) = %s",
        (inserted_logs,)
    )

    res = DB_CURSOR.fetchone()
    if res is not None:
        return res[0], True
    return None, False

def get_id_if_trace_already_inserted(inserted_spans) -> Tuple[Union[int, None], bool]:
    if db != compressed_db:
        return None, False
    inserted_spans = ",".join(sorted([str(x) for x in inserted_spans]))

    DB_CURSOR.execute(
        "SELECT `traceId` from `traceHasSpan` GROUP BY `traceId` HAVING GROUP_CONCAT(DISTINCT `spanId` ORDER BY `spanId`) = %s",
        (inserted_spans,)
    )

    res = DB_CURSOR.fetchone()
    if res is not None:
        return res[0], True
    return None, False

def insert_field_in_db(field) -> int:
    res = get_id_if_field_already_inserted(field)
    if res[1] == False:
        DB_CURSOR.execute(
            "INSERT INTO `field` (`key`, `value`) VALUES (%s, %s)",
            (field["key"], field["value"])
        )
        DB.commit()
        return get_last_insert_id()
    else:
        return res[0]

def insert_log_in_db(log) -> int:
    inserted_fields = []
    for field in log["fields"]:
        inserted_fields += [insert_field_in_db(field)]
    inserted_fields = list(set([str(x) for x in inserted_fields]))
    
    res = get_id_if_log_already_inserted(inserted_fields)
    if res[1] == False:
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
        DB.commit()
        return id
    else:
        return res[0]

def insert_span_in_db(span) -> int:
    inserted_logs = []
    for log in span["logs"]:
        inserted_logs += [insert_log_in_db(log)]
    inserted_logs = list(set([str(x) for x in inserted_logs]))

    res = get_id_if_span_already_inserted(inserted_logs)
    if res[1] ==  False:
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
        DB.commit()
        return id
    else:
        return res[0]

def insert_trace_in_db(trace) -> int:
    inserted_spans = []
    for span in trace["spans"]:
        inserted_spans += [insert_span_in_db(span)]
    inserted_spans = list(set([str(x) for x in inserted_spans]))

    res = get_id_if_trace_already_inserted(inserted_spans)
    if res[1] == False:
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
        DB.commit()
        return id
    else:
        return res[0]

def populate_tables(traces_dir, num_traces=0):
    files = glob.glob(traces_dir + "/*.json")
    if num_traces > 0:
        files = files[:num_traces]
    
    for count, file in enumerate(files):
        with open(file) as f:
            traces = json.load(f)
            for trace in traces["data"]:
                insert_trace_in_db(trace)
        if ((count % 10 == 0) or (count == len(files) - 1)):
            print("Completed " + str(count) + " files.")

def clear_database(db):
    DB_CURSOR.execute("SELECT CONCAT('DROP TABLE IF EXISTS `', table_name, '`;') \
        FROM information_schema.tables \
        WHERE table_schema = '"+db+"';"
    )
    drop_queries = DB_CURSOR.fetchall()
    for query in drop_queries:
        DB_CURSOR.execute(query[0])
    DB.commit()
    
def main(db_type, num_traces=0):
    global db
    global DB
    global DB_CURSOR

    db = db_type
    DB, DB_CURSOR = build_mysql_connection.main(db)
    
    print("Building database: " + db_type)
    clear_database(db)
    create_tables()
    start_time = perf_counter()
    populate_tables(TRACE_DIR, num_traces)
    end_time = perf_counter()
    print("===== Completed " + db + " =====")
    return end_time - start_time

if __name__ == "__main__":
	main("original")