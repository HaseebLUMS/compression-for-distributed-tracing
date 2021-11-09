import requests
import json
import os
from pathlib import Path
from hurry.filesize import size

LIMIT_TRACES="200"
API_ENDPOINT = "http://127.0.0.1:16686/api/traces"
ALL_TRACES_ENDPOINT = API_ENDPOINT + "?limit="+ LIMIT_TRACES +"&service=customer"
TRACES_DIR = "./traces"

def get_all_trace_ids(endpoint) -> list:
    res = requests.get(endpoint)
    if res.status_code != 200: 
        raise Exception("Error in getting trace ids.")
    res = res.json()
    return [x["traceID"] for x in res["data"]]

def fetch_and_store_traces(trace_ids, dir_to_store_jsons, endpoint):
    if not os.path.exists(dir_to_store_jsons):
        os.makedirs(dir_to_store_jsons)
    print("Writing files:", end="")
    for index, trace_id in enumerate(trace_ids):
        filename = dir_to_store_jsons + "/" + trace_id + ".json"
        if not os.path.exists(filename):
            res = requests.get(endpoint + "/" + trace_id)
            if res.status_code != 200: 
                raise Exception("Error in getting trace data for trace id:" + trace_id)
            res = res.json()
            with open(dir_to_store_jsons + "/" + trace_id + ".json", "x") as f:
                json.dump(res, f, ensure_ascii=False, indent=4)
        print(str(index + 1) + ",", end="")
    print("DONE.")

def show_stats(traces_dir):
    print(
        "Total Traces:",
        len([name for name in os.listdir(traces_dir)])
    )
    print(
        "Total Size: ",
        size(
            sum(f.stat().st_size for f in Path(traces_dir).glob('**/*') if f.is_file())
        )
    )

def main():
    trace_ids = get_all_trace_ids(endpoint=ALL_TRACES_ENDPOINT)
    fetch_and_store_traces(trace_ids=trace_ids, dir_to_store_jsons=TRACES_DIR, endpoint=API_ENDPOINT)
    show_stats(traces_dir=TRACES_DIR)

if __name__ == "__main__":
    main()