import importlib
import json

compressed_db = "compressed"
original_db = "original"
num_traces = 200

extract_traces = importlib.import_module("extract-traces")
build_database = importlib.import_module("build-database")
database_stats = importlib.import_module("database-stats")
def write_stats(stats, filename):
	with open(filename, "w") as f:
		f.write(json.dumps(stats, indent=4))
	print(filename + " written!")

def main():
	# extract_traces.main()
	build_database.main(original_db, num_traces)
	build_database.main(compressed_db, num_traces)
	stats = database_stats.main(original_db, compressed_db)
	write_stats(stats, "results/"+str(num_traces)+".json")

if __name__ == "__main__":
	main()