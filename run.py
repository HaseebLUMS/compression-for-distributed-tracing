import importlib
import json

compressed_db = "compressed"
original_db = "original"

extract_traces = importlib.import_module("extract-traces")
build_database = importlib.import_module("build-database")
database_stats = importlib.import_module("database-stats")
def write_stats(stats, filename):
	with open(filename, "w") as f:
		f.write(json.dumps(stats, indent=4))
	print(filename + " written!")

def main():
	# extract_traces.main()
	for num_traces in [50, 100, 150, 200, 250, 300, 350]:
		time_taken_by_original_db = build_database.main(original_db, num_traces)
		time_taken_by_compressed_db = build_database.main(compressed_db, num_traces)
		stats = database_stats.main(original_db, compressed_db, time_taken_by_original_db, time_taken_by_compressed_db)
		write_stats(stats, "results/"+str(num_traces)+".json")

if __name__ == "__main__":
	main()