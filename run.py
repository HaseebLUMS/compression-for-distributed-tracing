import importlib

compressed_db = "compressed"
original_db = "original"

extract_traces = importlib.import_module("extract-traces")
build_database = importlib.import_module("build-database")
database_stats = importlib.import_module("database-stats")

def main():
	#extract_traces.main()
	build_database.main(original_db)
	build_database.main(compressed_db)
	orig_stats = database_stats.main(original_db)
	comp_stats = database_stats.main(compressed_db)
	database_stats.compare(orig_stats, comp_stats)

if __name__ == "__main__":
	main()