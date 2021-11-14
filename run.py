import importlib

extract_traces = importlib.import_module("extract-traces")
build_database = importlib.import_module("build-database")
database_stats = importlib.import_module("database-stats")

def main():
	extract_traces.main()
	build_database.main("original")
	build_database.main("compressed")
	orig_stats = database_stats.main("original")
	comp_stats = database_stats.main("compressed")
	database_stats.compare(orig_stats, comp_stats)

if __name__ == "__main__":
	main()