import mysql.connector

def get_mysql_connection(db_name):
	db = mysql.connector.connect(
		host="localhost",
		user="root",
		password="root",
		database=db_name
	)
	return db

def get_db_cursor(db):
	db_cursor = db.cursor()
	return db_cursor

def main(db_name):
	db = get_mysql_connection(db_name)
	db_cursor = get_db_cursor(db)
	return db, db_cursor

if __name__ == "__main__":
	main("original")