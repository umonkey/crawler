database:
	sqlite3 spider.db < spider.sql

run:
	python spider.py

clean:
	rm -f spider.log spider.db
