# Simple web crawler

Reads website pages and saves the data in an SQLite database.

Needs a list of URLs to start with.
Never gets outside of the given websites.
Goes as deep as you specify, e.g., up to 2 clicks from the main page.
Runs 5 threads by default.
Produces a log file with status codes.
Can be stopped, reconfigured and restarted at any time, will continue.

## TODO

- MySQL connector.  SQLite updates take up to 1 second at times.
- Don't access database from the web worker threads, use a dedicated database thread and a queue.

## Other

License: public domain.

2019, Justin Forest <hex@umonkey.net>
