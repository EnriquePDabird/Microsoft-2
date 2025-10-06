# Stage 1 Execution Guide:

## API

This crawler works with the free plan of the API in https://gutenbergapi.com/. Although the default key is functional, it is recommended to obtain a new key and replace it in the header dictionary of main.py

## Main and Benchmark

For initializing the program, execute main.py located on stage1/crawler, the same can be done to execute the testing benchmark in benchmark.py

## Query

The query connects to the SQLite database that stores the books' metadata, to search if a specific book exists, just write the appropiate SQL Query for the book you're searching for, for example, if you're searching for The Great Gatsby:

cursor.execute("""
    SELECT id, title, authors
    FROM books
    WHERE title LIKE '%The Great Gatsby%'
""")

It will return:

(64317, 'The Great Gatsby', 'Fitzgerald, F. Scott (Francis Scott)')
