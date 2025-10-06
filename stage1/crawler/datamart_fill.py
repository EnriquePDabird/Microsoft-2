import json
import os
import sqlite3
from inverted_index_creation import inverted_index_creation

def datamart_fill(book_info, headers, stop_words):
    """
    This function creates a directory named 'datamart', if it doesn't already exist, 
    and processes the data stored in the 'datalake' directory. Inside datamart create two files,
    unless they already exist: metadata.sql and inverted_index.json

    Args:
        book_info (list): List of book metadata dictionaries.
        headers (dict): Headers for the API request.
        stop_words (set): Set of stop words to exclude from the index.
    """
    datamart_dir = 'stage1/datamart'
    os.makedirs(datamart_dir, exist_ok=True)

    metadata_path = os.path.join(datamart_dir, 'metadata.sql')
    inverted_index_path = os.path.join(datamart_dir, 'inverted_index.json')
    inverted_index = inverted_index_creation(book_info, headers, stop_words)
    
    datalake_dir = 'datalake'
    metadata = []

    for root, _, files in os.walk(datalake_dir):
        for file in files:
            if file.endswith('.header.txt'):
                with open(os.path.join(root, file), 'r', encoding='utf-8') as f:
                    book_metadata = json.load(f)
                    metadata.append(book_metadata)

    if not os.path.exists(inverted_index_path):
        with open(inverted_index_path, 'w', encoding='utf-8') as index_file:
            json.dump(inverted_index, index_file, ensure_ascii=False, indent=4)
    else:
        with open(inverted_index_path, 'r+', encoding='utf-8') as index_file:
            existing_index = json.load(index_file)
            existing_index.update(inverted_index) 
            index_file.seek(0)
            json.dump(existing_index, index_file, ensure_ascii=False, indent=4)
            index_file.truncate()
    
    if not os.path.exists(metadata_path):
        db_path = os.path.join(datamart_dir, 'metadata.db')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS books (
            id INTEGER PRIMARY KEY,
            title TEXT,
            authors TEXT,
            subjects TEXT,
            bookshelves TEXT
            );
            """
        )
        for book in metadata:
            authors = ', '.join(book.get('authors', []))
            subjects = ', '.join(book.get('subjects', []))
            bookshelves = ', '.join(book.get('bookshelves', []))
            title = book.get('title', '')
            cursor.execute(
            """
            INSERT OR IGNORE INTO books (id, title, authors, subjects, bookshelves)
            VALUES (?, ?, ?, ?, ?)
            """,
            (book.get('id'), title, authors, subjects, bookshelves)
            )
        conn.commit()
        conn.close()
    print("Datamart filled with metadata and inverted index.")
