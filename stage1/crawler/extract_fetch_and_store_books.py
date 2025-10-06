import requests
from datetime import datetime
import json
import os
from extract_book_info import extract_book_info

def extract_fetch_and_store_books(data, headers, datalake_dir='stage1/datalake'):
    """
    Extract book metadata, fetch book texts, and store metadata in a structured datalake directory.

    Args:
        data (str): JSON string from the API response.
        headers (dict): Headers for the API request.
        datalake_dir (str): Root directory for storing data.
    Returns:
        list: List of book metadata dictionaries.
    """
    book_info = extract_book_info(data)

    now = datetime.now()
    date_dir = now.strftime('%Y-%m-%d')
    time_dir = now.strftime('%H')
    full_path = os.path.join(datalake_dir, date_dir, time_dir)
    os.makedirs(full_path, exist_ok=True)

    control_dir = 'control'
    downloaded_books_path = os.path.join(control_dir, 'downloaded_books.txt')
    indexed_books_path = os.path.join(control_dir, 'indexed_books.txt')

    downloaded_books = set()
    if os.path.exists(downloaded_books_path):
        with open(downloaded_books_path, 'r', encoding='utf-8') as f:
            downloaded_books = set(line.strip() for line in f if line.strip())

    indexed_books = set()
    if os.path.exists(indexed_books_path):
        with open(indexed_books_path, 'r', encoding='utf-8') as f:
            indexed_books = set(line.strip() for line in f if line.strip())

    for book in book_info:
        book_id = str(book['id'])
        if book_id in downloaded_books and book_id in indexed_books:
            continue 

        header_path = os.path.join(full_path, f"{book_id}.header.txt")
        with open(header_path, 'w', encoding='utf-8') as header_file:
            json.dump(book, header_file, ensure_ascii=False, indent=4)
        response = requests.get(f"https://project-gutenberg-free-books-api1.p.rapidapi.com/books/{book_id}/text?cleaning_mode=simple", headers=headers)
        text = response.text
        book_text = json.loads(text).get('text', '')
        body_path = os.path.join(full_path, f"{book_id}.body.txt")
        with open(body_path, 'w', encoding='utf-8') as body_file:
            body_file.write(book_text)

    print("Books downloaded and stored in datalake.")
    return book_info
