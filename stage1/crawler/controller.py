from datamart_fill import datamart_fill
import os
import nltk
from nltk.corpus import stopwords
import extract_fetch_and_store_books

def controller(data, headers):
    """
    Controller function to control which books are downloaded 
    and which ones are indexed, creating in the directory control
    the files downloaded_books.txt and indexed_books.txt.
    """

    control_dir = 'stage1/control'
    os.makedirs(control_dir, exist_ok=True)
    downloaded_books_path = os.path.join(control_dir, 'downloaded_books.txt')
    indexed_books_path = os.path.join(control_dir, 'indexed_books.txt')
        
    nltk.download('stopwords')
    stop_words = set(stopwords.words('english'))

    book_info = extract_fetch_and_store_books.extract_fetch_and_store_books(data.decode("utf-8"), headers)
    
    with open(downloaded_books_path, 'w', encoding='utf-8') as f:
        for book in book_info:
            f.write(f"{book['id']}\n")

    datamart_fill(book_info, headers, stop_words)

    with open(indexed_books_path, 'w', encoding='utf-8') as f:
        for book in book_info:
            f.write(f"{book['id']}\n")

    print("Controller finished processing books.")
