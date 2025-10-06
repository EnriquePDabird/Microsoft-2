import requests
import json
from collections import defaultdict

def inverted_index_creation(book_info, headers, stop_words):
    """
    For each book in book_info, fetch the text, print a sample, and build an inverted index (excluding stop words).

    Args:
        book_info (list): List of book metadata dictionaries.
        headers (dict): Headers for the API request.
        stop_words (set): Set of stop words to exclude from the index.

    Returns:
        dict: Inverted index mapping words to lists of book IDs.
    """
    inverted_index = defaultdict(lambda: defaultdict(int))

    for book in book_info:
        book_id = book['id']
        response = requests.get(
            f"https://project-gutenberg-free-books-api1.p.rapidapi.com/books/{book_id}/text?cleaning_mode=simple", 
            headers=headers
        )
        text = response.text
        books = json.loads(text)

        if isinstance(books, dict) and 'text' in books:
            text = books['text']

        for word in text.split():
            word = word.lower().strip('.,!?;"()[]{}')
            if word and word not in stop_words:
                inverted_index[word][book_id] += 1

    return inverted_index
