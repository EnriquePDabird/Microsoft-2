import json

def extract_book_info(data):
    """
    Parse the API response data (assumed to be JSON) and extract book metadata.

    Args:
        data (str): JSON string from the API response.

    Returns:
        list: List of dictionaries containing book metadata.
    """
    books = json.loads(data)

    if isinstance(books, dict) and 'results' in books:
        books = books['results']

    book_info = []
    for book in books: 
        book_id = book.get('id')
        title = book.get('title')
        authors = [author.get('name') for author in book.get('authors', [])]
        subjects = book.get('subjects', [])
        bookshelves = book.get('bookshelves', [])
        book_info.append({
            'id': book_id,
            'title': title,
            'authors': authors,
            'subjects': subjects,
            'bookshelves': bookshelves
        })
    return book_info
