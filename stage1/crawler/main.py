import requests
from controller import controller

def main(url = "https://project-gutenberg-free-books-api1.p.rapidapi.com/books"):
    headers = {
        'x-rapidapi-key': "f38981c70cmsh4f5bf253e75dad5p1b6022jsn0f1975492054",
        'x-rapidapi-host': "project-gutenberg-free-books-api1.p.rapidapi.com"
        }

    response = requests.get(url, headers=headers)
    print(response.content)
    controller(response.content, headers)

    print("Page processed, moving to next page if available...")
    url = response.json().get('next')
    if url == "https://project-gutenberg-free-books-api1.p.rapidapi.com/books?page=5":
        return "First 5 pages done"
    else:
        main(url)

main()