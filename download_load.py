import requests
from bs4 import BeautifulSoup
from worker import TypeFunction, add, clear_queue, q, stop_threads


clear_queue()
def fetch_and_queue_zip_files():
    url = "https://s3.amazonaws.com/tripdata"
    response = requests.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, 'html.parser')

    keys = soup.find_all('key')

    zip_urls = [key.get_text() for key in keys if 'JC-' not in key.get_text()]

    for key in zip_urls:
        print(key)
        add((TypeFunction.EXTRACT_DATA, key))

if __name__ == '__main__':
    clear_queue()
    fetch_and_queue_zip_files()
    q.join()
    stop_threads()