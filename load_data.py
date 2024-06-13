import requests
from bs4 import BeautifulSoup

def get_zip_links(url):
    try:
        response = requests.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')
        print(soup.prettify())

        links = [a['href'] for a in soup.find_all('a', href=True) if a['href'].endswith('.zip')]
        full_links = ["https://s3.amazonaws.com/tripdata/" + link for link in links]
        print("Extracted links:", full_links)
        return full_links
    except requests.RequestException as e:
        print(f"Failed to fetch links from {url}: {e}")
        return []

base_url = 'https://s3.amazonaws.com/tripdata/index.html'

zip_links = get_zip_links(base_url)
print(zip_links) 
