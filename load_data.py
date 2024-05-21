import requests
from bs4 import BeautifulSoup

def get_zip_links(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Verificar si hay errores en la solicitud

        soup = BeautifulSoup(response.content, 'html.parser')
        # Imprimir toda la estructura HTML para analizar su contenido
        print(soup.prettify())

        # Extraer todos los enlaces que terminan en .zip
        links = [a['href'] for a in soup.find_all('a', href=True) if a['href'].endswith('.zip')]
        full_links = ["https://s3.amazonaws.com/tripdata/" + link for link in links]
        print("Extracted links:", full_links)
        return full_links
    except requests.RequestException as e:
        print(f"Failed to fetch links from {url}: {e}")
        return []

# URL base
base_url = 'https://s3.amazonaws.com/tripdata/index.html'

# Obtener todos los enlaces a archivos ZIP
zip_links = get_zip_links(base_url)
print(zip_links)  # Mostrar los enlaces obtenidos para verificar
