import requests
import zipfile
import xml.etree.ElementTree as ET
import threading
import os

base_citibike_url = 'https://s3.amazonaws.com/tripdata'
download_path = 'static/data/zips'
unzip_path = 'static/data/files'

os.makedirs(download_path, exist_ok=True)
os.makedirs(unzip_path, exist_ok=True)

def download_zip_file(url, filepath):
    filename = os.path.join(filepath, url.split('/')[-1])
    try:
        response = requests.get(url)
        with open(filename, 'wb') as f:
            f.write(response.content)
        print(f"Archivo descargado: {filename}")
    except Exception as e:
        print(f"Error al descargar {url}: {e}")

def get_zip_links(url):
    try:
        response = requests.get(url)
        response.raise_for_status()

        root = ET.fromstring(response.content)
        links = []
        for contents in root.findall('{http://s3.amazonaws.com/doc/2006-03-01/}Contents'):
            key = contents.find('{http://s3.amazonaws.com/doc/2006-03-01/}Key').text
            if key.endswith('.zip'):
                links.append(f"https://s3.amazonaws.com/tripdata/{key}")
        print("Extracted links:", links)
        return links
    except requests.RequestException as e:
        print(f"Failed to fetch links from {url}: {e}")
        return []


def download_files_in_batches(links, filepath, batch_size=10):
    for i in range(0, len(links), batch_size):
        print("Descargando lote", i)
        batch_links = links[i:i+batch_size]
        threads = [threading.Thread(target=download_zip_file, args=(link, filepath)) for link in batch_links]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()


def download_files(url, path):
    zip_links = get_zip_links(url)
    if zip_links:
        download_files_in_batches(zip_links, path)
    else:
        print("No se encontraron enlaces a archivos ZIP.")

def unzip_files(path_zip, path_unzip):
    try:
        with zipfile.ZipFile(path_zip, 'r') as zip_ref:
            zip_ref.extractall(path_unzip)
        return (path_zip, True)
    except Exception as e:
        return (path_zip, False, str(e))

def load_data(download_zip_url, path_zip, path_unzip):
    #download_files(download_zip_url, path_zip)
    unzip_files(path_zip, path_unzip)
