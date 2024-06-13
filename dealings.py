import requests
import zipfile
import io

url = "2013-citibike-tripdata.zip"
def extract_data_from_zip(url):
    response = requests.get("https://s3.amazonaws.com/tripdata/"+url)
    response.raise_for_status()

    with io.BytesIO(response.content) as zip_file:
        with zipfile.ZipFile(zip_file) as z:
            z.extractall("data")

