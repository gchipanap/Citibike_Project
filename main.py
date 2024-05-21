import os
import zipfile
from download_load import load_data
from pyspark import SparkContext, SparkConf

'''
$env:JAVA_HOME="C:\jdk-22_windows-x64_bin\jdk-22.0.1"
$env:HADOOP_HOME="C:\hadoop-3.4.0-src\hadoop-3.4.0-src"
$env:SPARK_HOME="C:\spark-3.5.1-bin-hadoop3\spark-3.5.1-bin-hadoop3"
$env:PATH="$env:SPARK_HOME\bin;$env:JAVA_HOME\bin;$env:PATH"

pyspark --version

'''

def main():
    conf = SparkConf().setAppName("UnzipFiles").setMaster("local[*]")  # Usa todos los núcleos disponibles
    sc = SparkContext(conf=conf)
    #spark = SparkSession.builder.appName("Datacamp Pyspark Tutorial").config("spark.memory.offHeap.enabled","true").config("spark.memory.offHeap.size","10g").getOrCreate()

    zip_dir = 'static/data/zips'
    extract_to_dir = 'static/data/files'

    os.makedirs(zip_dir, exist_ok=True)
    os.makedirs(extract_to_dir, exist_ok=True)

    zip_files = [os.path.join(zip_dir, f) for f in os.listdir(zip_dir) if f.endswith('.zip')]
    print(zip_files)

    zip_files_rdd = sc.parallelize(zip_files)
    results = zip_files_rdd.map(lambda zip_file: load_data(zip_file, extract_to_dir)).collect()

    for result in results:
        if result[1]:
            print(f"Successfully unzipped: {result[0]}")
        else:
            print(f"Failed to unzip: {result[0]} - Error: {result[2]}")

if __name__ == "__main__":
    main()