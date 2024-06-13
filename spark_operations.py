# from pyspark.sql import SparkSession

# def cargar_datos(spark, ruta_archivo):
#     try:
#         df = spark.read.json(ruta_archivo)
#         print("Datos cargados exitosamente")
#         return df
#     except Exception as e:
#         print('Error al cargar los datos:', e)

# def ejecutar_consulta_spark(spark, df):
#     try:
#         df.createOrReplaceTempView("tabla_json")
#         resultado = spark.sql("SELECT * FROM tabla_json")
#         return resultado
#     except Exception as e:
#         print('Error al ejecutar la consulta Spark:', e)

# if __name__ == "__main__":
#     # Inicializar SparkSession si necesitas ejecutar este script independientemente
#     spark = SparkSession.builder \
#         .appName("Operaciones con Spark") \
#         .getOrCreate()

#     # Ejemplo de uso de las funciones
#     df = cargar_datos(spark, "data/archivos_json/archivo1.json")
#     resultado = ejecutar_consulta_spark(spark, df)
#     resultado.show()

#     # Cerrar la sesión de Spark al finalizar
#     spark.stop()
