from pyspark.sql import SparkSession

def setup_environment():
    """Configura el entorno inicial"""
    spark = SparkSession.builder.getOrCreate()
    
    # Crear el catálogo
    spark.sql("CREATE CATALOG IF NOT EXISTS mi_catalogo")
    spark.sql("USE CATALOG mi_catalogo")
    
    # Crear el schema
    spark.sql("CREATE SCHEMA IF NOT EXISTS mi_schema")
    
    # Crear datos de ejemplo
    datos_ejemplo = spark.createDataFrame([
        (1, "ejemplo1"),
        (2, "ejemplo2"),
        (3, "ejemplo3")
    ], ["id", "valor"])
    
    # Guardar los datos
    datos_ejemplo.write.format("delta").mode("overwrite").save("/dbfs/FileStore/mi_proyecto/datos")
    
    return "Ambiente configurado exitosamente" 