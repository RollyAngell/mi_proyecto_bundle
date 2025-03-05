from pyspark.sql import SparkSession

def leer_datos():
    """Lee los datos del asset bundle"""
    spark = SparkSession.builder.getOrCreate()
    try:
        df = spark.table("mi_catalogo.mi_schema.tabla_ejemplo")
        return df
    except Exception as e:
        print(f"Error al leer datos: {str(e)}")
        return None

def limpiar_recursos():
    """Limpia todos los recursos creados"""
    spark = SparkSession.builder.getOrCreate()
    spark.sql("DROP TABLE IF EXISTS mi_catalogo.mi_schema.tabla_ejemplo")
    spark.sql("DROP SCHEMA IF EXISTS mi_catalogo.mi_schema")
    spark.sql("DROP CATALOG IF EXISTS mi_catalogo")
    return "Recursos limpiados exitosamente" 