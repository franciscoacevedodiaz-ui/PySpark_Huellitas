
import os
import traceback
import uuid
import psycopg

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from pyspark.sql.types import StringType

from datetime import datetime, timezone
from time import perf_counter


# ==========================================
# 1. FUNCION DE LOGGING
# ==========================================

def registrar_ejecucion(
    execution_id,
    start_time,
    end_time,
    duration,
    extraidos,
    transformados,
    cargados,
    duplicados,
    status,
    failed_stage=None,
    error_message=None
):
    password = os.getenv("PGPASSWORD")

    with psycopg.connect(
        host="localhost",
        port=5432,
        dbname="postgres",
        user="postgres",
        password=password
    ) as conn:

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO public.etl_logs (
                    execution_id,
                    process_name,
                    start_time,
                    end_time,
                    duration_seconds,
                    records_extracted,
                    records_transformed,
                    records_loaded,
                    duplicates_removed,
                    status,
                    failed_stage,
                    error_message
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                )
            """, (
                execution_id,
                "ETL_POSTGRESQL_CLIENTES",
                start_time,
                end_time,
                duration,
                extraidos,
                transformados,
                cargados,
                duplicados,
                status,
                failed_stage,
                error_message
            ))


# ==========================================
# 2. VARIABLES DE CONTROL
# ==========================================

execution_id = str(uuid.uuid4())

start_time = datetime.now(timezone.utc)
start_counter = perf_counter()

registros_extraidos = 0
registros_transformados = 0
registros_cargados = 0
duplicados_eliminados = 0

etapa_actual = "CONFIGURACION"
spark = None


try:

    # ==========================================
    # 3. CONFIGURAR CONEXION POSTGRESQL
    # ==========================================

    etapa_actual = "CONFIGURACION"

    password = os.getenv("PGPASSWORD")

    if not password:
        raise ValueError("No se ha configurado PGPASSWORD")

    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"

    properties = {
        "user": "postgres",
        "password": password,
        "driver": "org.postgresql.Driver"
    }

    # ==========================================
    # 4. CREAR SESION SPARK
    # ==========================================

    etapa_actual = "INICIALIZACION SPARK"

    spark = (
        SparkSession.builder
        .appName("ETL_PostgreSQL_Clientes")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.7"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("====================================")
    print("INICIANDO PROCESO ETL")
    print("Execution ID:", execution_id)
    print("Spark:", spark.version)
    print("====================================")

    # ==========================================
    # 5. EXTRACT
    # ==========================================

    etapa_actual = "EXTRACT"

    print("\n[EXTRACT] Leyendo tabla clientes...")

    df = spark.read.jdbc(
        url=jdbc_url,
        table="public.clientes",
        properties=properties
    )

    registros_extraidos = df.count()

    print("Registros extraidos:", registros_extraidos)

    print("\nEstructura de la tabla:")
    df.printSchema()

    print("\nDatos originales:")
    df.show(10, truncate=False)

    # ==========================================
    # 6. TRANSFORM
    # ==========================================

    etapa_actual = "TRANSFORM"

    print("\n[TRANSFORM] Limpiando informacion...")

    # Eliminar espacios al inicio y al final

    for field in df.schema.fields:

        if isinstance(field.dataType, StringType):

            df = df.withColumn(
                field.name,
                trim(col(field.name))
            )

    # Eliminar registros completamente duplicados

    df = df.dropDuplicates()

    registros_transformados = df.count()

    duplicados_eliminados = (
        registros_extraidos - registros_transformados
    )

    print("Registros originales:", registros_extraidos)
    print("Registros despues de limpieza:", registros_transformados)
    print("Duplicados eliminados:", duplicados_eliminados)

    # ==========================================
    # 7. LOAD
    # ==========================================

    etapa_actual = "LOAD"

    print("\n[LOAD] Guardando resultados en PostgreSQL...")

    df.write.jdbc(
        url=jdbc_url,
        table="public.clientes_procesados",
        mode="overwrite",
        properties=properties
    )

    print("Datos guardados correctamente.")

    # ==========================================
    # 8. VALIDACION
    # ==========================================

    etapa_actual = "VALIDACION"

    print("\n[VALIDACION] Verificando tabla destino...")

    df_resultado = spark.read.jdbc(
        url=jdbc_url,
        table="public.clientes_procesados",
        properties=properties
    )

    registros_cargados = df_resultado.count()

    print("Registros almacenados:", registros_cargados)

    df_resultado.show(10, truncate=False)

    # Comprobar cantidad de registros

    if registros_cargados != registros_transformados:
        raise ValueError(
            f"Error de validacion: esperados {registros_transformados}, "
            f"encontrados {registros_cargados}"
        )

    print("\nVALIDACION EXITOSA")

    # ==========================================
    # 9. LOGGING - EJECUCION EXITOSA
    # ==========================================

    etapa_actual = "LOGGING"

    end_time = datetime.now(timezone.utc)
    duration = perf_counter() - start_counter

    registrar_ejecucion(
        execution_id=execution_id,
        start_time=start_time,
        end_time=end_time,
        duration=duration,
        extraidos=registros_extraidos,
        transformados=registros_transformados,
        cargados=registros_cargados,
        duplicados=duplicados_eliminados,
        status="SUCCESS"
    )

    print("LOGGING: Ejecucion registrada correctamente.")

    print("\n====================================")
    print("PROCESO ETL FINALIZADO CORRECTAMENTE")
    print("====================================")


except Exception as e:

    print("\n====================================")
    print("ERROR EN EL PROCESO ETL")
    print("====================================")

    end_time = datetime.now(timezone.utc)
    duration = perf_counter() - start_counter

    print(f"ERROR EN ETAPA: {etapa_actual}")
    print(f"DETALLE: {str(e)}")

    try:

        registrar_ejecucion(
            execution_id=execution_id,
            start_time=start_time,
            end_time=end_time,
            duration=duration,
            extraidos=registros_extraidos,
            transformados=registros_transformados,
            cargados=registros_cargados,
            duplicados=duplicados_eliminados,
            status="FAILED",
            failed_stage=etapa_actual,
            error_message=str(e)[:5000]
        )

        print("LOGGING: Error registrado correctamente.")

    except Exception as log_error:

        print("ERROR AL REGISTRAR LOG:", str(log_error))

    traceback.print_exc()

    raise


finally:

    # ==========================================
    # 10. CIERRE SEGURO DE SPARK
    # ==========================================

    if spark is not None:

        print("\n[FINALIZACION] Cerrando Spark...")

        try:
            spark.stop()
            print("Sesion Spark finalizada.")

        except Exception as e:
            print("Advertencia al cerrar Spark:", str(e))
