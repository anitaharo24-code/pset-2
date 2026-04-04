if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from mage_ai.data_preparation.shared.secrets import get_secret_value
from sqlalchemy import create_engine, text
import pandas as pd
import requests
import pyarrow.parquet as pq
import traceback
import gc
import os


@data_exporter
def export_data_to_postgres(df_urls, **kwargs) -> None:

    user     = get_secret_value('pg_user')
    password = get_secret_value('pg_password')
    host     = get_secret_value('pg_host')
    port     = get_secret_value('pg_port')
    db       = get_secret_value('pg_db')

    engine = create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{db}",
        pool_pre_ping=True
    )

    # 1. Obtener meses del nuevo dataset
    nuevos = set(zip(df_urls['year'].astype(int), 
                     df_urls['month'].astype(int)))

    # 2. Obtener meses existentes en PostgreSQL
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT DISTINCT raw_year, raw_month 
                FROM raw.ny_taxi
            """))
            existentes = set((int(row[0]), int(row[1])) for row in result)
    except:
        existentes = set()
        print("Tabla vacía o no existe. Cargando todo.")

    # 3. Borrar meses que ya no están en el nuevo dataset
    a_borrar = existentes - nuevos
    if a_borrar:
        print(f"\nBorrando {len(a_borrar)} meses que no están en el nuevo dataset...")
        for year_del, month_del in a_borrar:
            with engine.begin() as conn:
                conn.execute(text("""
                    DELETE FROM raw.ny_taxi 
                    WHERE raw_year = :year 
                    AND raw_month = :month
                """), {'year': year_del, 'month': month_del})
            print(f"  Borrado: {year_del}-{month_del:02d}")

    exitosos = 0
    fallidos = []
    tmp_file  = "/tmp/tmp_taxi.parquet"

    for _, row in df_urls.iterrows():
        year  = int(row['year'])
        month = int(row['month'])
        url   = row['url']

        # 4. Saltar si ya existe
        if (year, month) in existentes:
            print(f"  {year}-{month:02d} ya existe. Saltando...")
            exitosos += 1
            continue

        print(f"\nProcesando: {year}-{month:02d}...")

        try:
            # Descargar a disco
            print(f"  Descargando a disco...")
            with requests.get(url, stream=True, timeout=300) as r:
                if r.status_code == 404:
                    print(f"  Archivo no encontrado: {year}-{month:02d}")
                    fallidos.append({'year': year, 'month': month, 'error': '404'})
                    continue
                r.raise_for_status()
                with open(tmp_file, 'wb') as f:
                    for chunk_bytes in r.iter_content(chunk_size=8192):
                        f.write(chunk_bytes)

            print(f"  Descargado a disco.")

            # Leer en chunks
            parquet_file = pq.ParquetFile(tmp_file)
            chunk_num    = 0

            for batch in parquet_file.iter_batches(batch_size=50_000):
                chunk = batch.to_pandas()

                chunk.columns = [col.lower() for col in chunk.columns]
                chunk.rename(columns={
                    'vendorid':     'vendor_id',
                    'ratecodeid':   'rate_code_id',
                    'pulocationid': 'pu_location_id',
                    'dolocationid': 'do_location_id'
                }, inplace=True)

                chunk['raw_year']       = year
                chunk['raw_month']      = month
                chunk['raw_source_url'] = url

                chunk.to_sql(
                    name='ny_taxi',
                    schema='raw',
                    con=engine,
                    if_exists='append',
                    index=False,
                    method='multi'
                )

                chunk_num += 1
                print(f"  Chunk {chunk_num} cargado ({len(chunk):,} filas)")

                del chunk
                gc.collect()

            print(f"  Mes {year}-{month:02d} completo.")
            exitosos += 1

        except Exception as e:
            print(f"  ERROR en {year}-{month:02d}: {e}")
            print(traceback.format_exc())
            fallidos.append({'year': year, 'month': month, 'error': str(e)})

        finally:
            if os.path.exists(tmp_file):
                os.remove(tmp_file)
            gc.collect()

    engine.dispose()

    print(f"\n{'='*40}")
    print(f"Exitosos: {exitosos}")
    print(f"Fallidos: {len(fallidos)}")
    if fallidos:
        for f in fallidos:
            print(f"  {f['year']}-{f['month']:02d}: {f['error']}")