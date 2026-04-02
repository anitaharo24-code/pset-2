if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from mage_ai.data_preparation.shared.secrets import get_secret_value
from sqlalchemy import create_engine
from io import BytesIO
import pandas as pd
import requests
import gc


@data_exporter
def export_data_to_postgres(df_urls, **kwargs) -> None:
    """
    Descarga y carga cada mes de NY Taxi a PostgreSQL.
    Procesa un mes a la vez para no acumular memoria.
    """
    user     = get_secret_value('pg_user')
    password = get_secret_value('pg_password')
    host     = get_secret_value('pg_host')
    port     = get_secret_value('pg_port')
    db       = get_secret_value('pg_db')

    engine = create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{db}",
        pool_pre_ping=True
    )

    exitosos = 0
    fallidos = []

    for _, row in df_urls.iterrows():
        year  = row['year']
        month = row['month']
        url   = row['url']

        print(f"\nProcesando: {year}-{month:02d}...")

        try:
            # 1. Descargar el archivo
            response = requests.get(url, timeout=120)

            if response.status_code == 404:
                print(f"  Archivo no encontrado: {year}-{month:02d}")
                fallidos.append({'year': year, 'month': month, 'error': '404'})
                continue

            response.raise_for_status()

            # 2. Leer parquet desde bytes
            df = pd.read_parquet(BytesIO(response.content))
            print(f"  Descargado: {len(df):,} filas")

            # 3. Transformaciones mínimas para capa raw
            df.columns = [col.lower() for col in df.columns]

            # Renombrar columnas clave
            df.rename(columns={
                'vendorid':     'vendor_id',
                'ratecodeid':   'rate_code_id',
                'pulocationid': 'pu_location_id',
                'dolocationid': 'do_location_id'
            }, inplace=True)

            # Columnas de trazabilidad
            df['raw_year']       = year
            df['raw_month']      = month
            df['raw_source_url'] = url

            # 4. Cargar a PostgreSQL en lotes de 50K filas
            df.to_sql(
                name='ny_taxi',
                schema='raw',
                con=engine,
                if_exists='append',
                index=False,
                chunksize=50_000,
                method='multi'
            )

            print(f"  Cargado correctamente.")
            exitosos += 1

        except Exception as e:
            print(f"  ERROR en {year}-{month:02d}: {e}")
            fallidos.append({'year': year, 'month': month, 'error': str(e)})

        finally:
            # 5. Liberar memoria antes del siguiente mes
            try:
                del df
                del response
            except:
                pass
            gc.collect()

    engine.dispose()

    print(f"\n{'='*40}")
    print(f"Exitosos: {exitosos}")
    print(f"Fallidos: {len(fallidos)}")
    if fallidos:
        for f in fallidos:
            print(f"  {f['year']}-{f['month']:02d}: {f['error']}")