if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd
import os

def load_parquet_in_chunks(df, chunk_size=50000):
    """
    Divide un DataFrame en chunks.
    """
    for start in range(0, len(df), chunk_size):
        yield df.iloc[start:start + chunk_size]



@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    year = 2025
    months = range(1, 13)  # Enero y Febrero como prueba

    base_columns = None
    all_chunks = []

    for month in months:
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet'
# Leer parquet directamente desde la URL
        df = pd.read_parquet(url)

        # Definir columnas base en el primer mes
        if base_columns is None:
            base_columns = df.columns.tolist()
            print("Columnas base:", base_columns)
        else:
            df = df.reindex(columns=base_columns)

        # Chunking
        for chunk in load_parquet_in_chunks(df):
            all_chunks.append(chunk)

        print(f'Mes {month} cargado.')
 
    return all_chunks# Specify your data loading logic here


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
