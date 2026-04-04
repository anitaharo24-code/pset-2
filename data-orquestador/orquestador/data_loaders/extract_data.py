if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd

@data_loader
def load_data(*args, **kwargs):
    """
    Genera lista de URLs de green taxi 2021-2025.
    No descarga nada aquí, solo prepara las URLs.
    """
    years  = range(2021, 2026)  # 2021, 2022, 2023, 2024, 2025
    months = range(1, 13)       # enero a diciembre

    lista_url = []
    for year in years:          # ← loop de años
        for month in months:    # ← loop de meses dentro de cada año
            lista_url.append({
                'year':  year,
                'month': month,
                'url':   f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet'
            })

    df_url = pd.DataFrame(lista_url)
    print(f"Archivos a procesar: {len(df_url)}")
    print(df_url)
    return df_url


@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
    assert len(output) > 0, 'No hay URLs generadas'
