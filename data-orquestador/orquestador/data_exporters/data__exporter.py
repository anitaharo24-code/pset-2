from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path
from tqdm.auto import tqdm
import math

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a PostgreSQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#postgresql
    """
    schema_name = 'raw'  # Specify the name of the schema to export data to
    table_name = 'ny_taxi_trips'  # Specify the name of the table to export data to
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    tamano = 1000

    num_chunks = math.ceil(df.shape[0]/tamano)
    
    inicio = 0
    fin = tamano

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:

        loader.export(
            df.head(0),
            schema_name,
            table_name,
            index=False,  # Specifies whether to include index in exported table
            if_exists='replace',  # Specify resolution policy if table name already exists
            )

        for i in tqdm(range(1, num_chunks)):
            loader.export(
                df.iloc[inicio:fin],
                schema_name,
                table_name,
                index=False,  # Specifies whether to include index in exported table
                if_exists='append',  # Specify resolution policy if table name already exists
            )
        
            inicio = fin
            fin = tamano * i
        

    # Idempotencia: No importa cuando yo ejecute el script, simpre da el mismo resultado

    print('Creacion de la tabla')
    datos_crudos.head(0).to_sql(
        name='viajes_taxi_amarillo',
        con=conexion,
        if_exists='replace'
    )

    print('Inicio de guardado de datos en el warehouse')

    
