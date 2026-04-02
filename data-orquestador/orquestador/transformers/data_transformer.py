if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from mage_ai.data_preparation.shared.secrets import get_secret_value
from sqlalchemy import create_engine, text

@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    # Leer credenciales desde los secrets de Mage
    user     = get_secret_value('pg_user')
    password = get_secret_value('pg_password')
    host     = get_secret_value('pg_host')
    port     = get_secret_value('pg_port')
    db       = get_secret_value('pg_db')
    
    engine = create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{db}"
    )

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
        print("Schema 'raw' listo.")

    engine.dispose()

    # Pasa la lista de URLs al siguiente bloque
    return data

   
@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert len(output) > 0, 'La lista de URLs está vacía'