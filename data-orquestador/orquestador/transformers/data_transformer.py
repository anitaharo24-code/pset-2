if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

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
    transformed_chunks = []

    for df in data:
        # Convertir columnas a minúsculas
        df.columns = [col.lower() for col in df.columns]

        # Renombrar columnas específicas
        df.rename(columns={
            'vendorid': 'vendor_id',
            'ratecodeid': 'rate_code_id',
            'pulocationid': 'pu_location_id',
            'dolocationid': 'do_location_id'
        }, inplace=True)

        transformed_chunks.append(df)

    return transformed_chunks


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
