if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom

from mage_ai.orchestration.db.models.schedules import PipelineRun

@custom
def check_raw_pipeline_completed(*args, **kwargs):
    """
    Espera a que raw_pipeline haya corrido exitosamente
    antes de ejecutar clean_pipeline.
    """
    runs = PipelineRun.query.filter(
        PipelineRun.pipeline_uuid == 'raw_pipeline',
        PipelineRun.status == 'completed'
    ).all()

    if runs:
        print("raw_pipeline completado. Procediendo con clean_pipeline.")
        return True

    print("Esperando que raw_pipeline termine...")
    return False