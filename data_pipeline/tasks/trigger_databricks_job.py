from prefect import task
from prefect_databricks import DatabricksCredentials
from prefect_databricks.jobs import jobs_runs_submit
from prefect_databricks.models.jobs import (
    JobTaskSettings,
    NotebookTask,
)


@task(name="Triggers databricks job to load data from s3 into delta lake")
async def trigger_databrick_job(job_name: str, s3_path: str):
    databricks_credentials = await DatabricksCredentials.load("databricks-block")

    notebook_path = (
        "/Workspace/Repos/gazmendhalili2016@gmail.com/stockPrediction/notebooks/jobs/"
        + job_name
    )

    notebook_task = NotebookTask(
        notebook_path=notebook_path,
        base_parameters={"s3_path": s3_path},
    )

    job_task_settings = JobTaskSettings(
        notebook_task=notebook_task, task_key="prefect-task"
    )

    run = await jobs_runs_submit(
        databricks_credentials=databricks_credentials,
        run_name="Load data from s3 into delta lake",
        tasks=[job_task_settings],
    )

    return run
