from prefect.deployments import Deployment
from etl_web_to_gcs import etl_web_to_gcs

from prefect.filesystems import GitHub

storage = GitHub.load("github-deployment")

deployment = Deployment.build_from_flow(
    flow=etl_web_to_gcs,
    name="github-deployment",
    storage=storage,
    entrypoint="week_2_workflow_orchestration/homework/question_4/etl_web_to_gcs.py:etl_parent_flow",

)

if __name__ == "__main__":
    deployment.apply()