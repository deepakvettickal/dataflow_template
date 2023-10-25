import os
import subprocess
import datetime
import logging
import sys

# Set up logging
logging.basicConfig(level=logging.INFO)

GCP_PROJECT = "project_id"
GCP_REGION = "gcp_region"
TEMPLATE_NAME = "etl-pipeline"
IMAGE_TAG = "latest"  # change the tag to keep track of images in Artifacts Registry
DATAFLOW_FOLDER = "gcp_storage_folder_path"  # create a folder for dataflow temporary and staging files on GCP Storage

GCS_PATH = os.getenv("GCS_PATH", f"gs://{DATAFLOW_FOLDER}")
TEMPLATE_PATH = f"{GCS_PATH}/templates/{TEMPLATE_NAME}"
TEMPLATE_IMAGE = f"{GCP_REGION}-docker.pkg.dev/{GCP_PROJECT}/{TEMPLATE_NAME}/{TEMPLATE_NAME}:{IMAGE_TAG}"
JOB_NAME = f"etl-pipeline-{datetime.datetime.now().strftime('%Y%m%dt%H%M%S')}"
WORKDIR = os.getenv("WORKDIR", "/opt/dataflow")
FLEX_TEMPLATE_PYTHON_PY_FILE = f"{WORKDIR}/dataflow.py"
FLEX_TEMPLATE_PYTHON_SETUP_FILE = f"{WORKDIR}/setup.py"

def run_cmd(cmd):
    try:
        subprocess.run(cmd, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        logging.error(f"Command '{cmd}' failed with error: {e}")
        sys.exit(1)

def help():
    print("""
    Available commands:
    - init
    - image
    - job
    - template
    """)

def init():
    logging.info("Initiating...")
    run_cmd(f"gcloud config set project {GCP_PROJECT}")
    logging.info(f"Enabling Private Google Access in Region {GCP_REGION} ......")
    run_cmd(f"gcloud compute networks subnets update default --region={GCP_REGION} --enable-private-ip-google-access")
    logging.info("Enabling Dataflow Service....")
    run_cmd(f"gcloud services enable dataflow --project {GCP_PROJECT}")
    logging.info("Enabling Artifact Registry...")
    run_cmd(f"gcloud services enable artifactregistry.googleapis.com --project {GCP_PROJECT}")
    logging.info("Building Artifact Repo to Store Docker Image of Code....")
    run_cmd(f"gcloud artifacts repositories create {TEMPLATE_NAME} --repository-format=docker --location={GCP_REGION} --async")
    logging.info("SUCCESS")

def image():
    logging.info("Building image...")
    run_cmd("gcloud config set builds/use_kaniko True")
    run_cmd("gcloud config set builds/kaniko_cache_ttl 480")
    run_cmd(f"gcloud builds submit --tag {TEMPLATE_IMAGE} .")
    logging.info("Image built and pushed to artifact-registry")
    logging.info("SUCCESS")

def job(prod):
    logging.info("Starting job...")
    run_cmd(f"python dataflow.py --job-name {JOB_NAME} --runner DataflowRunner --project {GCP_PROJECT} --region {GCP_REGION} --staging-location {GCS_PATH}/staging --temp-location {GCS_PATH}/temp --experiments enable_prime --prod {prod}")
    logging.info("SUCCESS")

def template(prod):
    logging.info("Creating template...")
    run_cmd(f"python -m dataflow --runner DataflowRunner --project {GCP_PROJECT} --staging_location {GCS_PATH}/staging --temp_location {GCS_PATH}/temp --template_location {GCS_PATH}/legacy-templates/{TEMPLATE_NAME}/{TEMPLATE_NAME} --region europe-west2 --prod {prod}")
    run_cmd(f"gsutil cp ./{TEMPLATE_NAME}-metadata {GCS_PATH}/legacy-templates/{TEMPLATE_NAME}/{TEMPLATE_NAME}_metadata")
    logging.info("SUCCESS")

def deploy(prod):
    image()
    template(prod)

commands = {
    "init": init,
    "image": image,
    "job": job,
    "template": template,
    "deploy": deploy,
}

if __name__ == "__main__":
    if len(sys.argv) < 2:
        help()
    else:
        command = sys.argv[1]
        arguments = sys.argv[2:]
        if command in commands:
            commands[command](*arguments)
        else:
            logging.error(f"Unknown command: {command}")
