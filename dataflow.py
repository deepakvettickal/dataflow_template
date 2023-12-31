import apache_beam as beam
from app.main import main
import app.config as cnf
import logging


class DoMain(beam.DoFn):
    def __init__(self, prod):
        self.main = main
        self.prod = prod

    def process(self, element):
        try:
            # Extract dict key and value from the pcollection
            site_key = element[0]
            site_val = element[1]

            config_dict = {site_key: site_val}

            self.main(self.prod, config_dict)
            return element
        except Exception as e:
            logging.basicConfig(level=logging.DEBUG, 
                                format='%(asctime)s - %(levelname)s - %(message)s')
            logger = logging.getLogger(__name__)
            logger.exception(f'Run for: {site_key} failed with error {e}')


# Adding a custom pipeline argument, prod, to set the mode of the pipeline between testing(False) and production(True)
class PipeOptions(beam.pipeline.PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--prod', dest='prod', default=False,
                            help='use True to run the job in production')


def pipeline(job_name, region):
    dataflow_bucket = 'gs://dataflow'  # bucket to store staging and temporary files generated by dataflow

    # Set the pipeline metadata
    options = PipeOptions(
        job_name = job_name,
        runner='DataflowRunner',
        # runner='DirectRunner',
        project='gcp-project-id',
        region=region,
        dataflow_service_options=['enable_prime'],
        temp_location=f'{dataflow_bucket}/temp',
        staging_location=f'{dataflow_bucket}/staging',
        experiments=['use_runner_v2', 'no_use_multiple_sdk_containers', 'use_sibling_sdk_workers'],
        sdk_container_image='artifacts-registry-image-tag:version',  # tag:version
        sdk_location='container',
        )

    prod = True if options.prod == 'True' else False  # prod determines if the pipeline is in production or testing

    # Initialize the pipeline
    pipeline = beam.Pipeline(options=options)
    # Apply the main.py logic as part of the pipeline
    pcollections = pipeline | 'Read Pcollections' >> beam.Create(cnf.pcollections_dict)
    result = pcollections | 'Run Main Fn' >> beam.ParDo(DoMain(prod))

    return pipeline


def create_dataflow_job(region):
    job = pipeline(job_name='etl-job', region=region)
    # Run the job
    result = job.run()
    result.wait_until_finish()


if __name__ == "__main__":
    create_dataflow_job(region='gcp-region')  # change the region to your gcp region
