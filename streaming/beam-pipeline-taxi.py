import apache_beam as beam
import json
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window

# Constants
INPUT_SUBSCRIPTION = "projects/purwadika/subscriptions/jcdeol3_capstone3_dimasadihartomo-sub"
OUTPUT_TABLE = "purwadika:jcdeol3_capstone3_dimasadihartomo.taxi_data_staging"

# Beam pipeline options
beam_options_dict = {
    'project': 'purwadika',
    'runner': 'DataflowRunner',
    'region': 'us-east1',
    'temp_location': 'gs://jdeol003-bucket/capstone3_dimasadihartomo/temp',
    'job_name': 'jcdeol3-capstone3-dimasadihartomo-taxi-streaming',
    'streaming': True,
    'service_account_email': 'jdeol-03@purwadika.iam.gserviceaccount.com'
}

beam_options = PipelineOptions.from_dictionary(beam_options_dict)

# Define pipeline with windowing
with beam.Pipeline(options=beam_options) as p:
    (
        p
        | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(subscription=INPUT_SUBSCRIPTION)
        | 'Decode' >> beam.Map(lambda x: x.decode('utf-8'))
        | 'Parse JSON' >> beam.Map(json.loads)
        | 'Fixed Window 60s' >> beam.WindowInto(window.FixedWindows(60))
        | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            OUTPUT_TABLE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )
