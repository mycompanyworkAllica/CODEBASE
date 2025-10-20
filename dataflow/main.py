import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
from datetime import datetime
import re

# 🔧 HARD-CODED CONFIGURATION
BUCKET_NAME = "testcodebucket-01"

class CopyBlobDoFn(beam.DoFn):
    def setup(self):
        self.client = storage.Client()
        self.bucket = self.client.bucket(BUCKET_NAME)

    def process(self, blob_name):
        match = re.match(r"(\d{4})/(\d{2})/(\d{2})/(.+)", blob_name)
        if not match:
            yield f"Skipped (no match): {blob_name}"
            return

        year, month, day, filename = match.groups()
        destination_path = f"year={year}/month={month}/day={day}/{filename}"

        source_blob = self.bucket.blob(blob_name)
        destination_blob = self.bucket.blob(destination_path)

        # 🔁 Loop until rewrite completes
        token = None
        while True:
            token, _, _ = destination_blob.rewrite(source_blob, token=token)
            if token is None:
                break

        yield f"Copied: {blob_name} → {destination_path}"

def get_today_prefix():
    today = datetime.today()
    return today.strftime("%Y/%m/%d/")

def run():
    options = PipelineOptions(
        runner='DataflowRunner',
        project='tech-labs-gui',
        region='us-central1',
        temp_location=f'gs://{BUCKET_NAME}/tmp/',
        staging_location=f'gs://{BUCKET_NAME}/staging/',
        save_main_session=True,
        network='vpc-gui-1',
        subnetwork='regions/us-central1/subnetworks/subnet-01'
    )

    source_prefix = get_today_prefix()

    client = storage.Client()
    blobs = client.list_blobs(BUCKET_NAME, prefix=source_prefix)
    blob_names = [blob.name for blob in blobs]

    if not blob_names:
        print(f"No files found for prefix: {source_prefix}")
        return

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Create blob list" >> beam.Create(blob_names)
            | "Copy blobs" >> beam.ParDo(CopyBlobDoFn())
            | "Log output" >> beam.Map(print)
        )

if __name__ == "__main__":
    run()
