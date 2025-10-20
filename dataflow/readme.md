gcloud config set project tech-labs-gui

BUCKET_NAME: alica_test_storage
PROJECT_ID: tech-labs-gui

create python environment
py -m venv env
set shell to use venv path: .\env\Scripts\activate
install packages: pip install google-cloud-storage
deactivate: deactivate

install the latest version of apache sdk: pip install apache-beam[gcp]
python -m apache_beam.examples.wordcount --output outputs

python -m apache_beam.examples.wordcount \
    --region us-central1 \
    --input gs://dataflow-samples/shakespeare/kinglear.txt \
    --output gs://alica_test_storage/results/outputs \
    --runner DataflowRunner \
    --project tech-labs-gui \
    --temp_location gs://alica_test_storage/tmp/ \
    --network vpc-gui-1 \
    --subnetwork regions/us-central1/subnetworks/subnet-01

python -m apache_beam.examples.wordcount --region us-central1 --input gs://dataflow-samples/shakespeare/kinglear.txt --output gs://alica_test_storage/results/outputs --runner DataflowRunner --project tech-labs-gui --temp_location gs://alica_test_storage/tmp/ --network vpc-gui-1 \ --subnetwork regions/us-central1/subnetworks/subnet-01

link: https://docs.cloud.google.com/dataflow/docs/guides/create-pipeline-python

TO USE A SERVICE ACCOUNT ACTIVATE sa LOCALLY: gcloud auth activate-service-account your-sa-name@tech-labs-gui.iam.gserviceaccount.com `
    --key-file="path/to/service-account-key.json"


