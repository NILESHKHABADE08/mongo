import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
import json
from datetime import datetime
from pymongo import MongoClient


# -------- Mongo Write DoFn --------
class WriteToMongo(beam.DoFn):

    def __init__(self, mongo_uri, database, collection, last_processed_ts):
        self.mongo_uri = mongo_uri
        self.database = database
        self.collection = collection
        self.last_processed_ts = last_processed_ts

    def setup(self):
        self.client = MongoClient(self.mongo_uri)
        self.coll = self.client[self.database][self.collection]

    def process(self, element):

        record = json.loads(element)

        record_ts = datetime.fromisoformat(record["updated_at"])

        # -------- Incremental Filter --------
        if record_ts > self.last_processed_ts:

            self.coll.update_one(
                {"patient_id": record["patient_id"]},
                {"$set": record},
                upsert=True
            )

        yield record


# -------- Pipeline Runner --------
def run():

    options = PipelineOptions(
        runner="DataflowRunner",
        project="My First Project",
        region="us-central1",
        temp_location="gs://mgdb/temp",
        staging_location="gs://mgdb/staging",
        save_main_session=True
    )

    mongo_uri = "mongodb+srv://nileshkhabade3818_db_user:KqW7FT4wt56XYl0N@mongodatabase.3sgw0wv.mongodb.net/"
    database = "mogotestdb"
    collection = "patients"

    # ---- Incremental checkpoint ----
    # In production read from BQ / Firestore / metadata store
    last_processed_ts = datetime.fromisoformat("2026-01-01T00:00:00")

    with beam.Pipeline(options=options) as p:

        (
            p
            | "Read JSON Files" >> ReadFromText("gs://mgdb/files/*.json")
            | "Write To Mongo" >> beam.ParDo(
                WriteToMongo(
                    mongo_uri,
                    database,
                    collection,
                    last_processed_ts
                )
            )
        )


if __name__ == "__main__":
    run()
