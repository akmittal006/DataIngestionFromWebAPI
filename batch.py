from __future__ import absolute_import

import logging, argparse, apache_beam as beam
from google.cloud import datastore, storage
from requests import get, post
from pytz import timezone, datetime
import json, time

def parse_pubsub(message):
    from google.cloud import datastore, storage
    from requests import get, post
    from pytz import timezone, datetime
    import json, time
	
    logging.getLogger().info("Starting for : " + message)
    print(message)
    client = datastore.Client()
    storage_client = storage.Client()

    entity = client.get(client.key("webapi", message.strip()))
    #Don't process the entity if we are already processing it
    if entity["Running"]:
        return
    #mark the entity processing to true
    entity["Running"] = True
    #update entity
    client.put(entity)
    try:
        while True:
            #fetch the entity using its key, which is table name
            entity = client.get(client.key("webapi", message.strip()))
            url = entity["URL"]
            date = datetime.datetime.strptime(entity["FromDate"], "%d/%m/%Y")
            todate = date + datetime.timedelta(days=1)
            bucket = entity["Bucket"]
            output_folder = entity["OutputFolder"]
            table_name = entity["TableName"]
            #Set output folder for data file
            output_file = output_folder + "/" + table_name + "/" + date.strftime("%Y/%m/%d") + ".csv"
            #Set output folder for schema file
            schema_file = "schemas/" + output_folder + "/" + table_name + ".csv"

            data = { "FromDate" : date.strftime("%m/%d/%Y %H:%M:%S"), 
                        "ToDate" : todate.strftime("%m/%d/%Y %H:%M:%S"), 
                        "CompressionType" : "A" }

            logging.getLogger().info("POST Payload : " + json.dumps(data))
            #Call the web api
            res = post(url, json=data)
            #If api call fails
            if res.status_code != 200:
                entity["Running"] = False
                client.put(entity)
                break
            #If api call is successful
            lines = []
            columns = ""
            # join values by comma to create csv
            for line in res.json():
                value = ",".join(map(str, line.values()))
                lines.append(value)
                columns = ",".join(map(str, line.keys()))

            bucket = storage_client.get_bucket(bucket)
            # create gcs object
            blob = bucket.blob(output_file)
            #Upload data file to gcs bucket
            blob.upload_from_string("\n".join(lines))
            #Upload schema file to gcs bucket
            if columns:
                blob = bucket.blob(schema_file)
                blob.upload_from_string(columns)
            #update FromDate column for the entity
            if datetime.datetime.now(timezone("Asia/Kolkata")).date() >= todate.date():
                entity["FromDate"] = unicode(todate.strftime("%d/%m/%Y"))
                client.put(entity)
            else:
                entity["Running"] = False
                client.put(entity)
                break
    except:
        entity["Running"] = False
        client.put(entity)


def run(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    client = datastore.Client()
    #fetch all entities from datastore, each entity contains information about a table
    entities = list(client.query(kind="webapi").fetch())
    names = [str(entity.key.name) for entity in entities]
    with beam.Pipeline(argv=pipeline_args) as p:
	    lines = ( p | beam.Create(names) | beam.Map(parse_pubsub))

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
