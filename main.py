from __future__ import absolute_import

import logging, argparse, apache_beam as beam

def parse_pubsub(message):
	from google.cloud import datastore, storage
	from requests import get, post
	from pytz import timezone, datetime
	import json, time

	logging.getLogger().info("Starting for : " + message)

	client = datastore.Client()
	storage_client = storage.Client()
	
	entity = client.get(client.key("webapi", message.strip()))
	if entity["Running"]:
		return

	entity["Running"] = True
	client.put(entity)
	try:
		while True:
			entity = client.get(client.key("webapi", message.strip()))
			url = entity["URL"]
			date = datetime.datetime.strptime(entity["FromDate"], "%d/%m/%Y")
			todate = date + datetime.timedelta(days=1)
			bucket = entity["Bucket"]
			output_folder = entity["OutputFolder"]
			table_name = entity["TableName"]
			output_file = output_folder + "/" + table_name + "/" + date.strftime("%Y/%m/%d") + ".csv"
			schema_file = "schemas/" + output_folder + "/" + table_name + ".csv"

			data = { "FromDate" : date.strftime("%m/%d/%Y %H:%M:%S"), 
					 "ToDate" : todate.strftime("%m/%d/%Y %H:%M:%S"), 
					 "CompressionType" : "A" }

			logging.getLogger().info("POST Payload : " + json.dumps(data))

			res = post(url, json=data)
			
			if res.status_code != 200:
				entity["Running"] = False
				client.put(entity)
				break

			lines = []
			columns = ""
			for line in res.json():
				value = ",".join(map(str, line.values()))
				lines.append(value)
				columns = ",".join(map(str, line.keys()))

			bucket = storage_client.get_bucket(bucket)
			blob = bucket.blob(output_file)
			blob.upload_from_string("\n".join(lines))

			if columns:
				blob = bucket.blob(schema_file)
				blob.upload_from_string(columns)

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
	parser.add_argument(
		'--input_subscription', required=True,
		help='Input PubSub subscription of the form "projects/<PROJECT>/subscriptions/<SUBSCRIPTION>".')
	known_args, pipeline_args = parser.parse_known_args(argv)
	#beam pipeline 
	with beam.Pipeline(argv=pipeline_args) as p:
		lines = ( p | beam.io.ReadStringsFromPubSub(subscription=known_args.input_subscription)
					| beam.Map(parse_pubsub)
				)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
