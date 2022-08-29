import asyncio
from distutils.command.config import config
from multiprocessing import connection
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
import json 
import numpy as np
from datetime import datetime
from configparser import ConfigParser
import time

config_object = ConfigParser()
config_object.read("config.ini")
eventhub = config_object["EVENTHUB"]
                                                                                                        
async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    producer = EventHubProducerClient.from_connection_string(conn_str="{connection_string}".format(connection_string=eventhub["CONNECTION_STRING"]), 
                    eventhub_name="{name}".format(name=eventhub["DATASTREAM_EVENTHUB_NAME"]))
 
    async with producer:
        # Create a batch.
        event_data_batch = await producer.create_batch()

        # Add events to the batch.
        # hr_data = {"streamName": "com.personicle.individual.datastreams.heart_intensity_minutes", "individual_id": "00u58i6uqkZLhvC0a5d7",
        #         "source": "test_source", "unit": "bpm", "confidence": 0.8, "dataPoints": []}
        hr_data = {'individual_id': 'test_user_id', 'streamName': 'com.personicle.individual.datastreams.location', 'source': 'PERSONICLE_IOS_APP', 'dataPoints': [{'timestamp': str(datetime.now()), 'value': [{'latitude': 59.606209, 'longitude': -122.332069}]}]}
        # for i in range(50):
        #     hr_data['dataPoints'].append({
        #         "timestamp": str(datetime.utcnow()),
        #         "value": int(np.random.normal(80, 10))
        #     })
        #     time.sleep(0.01)

        print(hr_data)
        event_data_batch.add(EventData(json.dumps(hr_data)))

        # power_data = {"streamName": "com.personicle.individual.datastreams.step.count", "individual_id": "00u58i6uqkZLhvC0a5d7",
        #         "source": "google-fit", "unit": "watts", "dataPoints": []}
        # for i in range(50):
        #     power_data['dataPoints'].append({
        #         "timestamp": str(datetime.now()),
        #         "value": np.random.randint(10, 120)
        #     })
        #     time.sleep(0.01)

        # print(power_data)
        # event_data_batch.add(EventData(json.dumps(power_data)))
        
        # Send the batch of events to the event hub.
        await producer.send_batch(event_data_batch)

loop = asyncio.get_event_loop()
loop.run_until_complete(run())