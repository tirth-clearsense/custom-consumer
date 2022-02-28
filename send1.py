# import asyncio
# from distutils.command.config import config
# from multiprocessing import connection
# from azure.eventhub.aio import EventHubProducerClient
# from azure.eventhub import EventData
# import json 
# import numpy as np
# from datetime import datetime
# from configparser import ConfigParser

# config_object = ConfigParser()
# config_object.read("config.ini")
# eventhub = config_object("EVENTHUB")
                                                                                                        
# async def run():
#     # Create a producer client to send messages to the event hub.
#     # Specify a connection string to your event hubs namespace and
#     # the event hub name.
#     producer = EventHubProducerClient.from_connection_string(conn_str="{connection_string}".format(connection_string=eventhub["CONNECTION_STRING"]), 
#                     eventhub_name="{name}".format(name=eventhub["EVENTHUB_NAME"]))
 
#     async with producer:
#         # Create a batch.
#         event_data_batch = await producer.create_batch()

#         # Add events to the batch.
#         hr_data = {"streamName": "com.personicle.individual.datastreams.heartrate", "individual_id": "test_user",
#                 "source": "test_source", "unit": "bpm", "value":70, "confidence": 0.8, "dataPoints": []}
#         for i in range(500):
#             hr_data['dataPoints'].append({
#                 "timestamp": str(datetime.utcnow()),
#                 "value": np.random.normal(80, 10)
#             })

#         print(hr_data)
#         event_data_batch.add(EventData(json.dumps(hr_data)))
        
#         # Send the batch of events to the event hub.
#         await producer.send_batch(event_data_batch)

# loop = asyncio.get_event_loop()
# loop.run_until_complete(run())