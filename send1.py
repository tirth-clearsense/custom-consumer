import asyncio
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
import json 
import numpy as np
from datetime import datetime

async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    producer = EventHubProducerClient.from_connection_string(conn_str="Endpoint=sb://personicle-eventhub-dev.servicebus.windows.net/;SharedAccessKeyName=testhub-policy-s1;SharedAccessKey=/WtIDcSPgtcJXOS009LgYhKSPGUTmoJisRVRbmYZllo=;EntityPath=testhub-new", 
                    eventhub_name="testhub-new")
    # producer = EventHubProducerClient.from_connection_string(conn_str="Endpoint=sb://personicle-eventhub-dev.servicebus.windows.net/;SharedAccessKeyName=test_analytics_job_namespace_policy;SharedAccessKey=kMGKpnwn++uCnd7aSjQtrUzVq9QwEdKzRMMG1Q1/HQg=;EntityPath=datastream-hub",#Endpoint=sb://personicle-eventhub-dev.servicebus.windows.net/;SharedAccessKeyName=testhub-policy-s1;SharedAccessKey=/WtIDcSPgtcJXOS009LgYhKSPGUTmoJisRVRbmYZllo=;EntityPath=heartrate-hub", 
    #                 eventhub_name="datastream-hub")
    async with producer:
        # Create a batch.
        event_data_batch = await producer.create_batch()

        # Add events to the batch.
        hr_data = {"streamName": "com.personicle.individual.datastreams.heart_rate", "individual_id": "test_user",
                "source": "test_source", "unit": "bpm", "value":70, "confidence": 0.8, "dataPoints": []}
        for i in range(50):
            hr_data['dataPoints'].append({
                "timestamp": str(datetime.now()),
                "value": np.random.normal(80, 10)
            })

        print(hr_data)
        event_data_batch.add(EventData(json.dumps(hr_data)))
            # event_data_batch.add(EventData('Second event'))
            # event_data_batch.add(EventData('Third event'))

        # Send the batch of events to the event hub.
        await producer.send_batch(event_data_batch)

loop = asyncio.get_event_loop()
loop.run_until_complete(run())