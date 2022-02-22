import asyncio
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
import json 

async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    
    producer = EventHubProducerClient.from_connection_string(conn_str="Endpoint=sb://personicle-eventhub-dev.servicebus.windows.net/;SharedAccessKeyName=testhub-policy-s1;SharedAccessKey=/WtIDcSPgtcJXOS009LgYhKSPGUTmoJisRVRbmYZllo=;EntityPath=testhub-new", 
                    eventhub_name="testhub-new")
    async with producer:
        # Create a batch.
        event_data_batch = await producer.create_batch()

        # Add events to the batch.
        for i in range(50):
            event_data_batch.add(EventData(json.dumps({
                "user_id": "test-analytics-job-user{}".format(i+1),
                "value": i+1
            })))
            # event_data_batch.add(EventData('Second event'))
            # event_data_batch.add(EventData('Third event'))

        # Send the batch of events to the event hub.
      
        await producer.send_batch(event_data_batch)

loop = asyncio.get_event_loop()
loop.run_until_complete(run())