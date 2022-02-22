import asyncio
import json
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
import logging
import os
LOG = logging.getLogger() 
script_dir = os.path.dirname(__file__)
file_path = os.path.join(script_dir, 'data_dictionary/personicle_data_types.json')
with open(file_path, 'r') as fi:
        data_dict = json.load(fi)
       
async def on_event(partition_context, event):
    # Print the event data.

    print("Received the event: \"{}\" from the partition with ID: \"{}\"".format(event.body_as_str(encoding='UTF-8'), partition_context.partition_id))
    
    
    # get the type of data stream from the event and extract the corresponding table name
    table_name = data_dict["com.personicle"]["individual"]["datastreams"][{}]["TableName"]
    print(table_name)
    # Update the checkpoint so that the program doesn't read the events
    # that it has already read when you run it next time.
    await partition_context.update_checkpoint(event)

async def main():
    
    # Create an Azure blob checkpoint store to store the checkpoints.
    checkpoint_store = BlobCheckpointStore.from_connection_string("DefaultEndpointsProtocol=https;AccountName=personicle;AccountKey=6731/zkt1n/40xrwRSrHoHBz2heG1a/RP6jNfW8RXEN4MFEGdDVE8L4LXw31JeENk1TviF+LuaNxd2ZEQ4RGog==;EndpointSuffix=core.windows.net", 
                            "test-container")

    # Create a consumer client for the event hub.
    client = EventHubConsumerClient.from_connection_string("Endpoint=sb://personicle-eventhub-dev.servicebus.windows.net/;SharedAccessKeyName=testhub-policy-s1;SharedAccessKey=/WtIDcSPgtcJXOS009LgYhKSPGUTmoJisRVRbmYZllo=;EntityPath=testhub-new", 
                consumer_group="quick_test_group", eventhub_name="testhub-new", checkpoint_store=checkpoint_store)
    async with client:
        # Call the receive method. Read from the beginning of the partition (starting_position: "-1")
        await client.receive(on_event=on_event,  starting_position="-1")

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # Run the main method.
    loop.run_until_complete(main())