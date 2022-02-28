import asyncio
import datetime
import json
from urllib.request import CacheFTPHandler
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
import logging
import os
from avro_validator.schema import Schema
from fastavro.validation import validate
from fastavro import parse_schema
from postgresdb import *
from configparser import ConfigParser

# from sqlitedb import conn,heartrate
# import sqlalchemy as db 

LOG = logging.getLogger() 

config_object = ConfigParser()
config_object.read("config.ini")
eventhub = config_object["EVENTHUB"]
azureblob = config_object["AZUREBLOB"]

script_dir = os.path.dirname(__file__)
data_dict_path = os.path.join(script_dir,"data_dictionary/personicle_data_types.json")

with open(data_dict_path, 'r') as fi:
            personcile_data_types_json = json.load(fi)

session = loadSession()

# map table name to correcspoding model class name 
table_to_class = {'heartrate': Heartrate, 'heart_intensity_minutes': '', 'active_minutes': '', 'resting_calories': '','active_calories':'',
'total_calories': '', 'distance': '', 'weight': '', 'body_fat': '', 'height':'', 'location': '', 'speed': '', 'blood_glucose': '',
'body_temperature':''
}

# function to return model class name
def getClass(table_name):
    return table_to_class[table_name]

async def on_event(partition_context, event):
    # Print the event data.
    
    print("Received the event: \"{}\" from the partition with ID: \"{}\"".format(event.body_as_json(encoding='UTF-8'), partition_context.partition_id))
    
    try:
        objects=[] # for bulk inserts
        current_event = event.body_as_json(encoding='UTF-8')
        stream_type = current_event['streamName']
        personicle_data_type = stream_type.split(".")
        table_name = personcile_data_types_json["com.personicle"]["individual"]["datastreams"][personicle_data_type[-1]]["TableName"]
        #
        print(table_name)

        print(f"stream type is {stream_type}")
        file_path = os.path.join(script_dir, f"avro/{stream_type}"+".avsc")
        
        print(f"file path is {file_path}")
        with open(file_path, 'r') as fi:
            schema = json.load(fi)
        
        parsed_schema = parse_schema(schema) 
        validate(current_event, parsed_schema)
        print("Valid event")

        individual_id = current_event['individual_id']
        source=current_event['source']
        unit = current_event['unit']

        confidence = current_event['confidence']
        for datapoint in current_event['dataPoints']:
            timestamp = datapoint['timestamp']
            value = datapoint['value']
            model_class = getClass(table_name)
            new_record = model_class(individual_id=individual_id,timestamp=timestamp,source=source,value=value,unit=unit,confidence=confidence)
            objects.append(new_record)

        session.bulk_save_objects(objects)
        session.commit()
        
    except:
        print("Invalid event: \"{}\" from the partition with ID: \"{}\"".format(event.body_as_json(encoding='UTF-8'), partition_context.partition_id))
        pass
    
    # Update the checkpoint so that the program doesn't read the events
    # that it has already read when you run it next time.
    await partition_context.update_checkpoint(event)

async def main():
      
    # Create an Azure blob checkpoint store to store the checkpoints.
    checkpoint_store = BlobCheckpointStore.from_connection_string("{checkpoint}".format(checkpoint=azureblob["CHECKPOINT"]), 
                            "{container}".format(container=azureblob["CONTAINER"]))
 
    # # Create a consumer client for the event hub.
    client = EventHubConsumerClient.from_connection_string("{connection_string}".format(connection_string=eventhub["CONNECTION_STRING"]), 
                consumer_group="{consumer_group}".format(consumer_group=eventhub['CONSUMER_GROUP']), eventhub_name="{name}".format(name=eventhub["EVENTHUB_NAME"]), checkpoint_store=checkpoint_store)
    
    
    async with client:
        # Call the receive method. Read from the beginning of the partition (starting_position: "-1")
        await client.receive(on_event=on_event,  starting_position="-1")

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # Run the main method.
    loop.run_until_complete(main())