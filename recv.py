import asyncio
import json
from urllib.request import CacheFTPHandler
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
import logging
import os
from avro_validator.schema import Schema
import avro
from fastavro.validation import validate
from fastavro import parse_schema
from postgresdb import *
from sqlitedb import conn,heartrate
import sqlalchemy as db 

LOG = logging.getLogger() 
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

#function to return model class name
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
            # new_record = model_class(individual_id=individual_id,timestamp=timestamp,source=source,value=value,unit=unit,confidence=confidence)
            query = heartrate.insert().values(individual_id=individual_id,timestamp=timestamp,source=source,value=value,unit=unit,confidence=confidence)
            # objects.append(new_record)
            conn.execute(query)

        # session.bulk_save_objects(objects)
        # session.commit()
        
    except:
        print("Invalid event: \"{}\" from the partition with ID: \"{}\"".format(event.body_as_json(encoding='UTF-8'), partition_context.partition_id))
        pass
    query = db.select([heartrate])
    result_proxy = conn.execute(query)
    result_set = result_proxy.fetchall()
    print(result_set)
    # table_name = data_dict["com.personicle"]["individual"]["datastreams"][{}]["TableName"]
    # print(table_name)
    # Update the checkpoint so that the program doesn't read the events
    # that it has already read when you run it next time.
    await partition_context.update_checkpoint(event)

async def main():
        # current_event = {'streamName': 'com.personicle.individual.datastreams.heartrate', 'individual_id': 'test_user', 'source': 'test_source', 'unit': 'bpm', 'value': 70, 'confidence': 0.8, 'dataPoints': [{'timestamp': '2022-02-25 20:27:08.123187', 'value': 75.26720190743127}, {'timestamp': '2022-02-25 20:27:08.123265', 'value': 64.96455764351987}, {'timestamp': '2022-02-25 20:27:08.123272', 'value': 76.33972288445601}, {'timestamp': '2022-02-25 20:27:08.123276', 'value': 70.18792131489458}, {'timestamp': '2022-02-25 20:27:08.123280', 'value': 55.89733969954463}, {'timestamp': '2022-02-25 20:27:08.123284', 'value': 81.0142328209168}, {'timestamp': '2022-02-25 20:27:08.123288', 'value': 90.60477323034864}, {'timestamp': '2022-02-25 20:27:08.123292', 'value': 84.47981038691515}, {'timestamp': '2022-02-25 20:27:08.123296', 'value': 58.460625783260255}, {'timestamp': '2022-02-25 20:27:08.123301', 'value': 75.9406889349082}]}
        # objects=[]
        # stream_type = current_event['streamName']
        # personicle_data_type = stream_type.split(".")
        # table_name = personcile_data_types_json["com.personicle"]["individual"]["datastreams"][personicle_data_type[-1]]["TableName"]
        # print(table_name)
        # individual_id = current_event['individual_id']
        # source=current_event['source']
        # unit = current_event['unit']

        # confidence = current_event['confidence']
        # for datapoint in current_event['dataPoints']:
        #     timestamp = datapoint['timestamp']
        #     value = datapoint['value']
        #     model_class = getClass(table_name)
        #     new_record = model_class(individual_id=individual_id,timestamp=timestamp,source=source,value=value,unit=unit,confidence=confidence)
        #     objects.append(new_record)
      

        # session.bulk_save_objects(objects)
        # session.commit()
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