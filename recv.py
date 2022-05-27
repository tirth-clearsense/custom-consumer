import asyncio
import json
import traceback
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
import logging
import os
from fastavro.validation import validate
from fastavro import parse_schema
from postgresdb import *
from base_schema import base_schema
from configparser import ConfigParser
import copy
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select,update
import requests
import datetime
from sqlalchemy.sql import exists 

Log_Format = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(filename = "customconsumer.log",
                    filemode = "w",
                    format = Log_Format,
                    level=logging.DEBUG
                    )
logger = logging.getLogger(__name__) 

config_object = ConfigParser()
config_object.read("config.ini")
eventhub = config_object["EVENTHUB"]
azureblob = config_object["AZUREBLOB"]
schema_api = config_object["PERSONICLE_SCHEMA_API"]
script_dir = os.path.dirname(__file__)
data_dict_path = os.path.join(script_dir,"data_dictionary/personicle_data_types.json")

with open(data_dict_path, 'r') as fi:
    personcile_data_types_json = json.load(fi)

session = loadSession()


def match_data_dictionary(stream_name):
    """
    Match a data type to the personicle data dictionary
    returns the data type information from the data dictionary
    """
    params = {'data_type': 'datastream','stream_name': stream_name}
    schema_response = requests.get(schema_api['MATCH_DICTIONARY_ENDPOINT']+"/match-data-dictionary",params=params)
    # data_stream = personcile_data_types_json["com.personicle"]["individual"]["datastreams"][stream_name]
    # return data_stream
    if not schema_response.status_code == 200:
        logger.warn(f"stream name {stream_name} not found")
    print(schema_response.json())
    return schema_response.json()
# function to store the incoming events to postgres database
async def on_event(partition_context, event):
    # Print the event data.
    print("Received the event: \"{}\" from the partition with ID: \"{}\"".format(event.body_as_json(encoding='UTF-8'), partition_context.partition_id))
    
    try:
        objects=[] # for bulk inserts
        current_event = event.body_as_json(encoding='UTF-8')
        stream_type = current_event['streamName']
        
        # match the stream name to the data dictionary
        stream_information = match_data_dictionary(stream_type)
        # get the corresponding schema and the table
        file_path = os.path.join(script_dir, f"avro/{stream_information['base_schema']}")
        
        with open(file_path, 'r') as fi:
            schema = json.load(fi)
        
        # validate the event with avro schema
        parsed_schema = parse_schema(schema) 
        validate(current_event, parsed_schema)
        logger.info("Valid event")

        # if valid, get the table name and store data
        table_name = stream_information['TableName']

        individual_id = current_event['individual_id']
        source=current_event['source']
        unit = current_event['unit']
        model_class = generate_table_class(table_name, copy.deepcopy(base_schema[stream_information['base_schema']]))
        model_class_user_datastreams = generate_table_class("user_datastreams", copy.deepcopy(base_schema['user_datastreams_store.avsc']))
    
        # query =  model_class_user_datastreams.__table__.insert(individual_id=individual_id,datastream=stream_type,last_updated=datetime.datetime.now(),source=source)
        data_stream_exists = session.query(exists().where( (model_class_user_datastreams.individual_id==individual_id) & 
        (model_class_user_datastreams.datastream == stream_type) & (model_class_user_datastreams.source == source))).scalar()
       
        if data_stream_exists:
            query = update(model_class_user_datastreams.__table__).where((model_class_user_datastreams.individual_id==individual_id) & 
            (model_class_user_datastreams.datastream == stream_type) & (model_class_user_datastreams.source == source)).values(last_updated = datetime.datetime.now())
        else: 
            print("else")
            query = insert(model_class_user_datastreams.__table__).values(individual_id=individual_id,datastream=stream_type,last_updated=datetime.datetime.now(),source=source)
        
        res = session.execute(query)
        session.commit()
        # print(f"Add datastream to user_datastreams: { res.rowcount}")
        confidence = current_event.get('confidence', None)
        record_values = []
        for datapoint in current_event['dataPoints']:
            timestamp = datapoint['timestamp']
            value = datapoint['value']
            # model_class = getClass(table_name) 
            logger.info(f"Adding data point: individual_id: {individual_id} \n \
                timestamp= {timestamp}, source= {source}, value={value}, unit={unit}, confidence={confidence}")
            record_values.append({"individual_id": individual_id,"timestamp": timestamp,"source": source,"value": value,"unit": unit,"confidence": confidence})
            # new_record = model_class(individual_id=individual_id,timestamp=timestamp,source=source,value=value,unit=unit,confidence=confidence)
            # objects.append(new_record)
        statement = insert(model_class).values(record_values)
        statement = statement.on_conflict_do_nothing(index_elements=[model_class.individual_id, model_class.timestamp, model_class.source])\
            .returning(model_class)
        orm_stmt = (
            select(model_class)
            .from_statement(statement)
            .execution_options(populate_existing=True)
        )
        
        return_values = session.execute(orm_stmt,)
        session.commit()

        logger.info("inserted {} rows".format(len(list(return_values))))
        print("inserted {} rows".format(len(list(return_values))))


        # session.bulk_save_objects(objects)
        
        
    except Exception as e:
        # print("Invalid event: \"{}\" from the partition with ID: \"{}\"".format(event.body_as_json(encoding='UTF-8'), partition_context.partition_id))
        session.rollback()
        logger.error(e)
        logger.error(traceback.format_exc())
    
    # Update the checkpoint so that the program doesn't read the events
    # that it has already read when you run it next time.
    await partition_context.update_checkpoint(event)

async def main():

    # Create an Azure blob checkpoint store to store the checkpoints.
    checkpoint_store = BlobCheckpointStore.from_connection_string("{checkpoint}".format(checkpoint=azureblob["CHECKPOINT"]), 
                            "{container}".format(container=azureblob["CONTAINER"]))
 
    # # Create a consumer client for the event hub.
    client = EventHubConsumerClient.from_connection_string("{connection_string}".format(connection_string=eventhub["CONNECTION_STRING"]), 
                consumer_group="{consumer_group}".format(consumer_group=eventhub['DATASTREAM_CONSUMER_GROUP']), eventhub_name="{name}".format(name=eventhub["DATASTREAM_EVENTHUB_NAME"]), checkpoint_store=checkpoint_store)
    
    
    async with client:
        # Call the receive method. Read from the beginning of the partition (starting_position: "-1")
        await client.receive(on_event=on_event,  starting_position="-1")

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # Run the main method.
    loop.run_until_complete(main())