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
from dateutil.parser import parse
from flask import Flask

app = Flask(__name__)
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
data_sync_api = config_object["PERSONICLE_DATA_SYNC_API"]

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
        print(stream_information)
        # get the corresponding schema and the table
        # file_path = os.path.join(script_dir, f"avro/{stream_information['base_schema']}")
        
        # with open(file_path, 'r') as fi:
        #     schema = json.load(fi)
        
        # validate the event with avro schema
        # parsed_schema = parse_schema(schema) 
        # validate(current_event, parsed_schema) # call api

        data_dict_params = {"data_type": "datastream"}
        data_dict_response = requests.post(schema_api['VALIDATE_DATA_PACKET'], 
        json=current_event, params=data_dict_params)
        
        if not json.loads(data_dict_response.text).get("schema_check", False):
            logger.error(f"Invalid event: {current_event}")
            return
#         logger.info("Valid event")
        print("valid event")
        # if valid, get the table name and store data
        table_name = stream_information['TableName']
        print(table_name)
        print(json.loads(data_dict_response.text))
        individual_id = current_event['individual_id']
        source=current_event['source']
        if source == "Personicle":
#             logger.info("Personicle source")
        unit = current_event.get('unit', None)
        model_class = generate_table_class(table_name, copy.deepcopy(base_schema[stream_information['base_schema']]))
        print(model_class)
        model_class_user_datastreams = generate_table_class("user_datastreams", copy.deepcopy(base_schema['user_datastreams_store.avsc']))
    
        # query =  model_class_user_datastreams.__table__.insert(individual_id=individual_id,datastream=stream_type,last_updated=datetime.datetime.now(),source=source)
        data_stream_exists = session.query(exists().where( (model_class_user_datastreams.individual_id==individual_id) & 
        (model_class_user_datastreams.datastream == stream_type) & (model_class_user_datastreams.source == source))).scalar()
        # print(data_stream_exists)
        # print(f"Add datastream to user_datastreams: { res.rowcount}")
        confidence = current_event.get('confidence', None)
        # if source != 'Personicle':
        record_values = []
        is_interval = "datastreams.interval" in stream_type
        for datapoint in current_event['dataPoints']:
            try:
              value = datapoint['value']
              # model_class = getClass(table_name) 
              if is_interval:
#                 logger.info("Interval data stream {}".format(stream_type))
                start_time = datapoint['start_time']
                end_time = datapoint['end_time']
#                 logger.info(f"Adding data point: individual_id: {individual_id} \n \
#                   start_time= {start_time}, end_time={end_time}, source= {source}, value={value}, unit={unit}, confidence={confidence}")
                record_values.append({"individual_id": individual_id,"start_time": start_time, "end_time": end_time,"source": source,"value": value,"unit": unit,"confidence": confidence})
              else:
#                 logger.info("Instantaneous data stream {}".format(stream_type))
                timestamp = datapoint['timestamp']
#                 logger.info(f"Adding data point: individual_id: {individual_id} \n \
#                   timestamp= {timestamp}, source= {source}, value={value}, unit={unit}, confidence={confidence}")
                record_values.append({"individual_id": individual_id,"timestamp": timestamp,"source": source,"value": value,"unit": unit,"confidence": confidence})
            except Exception as e:
              logger.error("Error while adding point for datastream {}".format(stream_type))
              logger.error(e)
              continue
        # new_record = model_class(individual_id=individual_id,timestamp=timestamp,source=source,value=value,unit=unit,confidence=confidence)
        # objects.append(new_record)
        if is_interval:
          max_timestamp = max(record_values, key=lambda dp: parse(dp['end_time']) )['end_time'] 
          min_timestamp = min(record_values, key=lambda dp: parse(dp['start_time']) )['start_time'] 
        else:
          max_timestamp = max(record_values, key=lambda dp: parse(dp['timestamp']) )['timestamp'] 
          min_timestamp = min(record_values, key=lambda dp: parse(dp['timestamp']) )['timestamp'] 

    # max_timestamp = max(record_values, key=lambda dp: datetime.datetime.strptime(dp['timestamp'],'%Y-%m-%d %H:%M:%S.%f') if datetime.datetime.strptime(dp['timestamp'],'%Y-%m-%d %H:%M:%S.%f') else
    # datetime.datetime.strptime(dp['timestamp'],'%Y-%m-%d %H:%M:%S') )['timestamp'] 
    # min_timestamp = min(record_values, key=lambda dp: datetime.datetime.strptime(dp['timestamp'],'%Y-%m-%d %H:%M:%S.%f') if datetime.datetime.strptime(dp['timestamp'],'%Y-%m-%d %H:%M:%S.%f') else
    # datetime.datetime.strptime(dp['timestamp'],'%Y-%m-%d %H:%M:%S') )['timestamp'] 
#         logger.info(f"max timestamp is {max_timestamp}")
        if data_stream_exists:
            query = update(model_class_user_datastreams.__table__).where((model_class_user_datastreams.individual_id==individual_id) & 
            (model_class_user_datastreams.datastream == stream_type) & (model_class_user_datastreams.source == source)).values(last_updated = max_timestamp)
        else: 
            query = insert(model_class_user_datastreams.__table__).values(individual_id=individual_id,datastream=stream_type,last_updated=max_timestamp,source=source)
        
        session.execute(query)
        session.commit()
        
        statement = insert(model_class).values(record_values)
        
        if is_interval:
          statement = statement.on_conflict_do_nothing(index_elements=[model_class.individual_id, model_class.start_time, model_class.end_time, model_class.source])\
            .returning(model_class)
          orm_stmt = (
              select(model_class)
              .from_statement(statement)
              .execution_options(populate_existing=True)
          )
        else:
          statement = statement.on_conflict_do_nothing(index_elements=[model_class.individual_id, model_class.timestamp, model_class.source])\
              .returning(model_class)
          orm_stmt = (
              select(model_class)
              .from_statement(statement)
              .execution_options(populate_existing=True)
          )
        
        return_values = session.execute(orm_stmt,)
        session.commit()
#         logger.info("inserted {} rows".format(len(list(return_values))))
        print("inserted {} rows".format(len(list(return_values))))

        if not source.startswith("Personicle"):
            #call data cleaning api
#             logger.info("Calling data sync api")
            params = {"user": individual_id, "freq": "1min", "data": stream_type,"source":source,"starttime": min_timestamp, "endtime": max_timestamp}
            # res = requests.get(data_sync_api["ENDPOINT"], params=params).json()
#             logger.info(f"Datastream cleaned response: {res}")
         
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
    app.run(loop.run_until_complete(main()))
