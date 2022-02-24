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

LOG = logging.getLogger() 
script_dir = os.path.dirname(__file__)
     
async def on_event(partition_context, event):
    # Print the event data.
    
    print("Received the event: \"{}\" from the partition with ID: \"{}\"".format(event.body_as_json(encoding='UTF-8'), partition_context.partition_id))
    
    try:
        stream_type = event.body_as_json(encoding='UTF-8')['streamName']
        print(f"stream type is {stream_type}")
        file_path = os.path.join(script_dir, f'avro/{stream_type}+".avsc')
        print(f"file path is {file_path}")
        with open(file_path, 'r') as fi:
            schema = json.load(fi)
        
        parsed_schema = parse_schema(schema) 
        validate(event.body_as_json(encoding='UTF-8'), parsed_schema)
        print("Valid event")
        # get table name here from the event
    except:
        print("Invalid event: \"{}\" from the partition with ID: \"{}\"".format(event.body_as_json(encoding='UTF-8'), partition_context.partition_id))
        pass
    
#    {"streamName": "com.personicle.data.heart_rate", "individual_id": "test_user",

# "source": "test_source","dataPoints": []}

   

    # get the type of data stream from the event and extract the corresponding table name
    # table_name = data_dict["com.personicle"]["individual"]["datastreams"][{}]["TableName"]
    # print(table_name)
    # Update the checkpoint so that the program doesn't read the events
    # that it has already read when you run it next time.
    await partition_context.update_checkpoint(event)

async def main():
    # data_to_validate = {
    #   "individual_id": "00u3w69sw5zLDtlYK5d7", 
    #   "start_time": 1639102305000, 
    #   "end_time": 1639104532000, 
    #   "event_name": "activity", 
    #   "source": "fitbit", 
    #    "parameters": "{\"duration\": 2227.0, \"caloriesBurned\": 335, \"activityName\": \"Fitbod: Hamstrings, Chest, Biceps, Shoulders, Quadriceps\", \"distance\": 2.641, \"distanceUnit\": \"Kilometer\", \"activityLevel\": [{\"minutes\": 0, \"name\": \"sedentary\"}, {\"minutes\": 0, \"name\": \"lightly\"}, {\"minutes\": 0, \"name\": \"fairly\"}, {\"minutes\": 37, \"name\": \"very\"}]}"
    # }
  
    # data_to_validate = {'streamName': 'com.personicle.individual.datastreams.heart_rate', 
    # 'individual_id': 'test_user', 'source': 'test_source',
    #  'confidence': 0.8, 'dataPoints': [{'timestamp': 1645746223.941225, 'value': 82.12154493462943}, {'timestamp': 1645746223.941286, 'value': 80.08607576828906}, {'timestamp': 1645746223.941294, 'value': 70.61598081098563}, {'timestamp': 1645746223.941299, 'value': 63.9757691764094}, {'timestamp': 1645746223.941304, 'value': 111.73535867126361}, {'timestamp': 1645746223.941309, 'value': 88.71505379443602}, {'timestamp': 1645746223.941313, 'value': 54.4984989841015}, {'timestamp': 1645746223.941318, 'value': 73.69516398947049}, {'timestamp': 1645746223.941323, 'value': 70.18788894672744}, {'timestamp': 1645746223.941327, 'value': 75.3669876763237}, {'timestamp': 1645746223.941332, 'value': 86.06837368512242}, {'timestamp': 1645746223.941336, 'value': 96.79567237079436}, {'timestamp': 1645746223.941341, 'value': 70.97902953897247}, {'timestamp': 1645746223.941345, 'value': 75.64543434439877}, {'timestamp': 1645746223.94135, 'value': 84.44216175270418}, {'timestamp': 1645746223.941354, 'value': 79.49462521813136}, {'timestamp': 1645746223.941358, 'value': 77.62285647082484}, {'timestamp': 1645746223.941363, 'value': 75.1054573060388}, {'timestamp': 1645746223.941367, 'value': 72.78127440926191}, {'timestamp': 1645746223.941371, 'value': 70.75366359195328}, {'timestamp': 1645746223.941376, 'value': 90.82702863089824}, {'timestamp': 1645746223.94138, 'value': 84.36433246845839}, {'timestamp': 1645746223.941384, 'value': 92.584136605119}, {'timestamp': 1645746223.941389, 'value': 78.89789328439404}, {'timestamp': 1645746223.941393, 'value': 81.15993562300693}, {'timestamp': 1645746223.941397, 'value': 83.40006184284785}, {'timestamp': 1645746223.941402, 'value': 89.10088818860879}, {'timestamp': 1645746223.941409, 'value': 78.50233073361038}, {'timestamp': 1645746223.941414, 'value': 71.17713470153559}, {'timestamp': 1645746223.941418, 'value': 81.71237757995155}, {'timestamp': 1645746223.941423, 'value': 87.44088868829908}, {'timestamp': 1645746223.941427, 'value': 81.52713534761952}, {'timestamp': 1645746223.941431, 'value': 76.99346631450457}, {'timestamp': 1645746223.941436, 'value': 75.47843474327341}, {'timestamp': 1645746223.94144, 'value': 58.91990613887803}, {'timestamp': 1645746223.941444, 'value': 72.71951503701075}, {'timestamp': 1645746223.941449, 'value': 67.99099945063747}, {'timestamp': 1645746223.941453, 'value': 84.24879229238493}, {'timestamp': 1645746223.94146, 'value': 80.57477459942322}, {'timestamp': 1645746223.941466, 'value': 75.68867744755163}, {'timestamp': 1645746223.941471, 'value': 88.27122015874004}, {'timestamp': 1645746223.941475, 'value': 71.4726875260184}, {'timestamp': 1645746223.94148, 'value': 95.14967625794641}, {'timestamp': 1645746223.941484, 'value': 82.99917378519561}, {'timestamp': 1645746223.941489, 'value': 81.88085976395872}, {'timestamp': 1645746223.941493, 'value': 85.5692229327405}, {'timestamp': 1645746223.941498, 'value': 64.96075945870184}, {'timestamp': 1645746223.941502, 'value': 80.70173615217165}, {'timestamp': 1645746223.941507, 'value': 75.51650719918199}, {'timestamp': 1645746223.941511, 'value': 75.3940198863882}]
    #  }
    # print(validate(data_to_validate,parsed_schema)) 
    # parsed_schema.validate(data_to_validate)
    # print(parsed_schema.validate(data_to_validate))
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