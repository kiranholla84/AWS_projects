import json
import boto3
import boto3
import json
import csv
import datetime
import os
import random
import base64


def lambda_handler(event, context):
    dynamodb_res = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb_res.Table('<Enter your dynamodDbTableName>')
    sns_client = boto3.client('sns', region_name='us-east-1')
    topic_arn = "arn:aws:sns:us-east-1:<Enter your username>:dynamodb"

    #print("TEST event is\n===========================================================\n", event)
    for record in event['Records']:
        print("RECORD  is", record)
        payload = base64.b64decode(record['kinesis']['data'])
        payload = json.loads(payload)

        print("payload is", payload, type(payload))

        date_of_record = record['kinesis']['partitionKey'].split(" ")[0]

        for company_name in payload['timestamped_data'].keys():
            if payload['timestamped_data'][company_name] > (0.85 * payload['fiftyTwoWk_high_data'][company_name]):############################if near to 80% of ATL

                # If item exists in dynamodb, a key named "Item" will be available
                if 'Item' in (table.get_item(Key={'RecordDate': date_of_record, 'Stock': company_name})):
                    print("entry already exists for company", company_name, "on date", date_of_record,)
                else:
                    poi_record = {'RecordDate': date_of_record, 'Stock': company_name,
                                  'CurrentPrice': str(payload['timestamped_data'][company_name]),
                                  'POI': '52w high:' + str(payload['fiftyTwoWk_high_data'][company_name])
                                  }

                    print("sending data", poi_record, "to dynamodb and sns")
                    table.put_item(Item=poi_record)

                    try:
                        pass
                        sns_client.publish(TopicArn=topic_arn, Message=str(poi_record), Subject="Close to 52w high")
                    except Exception:
                        print("Except:(")

            elif payload['timestamped_data'][company_name] < (1.25 * payload['fiftyTwoWk_low_data'][company_name]): ############################if near to 125% of ATL

                # If item exists in dynamodb, a key named "Item" will be available
                if 'Item' in (table.get_item(Key={'RecordDate': date_of_record, 'Stock': company_name})):
                    print("entry for day already exists for day", date_of_record, "company", company_name)
                else:


                    poi_record = {'RecordDate': date_of_record, 'Stock': company_name,
                                  'CurrentPrice': str(payload['timestamped_data'][company_name]),
                                  'POI': '52w low:' + str(payload['fiftyTwoWk_low_data'][company_name])
                                  }

                    print("sending data", poi_record, "to dynamodb and sns")
                    table.put_item(Item=poi_record)

                try:
                    pass
                    sns_client.publish(TopicArn=topic_arn, Message=str(poi_record), Subject="Close to 52w low")
                except Exception:
                    print("Except:(")
        else:
            pass
