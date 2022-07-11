import json
import boto3
import os
import logging

# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def ResponseProcessor_handler(event, context):
    try:
        print('event:::::::::',event)
        # executing the lambda only for insert events
        newImg_str_event_name = event['Records'][0]['eventName']
        if newImg_str_event_name == 'INSERT':
            
            statusMessage = os.environ['statusMessage']
            statusSuccessCode = os.environ['statusSuccessCode']
            # Getting the values from httpsink-staging-bass-response DynamoDB table
            statusCode = None
            newImg_str_body = event['Records'][0]['dynamodb']['NewImage']['value']['S']
            newImg_json_body = json.loads(newImg_str_body)
            
            if 'respUuid' in newImg_json_body:
                respUuid = newImg_json_body['respUuid']
                if respUuid is not None and respUuid != "":
                    if 'statusCode' in newImg_json_body:
                        statusCode = newImg_json_body['statusCode']
                    if 'status' in newImg_json_body:
                        statusMsg = newImg_json_body['status']
                    if statusCode is None and statusMsg == statusMessage and 'error' not in newImg_json_body:
                        statusCode = statusSuccessCode
                    elif  'error' in newImg_json_body and 'errorMessage' in newImg_json_body['error'] :
                        statusCode = newImg_json_body['error']['statusCode']
                    # print('statusCode=======',statusCode)  
                    # respUuid = newImg_json_body['respUuid']
                    targetSystem = newImg_json_body['targetSystem']
                    responseTimestamp = newImg_json_body['responseTimestamp']
                    internalTransactionId = newImg_json_body['header']['internalTransactionId']
                    BaasTransactionsInflight_tbl = boto3.resource('dynamodb', region_name="us-west-2").Table('BaasTransactionsInflight')
                    BaasTransactions = boto3.resource('dynamodb', region_name="us-west-2").Table('BaasTransactions')
                    # logger from dynamo db sync connector
                    logger.info('Step number: [{}], statusCode: [{}], statusMsg: [{}], respUuid: [{}], targetSystem: [{}], responseTimestamp: [{}],internalTransactionId: [{}], message: request received from dynamo db sync connector [{}]'.format(
                        "4", statusCode, statusMsg, respUuid, targetSystem, responseTimestamp,internalTransactionId, newImg_json_body ))
                    # Validating targetSystem to insert values in respective fields in BaasTrasactions table
                    internalTransactionId_exist = BaasTransactions.get_item(Key={"internalTransactionId": internalTransactionId})
                    print("internalTransactionId_exist",internalTransactionId_exist)
                    get_internalTransactionId_exist = None
                    if 'Item' in internalTransactionId_exist:
                        get_internalTransactionId_exist = BaasTransactions.get_item(Key={"internalTransactionId": internalTransactionId})['Item']
                    print("get_internalTransactionId_exist",get_internalTransactionId_exist)
                    if get_internalTransactionId_exist is None:
                        print("targetSystem", targetSystem)
                        if targetSystem == "Billing":
                            response = BaasTransactionsInflight_tbl.update_item(
                                Key={'internalTransactionId': internalTransactionId},
                                UpdateExpression="set billingRespPayload = :billingRespPayload, billingRespStatus = :billingRespStatus, billingRespTime = :billingRespTime ",
                                # ConditionExpression= 'attribute_not_exists(internalTransactionId)',
                                ExpressionAttributeValues={
                                    ':billingRespPayload': newImg_str_body,
                                    ':billingRespStatus': statusCode,
                                    ':billingRespTime': responseTimestamp
                                },
                                ReturnValues="UPDATED_NEW"
                            )
                        elif targetSystem == "Mediation":
                            response = BaasTransactionsInflight_tbl.update_item(
                                Key={'internalTransactionId': internalTransactionId},
                                UpdateExpression="set mediationRespPayload = :mediationRespPayload,mediationRespStatus = :mediationRespStatus, mediationRespTime = :mediationRespTime ",
                                # ConditionExpression= 'attribute_not_exists(internalTransactionId)',
                                ExpressionAttributeValues={
                                    ':mediationRespPayload': newImg_str_body,
                                    ':mediationRespStatus': statusCode,
                                    ':mediationRespTime': responseTimestamp
                                },
                                ReturnValues="UPDATED_NEW"
                            )
                        elif targetSystem == "Rating":
                            response = BaasTransactionsInflight_tbl.update_item(
                                Key={'internalTransactionId': internalTransactionId},
                                UpdateExpression="set ratingRespPayload = :ratingRespPayload, ratingRespStatus = :ratingRespStatus,ratingRespTime = :ratingRespTime ",
                                # ConditionExpression= 'attribute_not_exists(internalTransactionId)',
                                ExpressionAttributeValues={
                                    ':ratingRespPayload': newImg_str_body,
                                    ':ratingRespStatus': statusCode,
                                    ':ratingRespTime': responseTimestamp
                                },
                                ReturnValues="UPDATED_NEW"
                            )
                        # logger after updating baas transaction table
                        logger.info('Step number: [{}], statusCode: [{}], statusMsg: [{}], respUuid: [{}], targetSystem: [{}], responseTimestamp: [{}],internalTransactionId: [{}], message: [{}]'.format(
                            "5", statusCode, statusMsg, respUuid, targetSystem, responseTimestamp,internalTransactionId, "response update done in baas transaction table" ))
                   
            return {
                'statusCode': 200,
                'body': json.dumps({"status": "Success"})
            }
    except Exception as error:
        logger.exception("message: the error occured due to "+str(error))