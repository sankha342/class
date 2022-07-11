import os
import json
import boto3
from moto import mock_dynamodb2
import unittest
import mock
from unittest.mock import Mock, patch
from boto3.dynamodb.conditions import Attr
import ResponseProcessor
from ResponseProcessor import ResponseProcessor_handler

event = {'Records': [{'eventID': '7c7110d0cbe18b10419d8333cf33fc51', 'eventName': 'INSERT', 'eventVersion': '1.1',
                      'eventSource': 'aws:dynamodb', 'awsRegion': 'us-west-2',
                      'dynamodb': {'ApproximateCreationDateTime': 1656425709.0, 'Keys': {'offset': {'N': '112636'},
                                                                                         'value': {
                                                                                             'S': '            {"status":"SUCCESS","statusMessage":"Subscriber Creation Flow in Optima completed successfully","subscriptions":[{"serviceInternalId":"1604548"}],"header":{"spanId":"04e0cf33-f052-4a5e-9e16-3956b07a931c","traceId":"6dd5cffe-5234-4399-b1ea-4d54289d71d3","internalTransactionId":"998de9f6830944bd8f8c6869cd3abd1d1656421619409","partnerId":"partner1","channelId":"Online"},"targetSystem":"Billing","respUuid":"3f5ff2b1-f30d-41d1-bd56-b33e87aa3edd","responseTimestamp":"2022-06-28T14:15:07.000Z"}        '}},
                                   'NewImage': {'offset': {'N': '112636'}, 'value': {
                                       'S': '            {"status":"SUCCESS","statusMessage":"Subscriber Creation Flow in Optima completed successfully","subscriptions":[{"serviceInternalId":"1604548"}],"header":{"spanId":"04e0cf33-f052-4a5e-9e16-3956b07a931c","traceId":"6dd5cffe-5234-4399-b1ea-4d54289d71d3","internalTransactionId":"998de9f6830944bd8f8c6869cd3abd1d1656421619409","partnerId":"partner1","channelId":"Online"},"targetSystem":"Billing","respUuid":"3f5ff2b1-f30d-41d1-bd56-b33e87aa3edd","responseTimestamp":"2022-06-28T14:15:07.000Z"}        '}},
                                   'SequenceNumber': '66946600000000062352085023', 'SizeBytes': 1054,
                                   'StreamViewType': 'NEW_IMAGE'},
                      'eventSourceARN': 'arn:aws:dynamodb:us-west-2:792727994513:table/httpsink-staging-baas-response/stream/2022-06-15T10:16:17.033'}]}

returnStatement = {'statusCode': 200, 'body': '{"status": "Success"}'}

Baasflight_PutItem = {
    "internalTransactionId": {
        "S": "8225d78108c14f21a95e2f9e85b937831656930258681"
    },
    "channelId": {
        "S": "Online"
    },
    "lockStatus": {
        "S": "false"
    },
    "notificationSent": {
        "S": "false"
    },
    "overallTransactionStatus": {
        "S": "Inflight"
    },
    "partnerId": {
        "S": "partner1"
    },
    "requestPayload": {
        "S": "\n            {\"subscriber\":{\"subscriberId\":\"ck-Subscriber-84-35985efa-a381-49cf-9692-9581a5281d8f\",\"attr\":{\"MasterAccount\":\"ck-MasterAccount-762-024f66e7-9bd7-40f2-ace1-e6a6b90a7eb5\",\"BillingAccount\":\"ck-BillingAccount-762-024f66e7-9bd7-40f2-ace1-e6a6b90a7eb5\",\"WholesalePlanId\":\"DISH-Plan002\"},\"firstName\":\"Udaya\",\"lastName\":\"Shanker\",\"activeDt\":\"2022-05-02T18:00:00Z\",\"subscriptionAddresses\":[{\"addressLine1\":\"52-A-1\",\"addressLine2\":\"St Mount StreetA-1\",\"addressLine3\":\"St Mount StreetB-1\",\"city\":\"Ann Arbor\",\"state\":\"MI\",\"country\":\"USA\",\"postalCode\":\"12345\",\"addressType\":\"AEnd\"}],\"subscriptions\":[{\"subscriptionId\":\"ck-SubscriptionId-84-35985efa-a381-49cf-9692-9581a5281d8f\",\"networkName\":\"DISH\",\"billingCycle\":{\"billingCycleId\":200,\"dateOffset\":30,\"immediateChange\":true},\"offers\":[{\"offerId\":\"123\",\"allowanceDetails\":[{\"allowanceName\":\"Subscriber - Data Allowance\",\"allowanceReferenceId\":\"ef5a3a83-ce87-405d-b624-3db85efdb368\",\"allowanceInstanceId\":\"07f23b4c-bc00-11ec-aa10-0242ac110002\",\"allowanceType\":\"DDON\",\"externalName\":\"Data_Allowance\",\"allowanceGrant\":5,\"unitOfMeasure\":\"MB\",\"priority\":1,\"cycleBased\":true,\"rollOver\":true,\"allowancePeriodicity\":\"MONTH\",\"allowancePeriodicityUnits\":1,\"policies\":[{\"policyReferenceId\":\"9e2c7935-46f7-446d-9d5d-bd469886c64f\",\"policyInstanceId\":\"07f2b07d-bc00-11ec-aa10-0242ac110002\",\"policyName\":\"Subscriber - Policy Notify\",\"externalName\":\"Policy_Data_Notify2\",\"unitOfMeasure\":\"MB\",\"measurementType\":\"PERCENTAGE\",\"action\":\"NOTIFY\",\"level\":75},{\"policyReferenceId\":\"9e2c7935-46f7-446d-9d5d-bd469886c64f\",\"policyInstanceId\":\"07f2b07e-bc00-11ec-aa10-0242ac110002\",\"policyName\":\"Subscriber - Policy Notify\",\"externalName\":\"Policy_Data_Notify3\",\"unitOfMeasure\":\"MB\",\"measurementType\":\"PERCENTAGE\",\"action\":\"NOTIFY\",\"level\":50},{\"policyReferenceId\":\"9e2c7935-46f7-446d-9d5d-bd469886c64f\",\"policyInstanceId\":\"07f2b07f-bc00-11ec-aa10-0242ac110002\",\"policyName\":\"Subscriber - Policy Notify\",\"externalName\":\"Policy_Data_Notify1\",\"unitOfMeasure\":\"MB\",\"measurementType\":\"PERCENTAGE\",\"action\":\"NOTIFY\",\"level\":100},{\"policyReferenceId\":\"f995d6f9-12e1-4ca9-9463-bb87454c4c62\",\"policyInstanceId\":\"07f2b080-bc00-11ec-aa10-0242ac110002\",\"policyName\":\"Subscriber - Policy Cap\",\"externalName\":\"Policy_Data_Cap\",\"unitOfMeasure\":\"MB\",\"measurementType\":\"PERCENTAGE\",\"action\":\"CAP\",\"level\":100}]}]},{\"offerId\":\"123\",\"rate\":2000,\"allowanceDetails\":[{\"allowanceName\":\"Subscriber - Data Allowance\",\"allowanceReferenceId\":\"allowance-ref-id-1\",\"allowanceInstanceId\":\"allowance-instance-id-1\",\"allowanceType\":\"DDON\",\"externalName\":\"One Time Data Bucket (Expires)\",\"allowanceGrant\":100,\"unitOfMeasure\":\"MB\",\"priority\":1,\"cycleBased\":false,\"rollOver\":false,\"allowancePeriodicityUnits\":1,\"allowancePeriodicity\":\"MONTH\",\"policies\":[{\"policyReferenceId\":\"policy-ref-id-1\",\"policyInstanceId\":\"policy-inst-id-1\",\"policyName\":\"policy-name-1\",\"externalName\":\"Notification 1st Level\",\"action\":\"NOTIFY\",\"level\":33,\"unitOfMeasure\":\"MB\",\"measurementType\":\"PERCENTAGE\"},{\"policyReferenceId\":\"policy-ref-id-2\",\"policyInstanceId\":\"policy-inst-id-2\",\"policyName\":\"policy-name-2\",\"externalName\":\"Notification 2nd Level\",\"action\":\"NOTIFY\",\"level\":67,\"unitOfMeasure\":\"MB\",\"measurementType\":\"PERCENTAGE\"},{\"policyReferenceId\":\"policy-ref-id-3\",\"policyInstanceId\":\"policy-inst-id-3\",\"policyName\":\"policy-name-3\",\"externalName\":\"Notification 3rd Level\",\"action\":\"NOTIFY\",\"level\":90,\"unitOfMeasure\":\"MB\",\"measurementType\":\"PERCENTAGE\"},{\"policyReferenceId\":\"policy-ref-id-4\",\"policyInstanceId\":\"policy-inst-id-4\",\"policyName\":\"policy-name-4\",\"externalName\":\"Cap Level\",\"action\":\"CAP\",\"level\":100,\"unitOfMeasure\":\"MB\",\"measurementType\":\"PERCENTAGE\"}]}]}],\"subscriptionAttributes\":[{\"gpsi\":\"229858599888\",\"supi\":\"739795323932471\"}]}]},\"header\":{\"spanId\":\"dfb404c2-ccff-4471-86bd-4f9685f0c408\",\"channelId\":\"Online\",\"traceId\":\"e503ad04-56c4-41c0-818b-2a11f49f4f2f\",\"partnerId\":\"partner1\"}}\n        "
    },
    "requestProcessingStartTime": {
        "S": "04-07-2022 10:33:55"
    },
    "requestTime": {
        "S": "04-07-2022 10:24:18"
    },
    "requestType": {
        "S": "createSubscription"
    },
    "spanId": {
        "S": "dfb404c2-ccff-4471-86bd-4f9685f0c408"
    },
    "topicName": {
        "S": "5g.bss.baas.createsubscription.am.1.0"
    },
    "traceId": {
        "S": "e503ad04-56c4-41c0-818b-2a11f49f4f2f"
    }
}

Baastrans_PutItem = {
    "internalTransactionId": {
        "S": "6e085f65c7ea4219ad8c5726d5a80bb81656608042673"
    },
    "billingRespPayload": {
        "S": "            {\"status\":\"SUCCESS\",\"statusMessage\":\"Subscriber Creation Flow in Optima completed successfully\",\"subscriptions\":[{\"serviceInternalId\":\"1639169\"}],\"header\":{\"spanId\":\"d94265f0-608c-4042-9851-97e0b0827d85\",\"traceId\":\"1732d6ad-59d7-48e7-b9ba-3216527b732e\",\"internalTransactionId\":\"6e085f65c7ea4219ad8c5726d5a80bb81656608042673\",\"partnerId\":\"partner1\",\"channelId\":\"Online\"},\"targetSystem\":\"Billing\",\"respUuid\":\"c7767389-ee7c-45de-8580-09e08c50dda9\",\"responseTimestamp\":\"2022-06-30T16:54:26.000Z\"}        "
    },
    "billingRespStatus": {
        "S": "200"
    },
    "billingRespTime": {
        "S": "2022-06-30T16:54:26.000Z"
    },
    "channelId": {
        "S": "Online"
    },
    "lockStatus": {
        "S": "false"
    },
    "mediationRespPayload": {
        "S": "            {\"status\":\"SUCCESS\",\"statusMessage\":\"Subscriber Creation Flow completed successfully\",\"header\":{\"spanId\":\"d94265f0-608c-4042-9851-97e0b0827d85\",\"traceId\":\"1732d6ad-59d7-48e7-b9ba-3216527b732e\",\"internalTransactionId\":\"6e085f65c7ea4219ad8c5726d5a80bb81656608042673\",\"partnerId\":\"partner1\",\"channelId\":\"Online\"},\"targetSystem\":\"Mediation\",\"respUuid\":\"a9103ecc-520e-4265-9676-4759058cd605\",\"responseTimestamp\":\"2022-06-30T16:54:25.000Z\"}        "
    },
    "mediationRespStatus": {
        "S": "200"
    },
    "mediationRespTime": {
        "S": "2022-06-30T16:54:25.000Z"
    },
    "notificationSent": {
        "S": "true"
    },
    "overallTransactionStatus": {
        "S": "Completed"
    },
    "partnerId": {
        "S": "partner1"
    },
    "requestPayload": {
        "S": "\n            {\"subscriber\":{\"subscriberId\":\"ck-Subscriber-535-5cc19911-8b59-4730-9495-c38c63878891\",\"attr\":{\"MasterAccount\":\"ck-MasterAccount-8-3a5fefe0-75d1-49ae-8d94-c331f7859b6b\",\"BillingAccount\":\"ck-BillingAccount-8-3a5fefe0-75d1-49ae-8d94-c331f7859b6b\",\"WholesalePlanId\":\"DISH-Plan002\"},\"activeDt\":\"2022-05-01T00:00:00Z\",\"subscriptionAddresses\":[{\"addressLine1\":\"1615 17th St\",\"city\":\"Denver\",\"county\":\"Denver\",\"country\":\"USA\",\"state\":\"CO\",\"postalCode\":\"80202\",\"addressType\":\"AEnd\"}],\"subscriptions\":[{\"subscriptionId\":\"ck-SubscriptionId-535-5cc19911-8b59-4730-9495-c38c63878891\",\"billingCycle\":{\"billingCycleId\":200,\"dateOffset\":30,\"immediateChange\":true},\"offers\":[{\"offerId\":\"777\",\"rate\":0,\"allowanceDetails\":[{\"allowanceName\":\"Subscriber - Data Allowance\",\"allowanceReferenceId\":\"allowance-ref-id-1\",\"allowanceInstanceId\":\"allowance-instance-id-1\",\"allowanceType\":\"DDON\",\"externalName\":\"One Time Data Bucket (Expires)\",\"allowanceGrant\":100,\"unitOfMeasure\":\"MB\",\"priority\":1,\"cycleBased\":false,\"rollOver\":false,\"allowancePeriodicityUnits\":1,\"allowancePeriodicity\":\"MONTH\",\"policies\":[{\"policyReferenceId\":\"policy-ref-id-1\",\"policyInstanceId\":\"policy-inst-id-1\",\"policyName\":\"policy-name-1\",\"externalName\":\"Notification 1st Level\",\"action\":\"NOTIFY\",\"level\":33,\"unitOfMeasure\":\"MB\",\"measurementType\":\"PERCENTAGE\"},{\"policyReferenceId\":\"policy-ref-id-2\",\"policyInstanceId\":\"policy-inst-id-2\",\"policyName\":\"policy-name-2\",\"externalName\":\"Notification 2nd Level\",\"action\":\"NOTIFY\",\"level\":67,\"unitOfMeasure\":\"MB\",\"measurementType\":\"PERCENTAGE\"},{\"policyReferenceId\":\"policy-ref-id-3\",\"policyInstanceId\":\"policy-inst-id-3\",\"policyName\":\"policy-name-3\",\"externalName\":\"Notification 3rd Level\",\"action\":\"NOTIFY\",\"level\":90,\"unitOfMeasure\":\"MB\",\"measurementType\":\"PERCENTAGE\"},{\"policyReferenceId\":\"policy-ref-id-4\",\"policyInstanceId\":\"policy-inst-id-4\",\"policyName\":\"policy-name-4\",\"externalName\":\"Cap Level\",\"action\":\"CAP\",\"level\":100,\"unitOfMeasure\":\"MB\",\"measurementType\":\"PERCENTAGE\"}]}]},{\"offerId\":\"123\",\"rate\":2000,\"allowanceDetails\":[{\"allowanceName\":\"Subscriber - Data Allowance\",\"allowanceReferenceId\":\"allowance-ref-id-1\",\"allowanceInstanceId\":\"allowance-instance-id-1\",\"allowanceType\":\"DDON\",\"externalName\":\"One Time Data Bucket (Expires)\",\"allowanceGrant\":100,\"unitOfMeasure\":\"MB\",\"priority\":1,\"cycleBased\":false,\"rollOver\":false,\"allowancePeriodicityUnits\":1,\"allowancePeriodicity\":\"MONTH\",\"policies\":[{\"policyReferenceId\":\"policy-ref-id-1\",\"policyInstanceId\":\"policy-inst-id-1\",\"policyName\":\"policy-name-1\",\"externalName\":\"Notification 1st Level\",\"action\":\"NOTIFY\",\"level\":33,\"unitOfMeasure\":\"MB\",\"measurementType\":\"PERCENTAGE\"},{\"policyReferenceId\":\"policy-ref-id-2\",\"policyInstanceId\":\"policy-inst-id-2\",\"policyName\":\"policy-name-2\",\"externalName\":\"Notification 2nd Level\",\"action\":\"NOTIFY\",\"level\":67,\"unitOfMeasure\":\"MB\",\"measurementType\":\"PERCENTAGE\"},{\"policyReferenceId\":\"policy-ref-id-3\",\"policyInstanceId\":\"policy-inst-id-3\",\"policyName\":\"policy-name-3\",\"externalName\":\"Notification 3rd Level\",\"action\":\"NOTIFY\",\"level\":90,\"unitOfMeasure\":\"MB\",\"measurementType\":\"PERCENTAGE\"},{\"policyReferenceId\":\"policy-ref-id-4\",\"policyInstanceId\":\"policy-inst-id-4\",\"policyName\":\"policy-name-4\",\"externalName\":\"Cap Level\",\"action\":\"CAP\",\"level\":100,\"unitOfMeasure\":\"MB\",\"measurementType\":\"PERCENTAGE\"}]}]}],\"subscriptionAttributes\":[{\"gpsi\":\"919450975282\",\"supi\":\"710477213325548\"}],\"networkName\":\"DISH\"}]},\"header\":{\"spanId\":\"d94265f0-608c-4042-9851-97e0b0827d85\",\"channelId\":\"Online\",\"traceId\":\"1732d6ad-59d7-48e7-b9ba-3216527b732e\",\"partnerId\":\"partner1\"}}\n        "
    },
    "requestProcessingStartTime": {
        "S": "30-06-2022 16:54:25"
    },
    "requestTime": {
        "S": "30-06-2022 16:54:02"
    },
    "requestType": {
        "S": "createSubscription"
    },
    "spanId": {
        "S": "d94265f0-608c-4042-9851-97e0b0827d85"
    },
    "topicName": {
        "S": "5g.bss.baas.createsubscription.am.1.0"
    },
    "traceId": {
        "S": "1732d6ad-59d7-48e7-b9ba-3216527b732e"
    }
}


class TestLambdaFunction(unittest.TestCase):

    @mock.patch.dict(os.environ, {"statusMessage": "SUCCESS", "statusSuccessCode": "200"}, clear=True)
    @mock_dynamodb2
    def test_ResponseProcessor_handler(self):
        with patch('requests.post') as mock_request:
            boto3.setup_default_session()
            client = boto3.client("dynamodb", region_name='us-west-2')
            client.create_table(
                TableName="BaasTransactionsInflight",
                KeySchema=[
                    {"AttributeName": "internalTransactionId", "KeyType": "HASH"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "internalTransactionId", "AttributeType": "S"},
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 50,
                    'WriteCapacityUnits': 50
                }
            )
            client.put_item(
                TableName="BaasTransactionsInflight",
                Item=Baasflight_PutItem)

            client.create_table(
                TableName="BaasTransactions",
                KeySchema=[
                    {"AttributeName": "internalTransactionId", "KeyType": "HASH"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "internalTransactionId", "AttributeType": "S"},
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 50,
                    'WriteCapacityUnits': 50
                }
            )
            client.put_item(
                TableName="BaasTransactions",
                Item=Baastrans_PutItem)

            response = ResponseProcessor.ResponseProcessor_handler(event, {})
            print("final response:::::", response)
            self.assertEqual(returnStatement, response)


if __name__ == '__main__':
    unittest.main()
