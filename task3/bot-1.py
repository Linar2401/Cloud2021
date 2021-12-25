import json
import requests
import boto3
import os


def get_bucket(name):
    session = boto3.session.Session()
    s3 = session.resource(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net'
    )
    sqs = session.resource(
        service_name='sqs',
        endpoint_url='https://message-queue.api.cloud.yandex.net'
    )
    bucket = s3.Bucket(name)
    return bucket, s3, sqs


def send_message(message, photo):
    url = '{}I/sendPhoto'.format(os.getenv('TELEGRAM_URL'))
    headers = {
        'Content-Type': 'application/json'
    }
    payload = {
        "chat_id": str(os.getenv('CHAT_ID')),
        "photo": "https://storage.yandexcloud.net/{}/{}".format(os.getenv('BUCKET_NAME'), photo),
        "caption": message
    }
    r = requests.post(url, headers=headers, data=json.dumps(payload))
    return r.json()['result']['message_id']


def handler(event, context):
    print(event)
    bucket, s3, sqs = get_bucket(os.getenv('BUCKET_NAME'))
    photo_name = event['messages'][0]['details']['message']['body']
    message_id = send_message("Кто на фото?", photo_name)
    bucket.put_object(Key="unrecognized_messages/{}".format(message_id), Body=photo_name)
