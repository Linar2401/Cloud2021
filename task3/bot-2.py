import json
import requests
import boto3
import os

def send_photo(message, photo):
    url = '{}I/sendPhoto'.format(os.getenv('TELEGRAM_URL'))
    headers = {
        'Content-Type': 'application/json'
    }
    payload = {
        "chat_id":str(os.getenv('CHAT_ID')),
        "photo": "https://storage.yandexcloud.net/{}/{}".format(os.getenv('BUCKET_NAME'), photo),
        "caption": message
    }
    r = requests.post(url, headers=headers, data=json.dumps(payload))
    return r.json()['result']['message_id']

def remember_name(name, message_id, bucket):
    file_name = bucket.Object('unrecognized_messages/{}'.format(message_id)).get()['Body'].read().decode('utf-8')
    album = file_name.split('/')[0]
    f_name = file_name.split('/')[-2]
    original = '{}/{}'.format(album, f_name)
    try:
        images = bucket.Object('recognized_messages/{}'.format(name)).get()['Body'].read().decode('utf-8').split("|")
        images.append(original)
        bucket.put_object(Key="recognized_messages/{}".format(name), Body='|'.join(images))
    except:
        bucket.put_object(Key="recognized_messages/{}".format(name), Body=original)

def find_command(name, message_id, bucket):
    try:
        files = bucket.Object('recognized_messages/{}'.format(name)).get()['Body'].read()
        images = files.decode('utf-8')
        images_paths = images.split('|')
        print(files)
        for i, image in enumerate(images_paths):
            send_photo('{} #{}'.format(name, i), image)
    except:
        send_message("Фото не нейдено",reply=message_id)


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

def send_message(message, reply=None):
    url = '{}I/sendPhoto'.format(os.getenv('TELEGRAM_URL'))
    headers = {
        'Content-Type': 'application/json'
    }
    if not reply:
        payload = {
        "chat_id":str(os.getenv('CHAT_ID')),
        "text": message,
        }
    else:
        payload = {
        "chat_id":str(os.getenv('CHAT_ID')),
        "text": message,
        "reply_to_message_id": reply
        }
    r = requests.post(url, headers=headers, data=json.dumps(payload))
    return r.json()

def handler(event, context):
    bucket, s3, sqs = get_bucket(os.getenv('BUCKET_NAME'))
    body = json.loads(event['body'])
    message = body['message']['text']
    print(body)
    if not message.startswith("/") and 'reply_to_message' in body['message'].keys():
        send_message("Я запомнил этого человека",reply=body['message']['message_id'])
        remember_name(message, body['message']['reply_to_message']['message_id'], bucket)
    elif message.startswith("/find "):
        find_command(message[6:], body['message']['message_id'], bucket)
    else:
        send_message("Нераспознанная команда",reply=body['message']['message_id'])