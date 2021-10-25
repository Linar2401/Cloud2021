import boto3
import sys
import os
import json
import requests
import base64
from PIL import Image
import io


def send_2_face_recognize(image):
    url = 'https://vision.api.cloud.yandex.net/vision/v1/batchAnalyze'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Api-Key {}'.format(os.getenv('API_KEY')),
    }
    payload = {
        "analyze_specs": [{
            "content": base64.b64encode(image).decode('utf-8'),
            "features": [{
                "type": "FACE_DETECTION"
            }]
        }]
    }

    r = requests.post(url, headers=headers, data=json.dumps(payload))
    return r.json()


def crop_image(image_data, vertices, extention):
    image = Image.open(io.BytesIO(image_data))
    image = image.crop(vertices)
    byte_obj = io.BytesIO()
    image.save(byte_obj, extention)
    byte_obj.seek(0)
    return byte_obj.read()


def add_face(bucket, image, index, folder_name, name):
    name = str(folder_name + "unrecognized/"  + name + '/' + str(index) + '.' + name.split('.')[1])
    bucket.put_object(Key=name, Body=image)
    return name


def add_folders(bucket, folder_name):
    bucket.put_object(Key=str(folder_name + 'unrecognized/'))
    bucket.put_object(Key=str(folder_name + 'recognized/'))


def send_message_2_queue(queue, message):
    queue.send_message(MessageBody=str(message))


def handler(event, context):
    bucket_name = event['messages'][0]['details']['bucket_id']
    object_id = event['messages'][0]['details']['object_id']
    if 'recognized' not in object_id and '/' in object_id:
        print(bucket_name, object_id)
        bucket, s3, sqs = get_bucket(bucket_name)
        # queue = sqs.Queue('https://message-queue.api.cloud.yandex.net/b1gs4a51unfsngpt0hke/dj6000000003n6o806dt/test1')
        queue = sqs.Queue(os.getenv('QUEUE_URL'))

        folder_name = object_id.replace(object_id.split('/')[-1], '')
        add_folders(bucket, folder_name)

        image = bucket.Object(object_id).get()['Body'].read()
        vertices = send_2_face_recognize(image)
        print(vertices)
        faces = vertices['results'][0]['results'][0]['faceDetection']['faces']
        print(faces)
        print(len(faces))

        names = []

        for id, face in enumerate(faces):
            face_coord = face['boundingBox']['vertices']
            vert = (int(face_coord[0]['x']), int(face_coord[0]['y']), int(face_coord[2]['x']), int(face_coord[2]['y']))
            print(vert)
            face_image = crop_image(image, vert, 'png' if object_id.split('.')[-1] == 'png' else 'jpeg')
            name = add_face(bucket, face_image, id, folder_name, object_id.split('.')[-1])
            names.append(name)
            print(id)
        send_message_2_queue(queue, names)


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


if __name__ == '__main__':
    event = {'messages': [
        {
            'event_metadata': {
                'event_id': '9af91fdb-7a50-46a7-975f-610431b27974',
                'event_type': 'yandex.cloud.events.storage.ObjectCreate',
                'created_at': '2021-10-24T13:07:05.880972841Z',
                'tracing_context': {
                    'trace_id': '7c6fe8526d051d31',
                    'span_id': '',
                    'parent_span_id': ''
                },
                'cloud_id': 'b1gs4a51unfsngpt0hke',
                'folder_id': 'b1gsn7r1e0llnt4r9khl'
            },
            'details': {
                'bucket_id': 'd20.itiscl.ru',
                'object_id': 'TestUpload2/5.jpg'
            }
        }
    ]
    }
    handler(event)
