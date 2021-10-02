import time

import boto3
import sys
import os
import json


class Info:
    EXTENTIONS = ('.jpg', '.jpeg')
    BUCKET_NAME = 'd20.itiscl.ru'
    ALBUM_NAME = 'album_list'

    albums = None
    album_object = None

    @classmethod
    def get_albums(cls, bucket):
        if cls.albums is None:
            album_object = bucket.Object(cls.ALBUM_NAME)
            Info.albums = json.loads(album_object.get()['Body'].read())
        return cls.albums

    @classmethod
    def update_album(cls, bucket):
        result = bucket.put_object(Key=cls.ALBUM_NAME, Body=json.dumps(cls.albums))
        print(result)


def add_to_album(object_name, album_name, s3, bucket):
    Info.get_albums(bucket)
    if album_name not in Info.albums.keys():
        Info.albums[album_name] = []
    if (album_name + "/" + object_name) not in Info.albums[album_name]:
        Info.albums[album_name].append((album_name + "/" + object_name))


def get_bucket():
    session = boto3.session.Session()
    s3 = session.resource(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net'
    )
    bucket = s3.Bucket(Info.BUCKET_NAME)
    return bucket, s3


def upload(path, album):
    bucket, s3 = get_bucket()
    images = list(filter(lambda x: x.endswith(Info.EXTENTIONS), os.listdir(os.path.abspath(path))))
    for image in images:
        bucket.put_object(Key=str(album + "/" + image), Body=open(os.path.join(os.path.abspath(path), image), 'rb'))
        add_to_album(image, album, s3, bucket)
        Info.update_album(bucket)


def download(path, album):
    bucket, s3 = get_bucket()
    path = os.path.abspath(path)
    Info.get_albums(bucket)
    keys = Info.albums[album]

    for key in keys:
        bucket.Object(key).download_file(os.path.join(path, key.split('/')[-1]))


def get_album_list():
    bucket, s3 = get_bucket()
    Info.get_albums(bucket)
    for key in Info.albums.keys():
        print(key)


def get_photos(album):
    bucket, s3 = get_bucket()
    Info.get_albums(bucket)
    for image in Info.albums[album]:
        print(image.split("/")[-1])


if __name__ == '__main__':
    # upload("../../../images/", "1")
    # # download('../', 'LLLLLaaaa')
    # get_album_list()
    # get_photos('1')

    short_options = "udlp:a:"
    long_options = ["upload", "download", "list", "path", "album"]

    args = sys.argv
    print(args)
    if 'upload' in args or 'download' in args:
        if '-p' not in args:
            print("Path argument(-p) is missed", file=sys.stderr)
            sys.exit(1)
        if '-a' not in args:
            print("Album argument(-a) is missed", file=sys.stderr)
            sys.exit(1)
        path = args[args.index('-p') + 1]
        album = args[args.index('-a') + 1]
        print(path, album)
        if 'upload' in args:
            upload(path, album)
        else:
            download(path, album)
    if 'list' in args:
        if args[-1] == 'list':
            get_album_list()
        else:
            if '-a' not in args:
                print("Album argument(-a) is missed", file=sys.stderr)
                sys.exit(1)
            album = args[args.index('-a') + 1]
            get_photos(album)
