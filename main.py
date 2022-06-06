import json
import argparse
import os
import datetime
import sys
from pprint import pprint
from pymongo import MongoClient, GEOSPHERE, InsertOne
from pymongo.errors import BulkWriteError

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('File',
                    metavar='file',
                    type=str,
                    help='the path to the file')

parser.add_argument('-s', '--server', default='localhost',
                    help='target server name (default is localhost)')
parser.add_argument('-uri', '--uri', help='uri (optional)')
parser.add_argument('--port', default='27017',
                    help='server port (default is 27017)')
parser.add_argument('-l', '--lowercase', type=bool, default=True,
                    help='lowercase  collection fields  (default to true)')
parser.add_argument('-d', '--database', required=True,
                    help="target database name")
parser.add_argument('-c', '--collection', required=True,
                    help='target collection to insert to')
parser.add_argument('-u', '--username', help='username (optional)')
parser.add_argument('-p', '--password', help='password (optional)')
args = parser.parse_args()

file_path = args.File
host = args.server
uri = args.uri
connection = args.connection
port = args.port
db_name = args.database
collection_name = args.collection
db_user = args.username
db_password = args.passowrd
lowercase_collection_fields = args.lowercase

if not os.path.isfile(file_path):
    print('The file specified does not exist')
    sys.exit()

connection_string = None
# defaults to localhost if no mongo URI or mongo server host is given
if not uri:
    if not db_user:
        connection_string = f'mongodb://{host}:{port}/'
    connection_string = f'mongodb://{db_user}:{db_password}/@{host}:{port}/{db_name}'

if uri:
    connection_string = uri

with open(file_path, 'r') as f:
    geojson = json.loads(f.read())

client = MongoClient(uri)

database = client[db_name]
collection = database[collection_name]
collection.create_index([("geometry", GEOSPHERE)])
bulk_requests = []
for feature in geojson['features']:
    if lowercase_collection_fields:
        feature = {k.lower(): v for k, v in feature.items()}
    feature['createdAt'] = datetime.datetime.utcnow()
    bulk_requests.append(feature)
try:
    collection.bulk_write(bulk_requests)
except BulkWriteError as e:
    pprint(e.details)
