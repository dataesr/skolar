import pymongo
MONGO_URL = 'mongodb://mongo:27017/'
def get_client():
    global client
    if client is None:
        client = pymongo.MongoClient(MONGO_URL, connectTimeoutMS=60000)
    return client
