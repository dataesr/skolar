import pymongo
from project.server.main.logger import get_logger

logger = get_logger(__name__)

MONGO_URL = 'mongodb://mongo:27017/'
client=None
def get_client():
    global client
    if client is None:
        client = pymongo.MongoClient(MONGO_URL, connectTimeoutMS=60000)
    return client

def get_oa(dois):
    logger.debug(f'getting metadata for {len(dois)} DOIs')
    _client = get_client()
    db = _client['unpaywall']
    collections = db.list_collection_names()
    collections_dates = [col for col in collections if col[0:2] == '20']
    collections_dates.sort()
    collection = db[collections_dates[-1]]
    res_loc = [e for e in collection.find({'doi': {'$in': dois}})]
    collection_global = db['global']
    res_global = [e for e in collection_global.find({'doi': {'$in': dois}})]
    for r in res_loc+res_global:
        if '_id' in r:
            del r['_id']
    assert(len(res_global) == len(res_loc))
    res = []
    for ix, e in enumerate(res_global):
        assert(e['doi'] == res_loc[ix]['doi'])
        e.update(res_loc[ix])
        res.append(e)
    return res
