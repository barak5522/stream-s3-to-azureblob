from pymongo import MongoClient

class MongoDB:
    def __init__(self, uri, db_name):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]

    def insert_one(self, collection_name, document):
        collection = self.db[collection_name]
        result = collection.insert_one(document)
        return result.inserted_id

    def find(self, collection_name, query):
        collection = self.db[collection_name]
        result = collection.find(query)
        return result
    
    def find_one(self, collection_name, query):
        collection = self.db[collection_name]
        result = collection.find_one(query)
        return result
    
    def update_one(self, collection_name, query, update):
        collection = self.db[collection_name]
        result = collection.update_one(query, update)
        return result.modified_count
