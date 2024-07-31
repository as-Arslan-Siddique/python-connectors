from pymongo import MongoClient

class MongoConnector:
    def __init__(self, db, collection, conn_env="local-mongo-connection-string"):
        CONNECTION_STRING = os.environ[conn_env]
        self.client = MongoClient(CONNECTION_STRING)
        
        self.db = db
        self.collection = collection
    
    def getMany(self, query={}, limit=0):
        db = self.client[self.db]
        collection = db[self.collection]
        
        cursor = collection.find(query).limit(limit)
        
        response = list(row for row in cursor)
        
        self.client.close()
        return {"status": 200, "message": "The data has been Successfully Extracted!", "response": response}
    
    def postMany(self, data):
        db = self.client[self.db]
        collection = db[self.collection]
        
        response = collection.insert_many(data)
        
        self.client.close()        
        return {"status": 200, "message": "The data has been Successfully Inserted!"}
    
    def deleteMany(self, query={}):
        db = self.client[self.db]
        collection = db[self.collection]
        
        response = collection.delete_many(query)
        
        self.client.close()
        return {"status": 200, "message": "The data has been Successfully Deleted!"}
