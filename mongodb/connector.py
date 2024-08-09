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
    
    def updateMany(self, data_model, input_data, query, query_var):
        """
            We will use this function to update data in MongoDB 
            
            "data_model": This variable will carry the model that we use to set the data to
            "input_data": This variable will have the data that we want to limit before updation
            "query": This is the filter to extract a chunk of data from the DB
            "query_var": This is the variable that we use to find every row in the data from DB
            
        """

        db = self.client["nt"]
        collection = db["insights"]
        
        final_data = list(dict([dct["column"], row.get(dct["column"])] for dct in data_model) for row in input_data)
        
        for row in final_data:
            query[query_var] = row[query_var]
            newdata = {
                "$set": row
            }

            collection.update_one(query, newdata)
            
        self.client.close()
        return {"status": 200, "message": "The request was successfully processed!"}
