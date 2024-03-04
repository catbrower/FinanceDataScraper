import pymongo

from Decorators import service
from PropertiesService import PropertiesService

@service
class DatabaseService:
    def __init__(self):
        self.propertiesService = PropertiesService()
        self.client = pymongo.MongoClient(self.propertiesService.get('mongo.host'), int(self.propertiesService.get('mongo.port')))
        try:
            self.client.server_info()
        except Exception as err:
            print(f'Cannot connect to database at: mongodb://{self.propertiesService.get("mongo.host")}:{self.propertiesService.get("mongo.port")}/')
            self.propertiesService.set_program_termination_flag()

        self.disable_inserts = self.propertiesService.get('testing.disable_inserts')

    def find(self, database, collection, query):
        return self.client[database][collection].find(query)
    
    def find_one(self, database, collection, query):
        return self.client[database][collection].find_one(query)
    
    def insert_one(self, database, collection, data):
        if not self.disable_inserts:
            self.client[database][collection].insert_one(data)

    def insert_many(self, database, collection, data):
        if not self.disable_inserts:
            self.client[database][collection].insert_many(data)

    def exists(self, database, collection, query):
        result = self.find_one(database, collection, query)
        return result != None