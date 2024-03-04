from DatabaseService import DatabaseService
from LoggingService import LoggingService
from PropertiesService import PropertiesService

# Base class for polygon data getters
class PolygonDataGetter:
    def __init__(self):
        self.databaseService = DatabaseService()
        self.loggingService = LoggingService()
        self.propertiesService = PropertiesService()
        self.polygon_database_name = 'polygon'

    # DB methods for convenience
    def insert_one(self, collection, data):
        return self.databaseService.insert_one(
            self.polygon_database_name,
            collection,
            data
        )

    def insert_many(self, collection, data):
        return self.databaseService.insert_many(
            self.polygon_database_name,
            collection,
            data
        )
    
    def find_one(self, collection, query={}):
        return self.databaseService.find_one(
            self.polygon_database_name,
            collection,
            query
        )
    
    def find(self, collection, query={}):
        return self.databaseService.find(
            self.polygon_database_name,
            collection,
            query
        )