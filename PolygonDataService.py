from tqdm import tqdm
from polygon import RESTClient

from Decorators import service
from PropertiesService import PropertiesService
from DatabaseService import DatabaseService
from DataGetters import PolygonAggregates, PolygonTickerDetails

@service
class PolygonDataService:
    def __init__(self):
        self.propertiesService = PropertiesService()
        self.databaseService = DatabaseService()
        self.polygon_client = RESTClient(api_key=self.propertiesService.get('polygon.api_key'))

    # TODO read from database and return these
    def get_all_tickers(self):
        return None
    
    def update_all(self):
        # if self.propertiesService.get('update.polygon_tickers'):
        #     self.update_tickers()

        if self.propertiesService.get('update.polygon_ticker_details'):
            self.update_ticker_details()

        if self.propertiesService.get('update.polygon_aggregates'):
            self.update_aggregates()
    
    def update_tickers(self):
        for x in tqdm(self.polygon_client.list_tickers()):
            if self.databaseService.exists('polygon', 'tickers', query={'ticker': x.__dict__}):
                self.databaseService.insert_one(x.__dict__, 'tickers')

    def update_ticker_details(self):
        tickerDetailGetter = PolygonTickerDetails()
        tickerDetailGetter.get_data()

    def update_aggregates(self):
        aggregateDataGetter = PolygonAggregates()
        aggregateDataGetter.get_data()