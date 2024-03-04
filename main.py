from PolygonDataService import PolygonDataService
from LoggingService import LoggingService
from PropertiesService import PropertiesService

# Update flags / testing variables
update_tickers = False
update_ticker_details = False
update_aggregates = True

def main():
    polygonDataService = PolygonDataService()
    polygonDataService.update_all()
    
main()