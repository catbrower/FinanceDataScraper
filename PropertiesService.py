import yaml
import multiprocessing

from Decorators import service

@service
class PropertiesService:
    def __init__(self):
        self.terminate_execution = False
        self.properties = {}

        with open('properties.yaml') as file:
            try:
                self.properties = yaml.safe_load(file)

                # Set overrideable defaults
                processors = multiprocessing.cpu_count() - 2
                self.set_property_if_not_exists('num_threads',  processors if processors > 0 else 1)
                
            except:
                print("Cannot read properties.yaml")

    def set_program_termination_flag(self):
        self.terminate_execution = True

    def should_terminate_execution(self):
        return self.terminate_execution

    def set_property_if_not_exists(self, path, value):
        if not self.exists(path):
            self.set(path, value)

    def set(self, path, value):
        if path is None or type(path) != str:
            return
        
        parts = path.strip().split('.')
        key = parts[-1]
        parts = parts[:-1]
        item = self.properties
        for part in parts:
            if part in item:
                item = item[part]
            else:
                item[part] = {}
                item = item[part]
        item[key] = value

    # Get a property from a period delimited path
    # if not exists return None
    def get(self, path):
        if path is None or type(path) != str:
            return None
        
        parts = path.strip().split('.')
        value = self.properties
        for part in parts:
            if part in value:
                value = value[part]
            else:
                return None
        return value

    # Return T / F if a property is defined
    def exists(self, path):
        if self.properties is None:
            return False
        
        return self.get(path) is not None
    
    def build_polygon_base_url(self, endpoint, params={}, version='v3'):
        params['apiKey'] = self.get('polygon.api_key')
        params_str = '&'.join([f'{key}={params[key]}' for key in params])
        base_url = f'{self.get("polygon.url")}/{version}/{endpoint}?{params_str}'
        return base_url

    def build_polygon_aggregates_url(self, ticker, date_from, date_to, timespan='second', multiplier='1'):
        dateFormat = '%Y-%m-%d'
        params = {
            'adjusted': 'true',
            'sort': 'asc',
            'limit': '50000'
        }
        url = f'aggs/ticker/{ticker.upper()}/range/{multiplier}/{timespan}/{date_from.strftime(dateFormat)}/{date_to.strftime(dateFormat)}'
        return self.build_polygon_base_url(url, version='v2', params=params)