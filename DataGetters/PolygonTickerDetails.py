import time
from tqdm import tqdm
import requests
import concurrent
import numpy as np
from threading import Lock, Thread

from .PolygonDataGetter import PolygonDataGetter

class PolygonTickerDetails(PolygonDataGetter):
    def __init__(self):
        super().__init__()
        self.counter = 0
        self._lock = Lock()
        self.failed = []

    def log_status(self, num_tickers):
        count = 0
        with tqdm(total=num_tickers) as pbar:
            while count < num_tickers:
                if self.counter > count:
                    with self._lock:
                        pbar.update(self.counter - count)
                        count = self.counter
                time.sleep(0.1)

    def get_data(self):
        all_tickers = [x['ticker'] for x in self.find('tickers')]

        status_monitor = Thread(target=self.log_status, args=[len(all_tickers)])
        status_monitor.start()
        num_threads = self.propertiesService.get('num_threads')
        chunks = np.array_split(all_tickers, num_threads)

        with concurrent.futures.ThreadPoolExecutor(num_threads) as executor:
            futures = []
            for chunk in chunks:
                futures.append(executor.submit(self.fetch_ticker_details, chunk))

    # Fetch ticker details
    def fetch_ticker_details(self, tickers):
        for ticker in tickers:
            data_does_not_exist = self.find_one('tickerDetails', {'ticker': ticker.upper()}) is None

            if data_does_not_exist:
                url = self.propertiesService.build_polygon_base_url(f'reference/tickers/{ticker.upper()}')
                if self.propertiesService.get('testing.disable_polygon_gets'):
                    continue

                response = requests.get(url)
                if response.status_code == 200:
                    json = response.json()
                    self.insert_one('tickerDetails', json['results'])
                    # db['tickerDetails'].insert_one(json['results'])
                    # insert_if_not_exists(json['results'], 'tickerDetails')
                else:
                    self.failed.append(ticker)

            with self._lock:
                self.counter += 1

        for ticker in self.failed:
            self.loggingService.error(f'Failed to fetch details for ticker {ticker}')
