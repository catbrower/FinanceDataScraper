from tqdm import tqdm
import time
import random
import requests
import concurrent
import numpy as np
from datetime import datetime
from threading import Lock, Thread

from Util import weekDayGenerator
from .PolygonDataGetter import PolygonDataGetter

class PolygonAggregates(PolygonDataGetter):
    def __init__(self):
        super().__init__()
        self.counter = 0
        self._lock = Lock()
        self.failed = []
        self.shouldLogStatus = True

    def log_status(self, num_tickers):
        count = 0
        with tqdm(total=num_tickers) as pbar:
            while count < num_tickers and self.shouldLogStatus:
                if self.counter > count:
                    with self._lock:
                        pbar.update(self.counter - count)
                        count = self.counter
                time.sleep(0.1)

    def get_data(self):
        # all_tickers = [x['ticker'] for x in db['tickers'].find({})]
        all_tickers = [x['ticker'] for x in self.find('tickers')]
        random.shuffle(all_tickers)

        # TODO allow a testing variable to do this through the yaml
        # all_tickers = ['SPY']

        status_monitor = Thread(target=self.log_status, args=[len(all_tickers)])
        status_monitor.start()
        num_threads = self.propertiesService.get('num_threads')

        chunks = np.array_split(all_tickers, num_threads)

        with concurrent.futures.ThreadPoolExecutor(num_threads) as executor:
            futures = []
            for chunk in chunks:
                futures.append(executor.submit(self.fetch_aggregates, chunk))

        self.shouldLogStatus = False
        status_monitor.join()

    # Fetch ticker details
    def fetch_aggregates(self, tickers):
        if tickers is None:
            print('Tickers is none')
            return

        endDate = datetime(datetime.now().year, datetime.now().month, datetime.now().day)
        startDate = datetime(datetime.now().year - self.propertiesService.get('polygon.years_of_history'), datetime.now().month, datetime.now().day)
        try:
            for ticker in tickers:
                if ticker is None:
                    print('Ticker is None')
                    continue

                # Some tickers are listed recently fetch that date and check so time isn't wasted fetching
                tickerDetail = self.find_one('tickerDetails', query={'ticker': ticker})
                if tickerDetail is not None and 'list_date' in tickerDetail:
                    listDate = datetime.strptime(tickerDetail['list_date'], '%Y-%m-%d')
                    if listDate > startDate:
                        startDate = listDate

                # Fetching fails A LOT so a fetch record was created to pickup where we left off
                # Check against this record and skip days that we already have
                # fetchRecord = [x['date'] for x in db['aggregates_fetch_record'].find({'ticker': ticker})]
                fetchRecord = [x['date'] for x in self.find('aggregates_fetch_record', {'ticker': ticker})]

                if startDate is None or endDate is None:
                    print('Encounted None type date')
                    continue

                # certain assets classes need to be ignored b/c subscription reasons
                if sum(map(ticker.__contains__, self.propertiesService.get('polygon.ignore_assets'))) < 1:
                    for day in weekDayGenerator(startDate, endDate):
                        strDate = day.strftime('%Y-%m-%d')
                        if strDate in fetchRecord:
                            continue

                        # Perform fetch
                        url = self.propertiesService.build_polygon_aggregates_url(ticker, day, day)
                        if self.propertiesService.get('testing.disable_polygon_gets'):
                            continue
                        
                        response = requests.get(url)
                        if response.status_code == 200:
                            json = response.json()
                            if 'results' in json:
                                for index, item in enumerate(json['results']):
                                    item['ticker'] = ticker
                                    # item['_id'] = hashlib.md5(''.join([str(item[key]) for key in item]).encode('utf-8')).hexdigest()
                                    json['results'][index] = item
                                
                                try:
                                    # db['aggregates'].insert_many(json['results'])
                                    self.insert_many('aggregates', json['results'])
                                    self.insert_one('aggregates_fetch_record', {
                                        'ticker': ticker,
                                        'date': strDate,
                                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                        'count': json['resultsCount']
                                    })
                                    # db['aggregates_fetch_record'].insert_one()
                                except Exception as err:
                                    self.loggingService.error('DB insert failed')
                                    self.loggingService.error(err)
                        else:
                            self.loggingService.warn(f'Failed to get aggregates for {ticker} for date {day}. Code: {response.status_code}')

                with self._lock:
                    self.loggingService.info(f'Updated ticker {ticker} for dates {startDate} - {endDate}')
                    self.counter += 1

                for ticker in self.failed:
                    self.loggingService.error(f'Failed to fetch details for ticker {ticker}')

        except Exception as err:
            self.loggingService.error(f'Processing ticker {ticker} failed on range {startDate} - {endDate}')
            self.loggingService.error(err)
