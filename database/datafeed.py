import re
import logging
import datetime
import warnings
import pandas as pd
import tushare as ts
from tqdm import tqdm
from typing import Union, Dict
from abc import ABC, abstractmethod

import duckdb
import chdb
from chdb.session import Session
from pymongo import MongoClient, errors

from .utils.log import L
from .base import datasource_cfg
from .utils.calendar import calendar
from .utils.util import rate_limiter
from .base import connection_factory

warnings.filterwarnings("ignore")
logger = logging.getLogger("frozen")


class DataFeed(ABC):

    def set_ticker(self, ticker: str):
        self.ticker = ticker
    
    @abstractmethod
    def fetch_volumn_price(self, *args, **kwargs):
        pass

    @abstractmethod
    def fetch_stock_limit(self, *args, **kwargs):
        pass

    @abstractmethod
    def fetch_stock_fundamental(self, *args, **kwargs):
        pass

    @abstractmethod
    def fetch_stock_dividend(self, *args, **kwargs):
        pass

    @abstractmethod
    def fetch_stock_basic(self, *args, **kwargs):
        pass

    @abstractmethod
    def fetch_stock_suspend(self, *args, **kwargs):
        pass


class TSDataFeed(DataFeed):
    """
    (T)u(S)hare Data Feed.

    Use tushare api to create 6 different types of feed.
    - volumn-price data
    - stock limit data
    - stock fundamental data
    - stock dividend data
    - stock suspend data
    - stock basic data
    """

    def __init__(self, ticker=None, start_date=None, end_date=None):

        token = datasource_cfg["tushare"]["token"]
        ts.set_token(token)
        self.pro = ts.pro_api()

        self.ticker = ticker
        self.START = "20050101" if start_date is None else start_date
        self.END = "20231231" if end_date is None else end_date
        self.TODAY = datetime.datetime.today().strftime("%Y%m%d")

    @rate_limiter(max_calls_per_minute=500)
    def fetch_volumn_price(self, asset="E", adj="", update=False):
        """Store instrument volumn-price data"""
        end_date = self.TODAY if update else self.END
        query = ts.pro_bar(self.ticker, start_date=self.START, end_date=end_date, asset=asset, adj=adj)
        if not self._check_validity(query):
            return 
        query["trade_date"] = pd.to_datetime(query["trade_date"])
        return query
    
    @rate_limiter(max_calls_per_minute=500)
    def fetch_stock_limit(self, update=False):
        """Store stock gain and loss limit data"""
        end_date = self.TODAY if update else self.END
        query = self.pro.stk_limit(ts_code=self.ticker, start_date=self.START, end_date=end_date)
        if not self._check_validity(query):
            return 
        query["trade_date"] = pd.to_datetime(query["trade_date"])
        return query
    
    @rate_limiter(max_calls_per_minute=500)
    def fetch_stock_fundamental(self, update=False):
        """Store stock fundamental data"""
        end_date = self.TODAY if update else self.END
        query = self.pro.daily_basic(ts_code=self.ticker, start_date=self.START, end_date=end_date, 
                                     fields="ts_code, trade_date, turnover_rate, volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm, total_share, float_share, total_mv, circ_mv")
        if not self._check_validity(query):
            return 
        query["trade_date"] = pd.to_datetime(query["trade_date"])
        return query
    
    @rate_limiter(max_calls_per_minute=500)
    def fetch_stock_dividend(self, update=False, **kwargs):
        """Store stock dividend data"""
        query = self.pro.dividend(ts_code=self.ticker, fields="ts_code, cash_div, stk_div, stk_bo_rate, stk_co_rate, ex_date")
        if not self._check_validity(query):
            return 
        query.drop_duplicates(inplace=True)
        query = query[query["ex_date"].notna()].reset_index(drop=True)
        if update:
            cutoff_date = kwargs.get("cutoff", None)
            query = query[query['ex_date']>cutoff_date]
        query["ex_date"] = pd.to_datetime(query["ex_date"])
        return query
    
    @rate_limiter(max_calls_per_minute=500)
    def fetch_stock_suspend(self, tradeday):
        """Store stock suspend status information"""
        query = self.pro.suspend_d(suspend_type="S", trade_date=tradeday)
        if not self._check_validity(query):
            return 
        query["trade_date"] = pd.to_datetime(query["trade_date"])
        return query
    
    def fetch_stock_basic(self, list_status="L"):
        """Store basic information about (de)listed stocks"""
        query = self.pro.stock_basic(exchange="", list_status=list_status, fields="ts_code, name, area, industry, fullname, enname, market, exchange, list_date")
        query["list_date"] = pd.to_datetime(query["list_date"])
        return query
    
    def _check_validity(self, data):
        try:
            if data.empty:
                return False
        except:
            if data is None:
                return False
        else:
            return True


# class CSVDataFeed(DataFeed):
#     def __init__(self, file_path):
#         self.file_path = file_path

#     def fetch_data(self):
#         data = pd.read_csv(self.file_path, parse_dates=True, index_col=0)
#         return data


class DataFeedFactory:
    @staticmethod
    def create_data_feed(source, ticker=None, start_date=None, end_date=None):
        if source == "tushare":
            return TSDataFeed(ticker, start_date, end_date)
        # elif source == "csv":
        #     return CSVDataFeed()
        else:
            raise ValueError(f"Unsupported data feed type: {source}")


class DataFeedManager:
    """
    The Data (E)xtract, (T)ransform, (L)oad Pipeline Module.
    Implement a robust ETL pipeline for efficient data processing 
    and storage.
   
   The pipeline consists of three primary stages:
    - Step 1: Extract
    Retrieve raw data from specified API, ensuring comprehensive data 
    acquisition.

    - Step 2: Transform
    Processes and refines the extracted data, applying necessary 
    cleansing, normalization, and structuring operations to prepare 
    it for analysis and storage.

    - Step 3: Load
    Dump the transformed data into target database, optimizing for 
    data integrity and query performance.

    This modular approach ensures scalability, maintainability, and 
    flexibility in handling diverse data sources and formats. The 
    pipeline is designed to accommodate potential future expansions 
    and modifications to meet evolving data management requirements.
    """

    def __init__(self, source, database, ticker_list, start_date=None, end_date=None):

        self.source = source
        self.database = database
        connection = connection_factory(database)

        if database == "chdb":
            # Specify database storage path
            storage_path = connection.data_path
            self.db = Session(path=storage_path)
            self._query("CREATE DATABASE IF NOT EXISTS BaseDB")
            self._query(("USE BaseDB"))
        elif database == "duckdb":
            self.db = connection.data_path
            self._init_duckdb()
        elif database == "mongodb":
            client = MongoClient(host=connection.host, port=connection.port)
            self.db = client.FrozenBacktest
        else:
            raise NotImplementedError(f"Database {database} not supported yet.")
        
        self.ticker_list = ticker_list
        self.start_date = "20050101" if start_date is None or str(start_date).lower() == 'none' else start_date
        self.end_date = "20231231" if end_date is None or str(end_date).lower() == 'none' else end_date
        self.data_feeds = self._load_datafeed()

        self.TODAY = datetime.datetime.today().strftime("%Y%m%d")

    def _load_datafeed(self, ticker_list=None, start_date=None, end_date=None, **kwargs):
        """
        Load data feeds for given tickers and date range.

        Args:
        -----
        ticker_list: list, optional
            List of instrument symbols. Defaults to None.
            If None, use self.ticker_list.
        start_date: str, optional
            Start date for data feed. Defaults to None.
            If None, use self.start_date.
        end_date: str, optional
            End date for data feed. Defaults to None.
            If None, use self.end_date.
        
        Returns:
        --------
        data_feeds: list
            List of DataFeed objects created for each ticker.
        """
        ticker_list = self.ticker_list if ticker_list is None else ticker_list
        start_date = self.start_date if start_date is None else start_date
        end_date = self.end_date if end_date is None else end_date
        update = kwargs.get("update", False)
        table_name = kwargs.get("table_name", "")
        date_list = kwargs.get("date_list", [])
        
        if update:
            assert len(table_name) != 0, "table name must be provided when `update` is set to `True`"
            table_date = self._get_table_date(table_name, latest=False)
            end_date = self.TODAY
        
        data_feeds = []

        if len(date_list) > 0:
            for _ in date_list:
                data_feed = DataFeedFactory.create_data_feed(self.source)
                data_feeds.append(data_feed)
            return data_feeds
        
        for ticker in ticker_list:
            if update:
                start_date = self._get_ticker_date(table_date, ticker, shift=1)
            data_feed = DataFeedFactory.create_data_feed(self.source, ticker, start_date, end_date)
            data_feeds.append(data_feed)
        
        return data_feeds

    def fetch_volumn_price_data(self, table_name, asset="E", adj="", update=False):
        # Check if table exists, create table if not exists
        if not self._check_table_exists(table_name):
            if self.database == "chdb":
                self._query(f"""
                            CREATE TABLE IF NOT EXISTS {table_name}
                            (
                                ts_code String,
                                trade_date DateTime,
                                close Float64,
                                open Float64,
                                high Float64,
                                low Float64,
                                pre_close Float64,
                                change Float64,
                                pct_chg Float64,
                                vol Float64,
                                amount Float64,
                                PRIMARY KEY (ts_code, trade_date)
                            )
                            ENGINE = ReplacingMergeTree
                            ORDER BY (ts_code, trade_date);
                            """)
            if self.database == "duckdb":
                self._query(f"""
                            CREATE TABLE IF NOT EXISTS {table_name}
                            (
                                ts_code VARCHAR,
                                trade_date TIMESTAMP,
                                open DOUBLE,
                                high DOUBLE,
                                low DOUBLE,
                                close DOUBLE,
                                pre_close DOUBLE,
                                change DOUBLE,
                                pct_chg DOUBLE,
                                vol DOUBLE,
                                amount DOUBLE,
                                PRIMARY KEY (ts_code, trade_date)
                            )
                            """)
            if self.database == "mongodb":
                self.db.create_collection(table_name)
                query_str = {
                    "collection": f"{table_name}",
                    "action": "create_index",
                    "index_fields": [("ts_code", 1), ("trade_date", 1)],
                    "unique": True
                }
                self._query(query_str)
            logger.info(f"Created table {table_name}, storing {' '.join(table_name.split('_'))} volumn-price data.")
        
        # Incremental update based on existing data
        if update:
            if self._check_table_empty(table_name):
                raise LookupError(f"Table is empty, insert data into table {table_name} first!")
            table_ticker_list = self._get_table_ticker(table_name)
            data_feeds = self._load_datafeed(table_ticker_list, update=True, table_name=table_name)
            for ticker, feed in tqdm(zip(table_ticker_list, data_feeds), total=len(data_feeds), desc="Incremeantal fetch:"):
                data = feed.fetch_volumn_price(asset=asset, adj=adj, update=True)
                if not self._check_validity(data):
                    logger.warning(f"Table {table_name}, {ticker} data is empty.")
                    continue
                self._insert_df_to_table(data, table_name)
            
            # for tickers other than existing ticker
            extra_ticker_list = list(set(self.ticker_list).difference(table_ticker_list))
            extra_data_feeds = self._load_datafeed(extra_ticker_list, end_date=self.TODAY)
            for ticker, feed in tqdm(zip(extra_ticker_list, extra_data_feeds), total=len(extra_data_feeds), desc="Incremeantal extra fetch:"):
                data = feed.fetch_volumn_price(asset=asset, adj=adj, update=True)
                if not self._check_validity(data):
                    logger.warning(f"Table {table_name}, {ticker} data is empty.")
                    continue
                self._insert_df_to_table(data, table_name)
            
            logger.info(f"{table_name} data update completed.")
        else:
            # Perform data extraction, transformation and insertion in a loop
            for ticker, feed in tqdm(zip(self.ticker_list, self.data_feeds), total=len(self.data_feeds), desc="Normal fetch:"):
                if not self._check_data_exists(table_name, ticker):
                    data = feed.fetch_volumn_price(asset=asset, adj=adj)
                    if not self._check_validity(data):
                        logger.warning(f"Table {table_name}, {ticker} data is empty.")
                        continue
                    self._insert_df_to_table(data, table_name)
                else:
                    continue
            logger.info(f"{table_name} data insertion completed.")
    
    def fetch_stock_limit_data(self, table_name="stock_daily_limit", update=False):
        if not self._check_table_exists(table_name):
            if self.database == "chdb":
                self._query(f"""
                            CREATE TABLE IF NOT EXISTS {table_name}
                            (
                                trade_date DateTime,
                                ts_code String,
                                up_limit Float64,
                                down_limit Float64,
                                PRIMARY KEY (ts_code, trade_date)
                            )
                            ENGINE = ReplacingMergeTree
                            ORDER BY (ts_code, trade_date);
                            """)
            if self.database == "duckdb":
                self._query(f"""
                            CREATE TABLE IF NOT EXISTS {table_name}
                            (
                                trade_date TIMESTAMP,
                                ts_code VARCHAR,
                                up_limit DOUBLE,
                                down_limit DOUBLE,
                                PRIMARY KEY (ts_code, trade_date)
                            )
                            """)
            if self.database == "mongodb":
                self.db.create_collection(table_name)
                query_str = {
                    "collection": f"{table_name}",
                    "action": "create_index",
                    "index_fields": [("ts_code", 1), ("trade_date", 1)],
                    "unique": True
                }
                self._query(query_str)
            logger.info(f"Created table {table_name}, storing {' '.join(table_name.split('_'))} price data.")
        
        if update:
            if self._check_table_empty(table_name):
                raise LookupError(f"Table is empty, insert data into table {table_name} first!")
            table_ticker_list = self._get_table_ticker(table_name)
            data_feeds = self._load_datafeed(table_ticker_list, update=True, table_name=table_name)
            for ticker, feed in tqdm(zip(table_ticker_list, data_feeds), total=len(data_feeds), desc="Incremeantal fetch:"):
                data = feed.fetch_stock_limit(update=True)
                if not self._check_validity(data):
                    logger.warning(f"Table {table_name}, {ticker} data is empty.")
                    continue
                self._insert_df_to_table(data, table_name)
            
            extra_ticker_list = list(set(self.ticker_list).difference(table_ticker_list))
            extra_data_feeds = self._load_datafeed(extra_ticker_list, end_date=self.TODAY)
            for ticker, feed in tqdm(zip(extra_ticker_list, extra_data_feeds), total=len(extra_data_feeds), desc="Incremeantal extra fetch:"):
                data = feed.fetch_stock_limit()
                if not self._check_validity(data):
                    logger.warning(f"Table {table_name}, {ticker} data is empty.")
                    continue
                self._insert_df_to_table(data, table_name)
            
            logger.info(f"{table_name} data update completed.")
        else:
            for ticker, feed in tqdm(zip(self.ticker_list, self.data_feeds), total=len(self.data_feeds), desc="Normal fetch:"):
                if not self._check_data_exists(table_name, ticker):
                    data = feed.fetch_stock_limit()
                    if not self._check_validity(data):
                        logger.warning(f"Table {table_name}, {ticker} data is empty.")
                        continue
                    self._insert_df_to_table(data, table_name)
                else:
                    continue
            logger.info(f"{table_name} data insertion completed.")
    
    def fetch_stock_fundamental_data(self, table_name="stock_daily_fundamental", update=False):
        if not self._check_table_exists(table_name):
            if self.database == "chdb":
                self._query(f"""
                            CREATE TABLE IF NOT EXISTS {table_name}
                            (
                                ts_code String,
                                trade_date DateTime,
                                turnover_rate Float64,
                                volume_ratio Float64,
                                pe Float64,
                                pe_ttm Float64,
                                pb Float64,
                                ps Float64,
                                ps_ttm Float64,
                                dv_ratio Float64,
                                dv_ttm Float64,
                                total_share Float64,
                                float_share Float64,
                                total_mv Float64,
                                circ_mv Float64,
                                PRIMARY KEY (ts_code, trade_date)
                            )
                            ENGINE = ReplacingMergeTree
                            ORDER BY (ts_code, trade_date);
                            """)
            if self.database == "duckdb":
                self._query(f"""
                            CREATE TABLE IF NOT EXISTS {table_name}
                            (
                                ts_code VARCHAR,
                                trade_date TIMESTAMP,
                                turnover_rate DOUBLE,
                                volume_ratio DOUBLE,
                                pe DOUBLE,
                                pe_ttm DOUBLE,
                                pb DOUBLE,
                                ps DOUBLE,
                                ps_ttm DOUBLE,
                                dv_ratio DOUBLE,
                                dv_ttm DOUBLE,
                                total_share DOUBLE,
                                float_share DOUBLE,
                                total_mv DOUBLE,
                                circ_mv DOUBLE,
                                PRIMARY KEY (ts_code, trade_date)
                            )
                            """)
            if self.database == "mongodb":
                self.db.create_collection(table_name)
                query_str = {
                    "collection": f"{table_name}",
                    "action": "create_index",
                    "index_fields": [("ts_code", 1), ("trade_date", 1)],
                    "unique": True
                }
                self._query(query_str)
            logger.info(f"Created table {table_name}, storing {' '.join(table_name.split('_'))} data.")
        
        if update:
            if self._check_table_empty(table_name):
                raise LookupError(f"Table is empty, insert data into table {table_name} first!")
            table_ticker_list = self._get_table_ticker(table_name)
            data_feeds = self._load_datafeed(table_ticker_list, update=True, table_name=table_name)
            for ticker, feed in tqdm(zip(table_ticker_list, data_feeds), total=len(data_feeds), desc="Incremeantal fetch:"):
                data = feed.fetch_stock_fundamental(update=True)
                if not self._check_validity(data):
                    logger.warning(f"Table {table_name}, {ticker} data is empty.")
                    continue
                self._insert_df_to_table(data, table_name)
            
            extra_ticker_list = list(set(self.ticker_list).difference(table_ticker_list))
            extra_data_feeds = self._load_datafeed(extra_ticker_list, end_date=self.TODAY)
            for ticker, feed in tqdm(zip(extra_ticker_list, extra_data_feeds), total=len(extra_data_feeds), desc="Incremeantal extra fetch:"):
                data = feed.fetch_stock_fundamental()
                if not self._check_validity(data):
                    logger.warning(f"Table {table_name}, {ticker} data is empty.")
                    continue
                self._insert_df_to_table(data, table_name)
            
            logger.info(f"{table_name} data update completed.")
        else:
            for ticker, feed in tqdm(zip(self.ticker_list, self.data_feeds), total=len(self.data_feeds), desc="Normal fetch:"):
                if not self._check_data_exists(table_name, ticker):
                    data = feed.fetch_stock_fundamental()
                    if not self._check_validity(data):
                        logger.warning(f"Table {table_name}, {ticker} data is empty.")
                        continue
                    self._insert_df_to_table(data, table_name)
                else:
                    continue
            logger.info(f"{table_name} data insertion completed.")
    
    def fetch_stock_dividend_data(self, table_name="stock_dividend", update=False):
        if not self._check_table_exists(table_name):
            if self.database == "chdb":
                self._query(f"""
                            CREATE TABLE IF NOT EXISTS {table_name}
                            (
                                ts_code String,
                                stk_div Float64,
                                stk_bo_rate Float64,
                                stk_co_rate Float64,
                                cash_div Float64,
                                ex_date DateTime,
                                PRIMARY KEY (ts_code, ex_date)
                            )
                            ENGINE = MergeTree
                            ORDER BY (ts_code, ex_date);
                            """)
            if self.database == "duckdb":
                self._query(f"""
                            CREATE TABLE IF NOT EXISTS {table_name}
                            (
                                ts_code VARCHAR,
                                stk_div DOUBLE,
                                stk_bo_rate DOUBLE,
                                stk_co_rate DOUBLE,
                                cash_div DOUBLE,
                                ex_date TIMESTAMP,
                                PRIMARY KEY (ts_code, ex_date)
                            )
                            """)
            if self.database == "mongodb":
                self.db.create_collection(table_name)
                query_str = {
                    "collection": f"{table_name}",
                    "action": "create_index",
                    "index_fields": [("ts_code", 1), ("ex_date", 1)],
                    "unique": True
                }
                self._query(query_str)
            logger.info(f"Created table {table_name}, storing {' '.join(table_name.split('_'))} data.")
        
        if update:
            if self._check_table_empty(table_name):
                raise LookupError(f"Table is empty, insert data into table {table_name} first!")
            table_ticker_list = self._get_table_ticker(table_name)
            data_feeds = self._load_datafeed(table_ticker_list, update=True, table_name=table_name)
            table_date = self._get_table_date(table_name)
            for ticker, feed in tqdm(zip(table_ticker_list, data_feeds), total=len(data_feeds),desc="Incremeantal fetch:"):
                next_ticker_date = self._get_ticker_date(table_date, ticker, shift=1)
                data = feed.fetch_stock_dividend(update=True, cutoff=next_ticker_date)
                if not self._check_validity(data):
                    logger.warning(f"Table {table_name}, {ticker} data is empty.")
                    continue
                self._insert_df_to_table(data, table_name)
            
            extra_ticker_list = list(set(self.ticker_list).difference(table_ticker_list))
            extra_data_feeds = self._load_datafeed(extra_ticker_list, end_date=self.TODAY)
            for ticker, feed in tqdm(zip(extra_ticker_list, extra_data_feeds), total=len(extra_data_feeds), desc="Incremeantal extra fetch:"):
                data = feed.fetch_stock_dividend()
                if not self._check_validity(data):
                    logger.warning(f"Table {table_name}, {ticker} data is empty.")
                    continue
                self._insert_df_to_table(data, table_name)
            
            logger.info(f"{table_name} data update completed.")
        else:
            for ticker, feed in tqdm(zip(self.ticker_list, self.data_feeds), total=len(self.data_feeds), desc="Normal fetch:"):
                if not self._check_data_exists(table_name, ticker):
                    data = feed.fetch_stock_dividend()
                    if not self._check_validity(data):
                        logger.warning(f"Table {table_name}, {ticker} data is empty.")
                        continue
                    self._insert_df_to_table(data, table_name)
                else:
                    continue
            logger.info(f"{table_name} data insertion completed.")
    
    def fetch_stock_suspend_data(self, table_name="stock_suspend_status", update=False):
        if not self._check_table_exists(table_name):
            if self.database == "chdb":
                self._query(f"""
                            CREATE TABLE IF NOT EXISTS {table_name}
                            (
                                ts_code String,
                                trade_date DateTime,
                                suspend_timing Nullable(String),
                                suspend_type String,
                                PRIMARY KEY trade_date
                            )
                            ENGINE = MergeTree
                            ORDER BY trade_date;
                            """)
            if self.database == "duckdb":
                self._query(f"""
                            CREATE TABLE IF NOT EXISTS {table_name}
                            (
                                ts_code VARCHAR,
                                trade_date TIMESTAMP,
                                suspend_timing VARCHAR,
                                suspend_type VARCHAR,
                            )
                            """)
            if self.database == "mongodb":
                self.db.create_collection(table_name)
            logger.info(f"Created table {table_name}, storing {' '.join(table_name.split('_'))} data.")
        
        if update:
            start_date = calendar.next_trade_day(self._get_table_date(table_name, latest=True))
            tradeday_list = calendar.get_trade_day(start_date, self.TODAY).strftime("%Y%m%d")
            date_for_query = calendar.get_trade_day(start_date, self.TODAY).strftime("%Y-%m-%d")
            data_feeds = self._load_datafeed(date_list=tradeday_list)
            for date, format_date, feed in tqdm(zip(tradeday_list, date_for_query, data_feeds), total=len(data_feeds), desc="Incremental fetch"):
                data = feed.fetch_stock_suspend(date)
                if not self._check_validity(data):
                    logger.warning(f"Table {table_name}, {date} data is empty.")
                    continue
                self._insert_df_to_table(data, table_name)
            logger.info(f"{table_name} data update completed.")
        else:
            tradeday_list = calendar.get_trade_day(self.start_date, self.end_date).strftime("%Y%m%d")
            date_for_query = calendar.get_trade_day(self.start_date, self.end_date).strftime("%Y-%m-%d")
            data_feeds = self._load_datafeed(date_list=tradeday_list)
            for date, format_date, feed in tqdm(zip(tradeday_list, date_for_query, data_feeds), total=len(data_feeds), desc="Normal fetch"):
                if not self._check_data_exists(table_name, format_date):
                    data = feed.fetch_stock_suspend(date)
                    if not self._check_validity(data):
                        logger.warning(f"Table {table_name}, {date} data is empty.")
                        continue
                    self._insert_df_to_table(data, table_name)
                else:
                    continue
            logger.info(f"{table_name} data insertion completed.")
    
    def fetch_stock_basic_data(self, table_name, list_status="L"):
        if not self._check_table_exists(table_name):
            if self.database == "chdb":
                self._query(f"""
                            CREATE TABLE IF NOT EXISTS {table_name}
                            (
                                ts_code String,
                                name String,
                                area String,
                                industry String,
                                fullname String,
                                enname String,
                                market String,
                                exchange String,
                                list_date DateTime,
                                PRIMARY KEY ts_code
                            )
                            ENGINE = ReplacingMergeTree
                            ORDER BY list_date;
                            """)
            if self.database == "duckdb":
                self._query(f"""
                            CREATE TABLE IF NOT EXISTS {table_name}
                            (
                                ts_code VARCHAR PRIMARY KEY,
                                name VARCHAR,
                                area VARCHAR,
                                industry VARCHAR,
                                fullname VARCHAR,
                                enname VARCHAR,
                                market VARCHAR,
                                exchange VARCHAR,
                                list_date TIMESTAMP
                            )
                            """)
            if self.database == "mongodb":
                self.db.create_collection(table_name)
                query_str = {
                    "collection": f"{table_name}",
                    "action": "create_index",
                    "index_fields": [("ts_code", 1)],
                    "unique": True
                }
                self._query(query_str)
            logger.info(f"Created table {table_name}, storing {' '.join(table_name.split('_'))} data.")

        data = self.data_feeds[0].fetch_stock_basic(list_status=list_status)
        if not self._check_validity(data):
            logger.warning(f"Stock basic data is empty.")
        self._insert_df_to_table(data, table_name)
        logger.info(f"{table_name} data insertion completed.")
    
    def _check_table_exists(self, table_name=""):

        if self.database == "chdb":
            query_str = f"""
                        SELECT count() AS count
                        FROM system.tables 
                        WHERE database='BaseDB' AND name='{table_name}'
                        """
            result = self._query(query_str, "Arrow")
            result_df = chdb.to_df(result)
            count = result_df["count"].iloc[0]
            res = count > 0
        
        if self.database == "duckdb":
            query_str = f"""
                        SELECT * 
                        FROM information_schema.tables 
                        WHERE table_name='{table_name}'
                        """
            res = self._query(query_str, fmt="list")
        
        if self.database == "mongodb":
            res = table_name in self.db.list_collection_names()
        
        return res
    
    def _check_table_empty(self, table_name):
        
        if self.database == "duckdb":
            query_str = f"SELECT COUNT(*) FROM {table_name}"
            result = self._query(query_str, fmt="tuple")
            res = result[0] == 0
        
        if self.database == "mongodb":
            query_str = {
                "collection": f"{table_name}",
                "action": "count_documents"
            }
            result = self._query(query_str)
            res = result == 0
        
        return res
    
    def _check_data_exists(self, table_name, data_str):

        if self.database == "duckdb":
            if "." in data_str:  # ts_code
                query_str = f"SELECT * FROM {table_name} WHERE ts_code='{data_str}'"
            else:  # trade_date
                query_str = f"SELECT * FROM {table_name} WHERE trade_date='{data_str}'"
            result = self._query(query_str, fmt="list")

        if self.database == "mongodb":
            if "." in data_str:
                query_str = {
                    "collection": f"{table_name}",
                    "action": "find",
                    "filter": {"ts_code": f"{data_str}"}
                }
            else:
                formatted_data = datetime.datetime.strptime(data_str, "%Y-%m-%d")
                query_str = {
                    "collection": f"{table_name}",
                    "action": "find",
                    "filter": {"trade_date": f"{formatted_data}"}
                }
            result = self._query(query_str)
        
        return result
    
    def _get_table_ticker(self, table_name):

        if self.database == "duckdb":
            query_str = f"SELECT DISTINCT ts_code FROM {table_name}"
            result = self._query(query_str, fmt="dataframe")
            res = result["ts_code"].tolist()
        
        if self.database == "mongodb":
            query_str = {
                "collection": f"{table_name}",
                "action": "distinct",
                "field": "ts_code"
            }
            res = self._query(query_str)
        
        return res
    
    def _insert_df_to_table(self, df, table_name):

        if self.database == "chdb":
            # Convert DataFrame to list of tuples
            records = [tuple(x) for x in df.to_numpy()]
            # format_records = ", ".join(str(tuple(row)) for row in records)
            format_records = ", ".join(
                                    f"({','.join(['NULL' if v is None else repr(v) for v in record])})"
                                    for record in records
                                )
            # Get columns from DataFrame
            columns = ", ".join(df.columns)
            # Insert data into database
            self._query(f"""
                        INSERT INTO {table_name} ({columns}) 
                        VALUES {format_records};
                        """)
        
        if self.database == "duckdb":
            with duckdb.connect(self.db) as conn:
                df.to_sql(table_name, conn, chunksize=1000000, if_exists="append", index=False)
        
        if self.database == "mongodb":
            docs = df.to_dict(orient="records")
            query_str = {
                "collection": f"{table_name}",
                "action": "insert_many",
                "documents": docs
            }
            self._query(query_str)

    
    def _delete_table(self, table_name):
        if self.database != "mongodb":
            try:
                self._query(f"DROP TABLE IF EXISTS {table_name}")
                logger.critical(f"Table {table_name} has been dropped.")
            except Exception as e:
                logger.error(f"Failed to drop table {table_name}: {e}")
        else:
            query_str = {
            "collection": f"{table_name}",
            "action": "drop_collection"
            }
            self._query(query_str)
    
    def _query(self, query_str: Union[str, Dict], fmt=None):
        """
        Execute query and return results in specified format.
        
        Args:
            query_str: SQL query string or Mongo query dictionary
            fmt: Return format
                - For chdb: "CSV" (default) or "Arrow"
                - For duckdb: "dataframe" (default), "list" or "tuple"
        
        Returns:
            Query results in specified format or None for non-SELECT queries
        """

        if self.database == "chdb":
            if fmt is None:
                fmt = "CSV"
            if fmt not in ["CSV", "Arrow"]:
                raise ValueError(f"chdb only supports 'CSV' or 'Arrow' format, got '{fmt}'")
            return self.db.query(query_str, fmt=fmt)
        
        if self.database == "duckdb":
            if fmt is None:
                fmt = "dataframe"
            if fmt not in ["dataframe", "list", "tuple"]:
                raise ValueError(f"duckdb only supports 'dataframe', 'list' or 'tuple' format, got '{fmt}'")
            with duckdb.connect(self.db) as conn:
                cursor = conn.cursor()
                cursor.execute(query_str)
                # Use regex to identify SELECT queries
                if not re.match(r'^\s*SELECT', query_str, re.IGNORECASE):
                    return None
                try:
                    if fmt == "dataframe":
                        return cursor.fetch_df()
                    elif fmt == "list":
                        return cursor.fetchall()
                    elif fmt == "tuple":
                        return cursor.fetchone()
                    else:
                        pass
                except Exception as e:
                    print(f"Error fetching data: {e}")
                    return None
        
        if self.database == "mongodb":
            # MongoDB query handling
            collection_name = query_str["collection"]
            action = query_str.get("action", "find")
            collection = self.db[collection_name]

            try:
                if action == "find":
                    # Regular find query
                    filter_query = query_str.get("filter", {})
                    projection = query_str.get("projection", None)
                    if projection:
                        res = list(collection.find(filter_query, projection))
                    else:
                        res = list(collection.find(filter_query))
                
                elif action == "create_index":
                    # Create index action
                    index_fields = query_str["index_fields"]
                    unique = query_str.get("unique", False)
                    res = collection.create_index(index_fields, unique=unique)

                elif action == "insert_many":
                    # Insert many action
                    documents = query_str["documents"]
                    res = collection.insert_many(documents)
                
                elif action == "aggregate":
                    # Aggregate action
                    agg_query = [query_str.get("aggregate", {})]
                    res = list(collection.aggregate(agg_query))
                
                elif action == "drop_collection":
                    # Drop collection action
                    res = collection.drop()
                
                elif action == "count_documents":
                    # Count documents action
                    filter_query = query_str.get("filter", {})
                    res = collection.count_documents(filter_query)
                
                elif action == "distinct":
                    # Distinct action
                    field = query_str["field"]
                    res = collection.distinct(field)

            except errors.PyMongoError as e:
                print(f"Error executing MongoDB operation: {e}")
                res = None
            
            if fmt == "dataframe":
                res = pd.DataFrame(res)
            
            return res
    
    def _check_validity(self, data):
        try:
            if data.empty:
                return False
        except:
            if data is None:
                return False
        else:
            return True
    
    def _init_duckdb(self):
        assert self.database == "duckdb"
        self._query("CREATE TABLE IF NOT EXISTS init (id VARCHAR PRIMARY KEY,)")
    
    def _get_table_date(self, table_name, latest=False) -> Union[pd.DataFrame, pd.Timestamp]:
        
        date_col = "ex_date" if table_name == "stock_dividend" else "trade_date"
        
        if self.database == "duckdb":
            query_str = f"SELECT ts_code, MAX({date_col}) AS max_date FROM {table_name} GROUP BY ts_code"
        
        if self.database == "mongodb":
            query_str = {
                "collection": f"{table_name}",
                "action": "aggregate",
                "aggregate": {
                    "$group": {
                        "_id": "$ts_code", 
                        "max_date": {"$max": f"${date_col}"}}
                }
            }
        
        table_date = self._query(query_str, fmt="dataframe")

        if latest:
            table_date = table_date["max_date"].max()
        
        return table_date
    
    def _get_ticker_date(self, table_date, ticker, shift=0) -> str:

        if self.database == "mongodb":
            ticker_date = table_date[table_date["_id"]==ticker]["max_date"].iloc[0]
        if self.database == "duckdb":
            ticker_date = table_date[table_date["ts_code"]==ticker]["max_date"].iloc[0]

        if isinstance(ticker_date, pd.Timestamp):
            ticker_date = (ticker_date + datetime.timedelta(shift)).strftime("%Y%m%d")
        if isinstance(ticker_date, str):
            ticker_date = (datetime.datetime.strptime(ticker_date, "%Y%m%d") + datetime.timedelta(shift)).strftime("%Y%m%d")

        return ticker_date


