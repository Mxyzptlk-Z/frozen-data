import re
import numpy as np
import pandas as pd
from datetime import datetime
from abc import ABC, abstractmethod
from typing import Union, Tuple, Dict
from dateutil.relativedelta import relativedelta

import duckdb
import chdb
from chdb.session import Session
from pymongo import MongoClient

from ..basis import frozen_config
from .base import connection_factory

# calculate extra dates for alphas to ensure valid value from start_date
START_EXT = (datetime.strptime(frozen_config.start_date, "%Y%m%d") - relativedelta(months=6)).strftime("%Y-%m-%d")
END = datetime.strptime(frozen_config.end_date, "%Y%m%d").strftime("%Y-%m-%d")


class DataLoader(ABC):
    """
    DataLoader is designed for loading raw data from original data source.
    """

    def __init__(self, database):

        self.database = database
        connection = connection_factory(database)

        if database == "chdb":
            # Specify database storage path
            storage_path = connection.data_path
            self.db = Session(path=storage_path)
            self._query(("USE BaseDB"))
        elif database == "duckdb":
            self.db = connection.data_path
        elif database == "mongodb":
            client = MongoClient(host=connection.host, port=connection.port)
            self.db = client.FrozenBacktest
        else:
            raise NotImplementedError(f"Database {database} not supported yet.")

    @abstractmethod
    def load(
        self,
    ) -> pd.DataFrame:
        
        raise NotImplementedError
    
    def _query(self, query_str: Union[str, Dict], fmt=None):

        if self.database == "chdb":
            if fmt is None:
                fmt = "CSV"
            if fmt not in ["CSV", "Arrow"]:
                raise ValueError(f"chdb only supports 'CSV' or 'Arrow' format, got '{fmt}'")
            return self.db.query(query_str, fmt=fmt)
        
        if self.database == "duckdb":
            if fmt is None:
                fmt = "dataframe"
            if fmt not in ["dataframe"]:
                raise ValueError(f"duckdb only supports 'dataframe' format, got '{fmt}'")
            with duckdb.connect(self.db, read_only=True) as conn:
                cursor = conn.cursor()
                cursor.execute(query_str)
                # Use regex to identify SELECT queries
                if not re.match(r'^\s*SELECT', query_str, re.IGNORECASE):
                    return None
                try:
                    return cursor.fetchdf()
                except Exception as e:
                    print(f"Error fetching data: {e}")
                    return None
        
        if self.database == "mongodb":
            # MongoDB query handling
            collection_name = query_str["collection"]
            collection = self.db[collection_name]
            # Regular find query
            filter_query = query_str.get("filter", {})
            projection = query_str.get("projection", None)
            if projection:
                res = pd.DataFrame(list(collection.find(filter_query, projection)))
            else:
                res = pd.DataFrame(list(collection.find(filter_query)))
            
            return res
    
    def _check_validity(self, data):
        if data.empty or data is None:
            # Data incompleteness
            raise ValueError(f"Missing data detected. Please check the integrity of {self.database} database entries.")


class TSDataLoader(DataLoader):
    """
    (T)ime-(S)eries DataLoader.
    """

    def __init__(
        self,
        database: str,
    ) -> None:
        """
        Parameters
        ----------
        database : str
            The database on disk.
        """
        super().__init__(database=database)

    def load(
        self,
        table_name: str,
        col: Union[str, Tuple] = None,  # if not specify col, all columns will be loaded
        universe: Tuple = (),
        start_date: str = None,
        end_date: str = None
    ) -> pd.DataFrame:
        """
        Parameters
        ----------
        table_name: str
            Load data from what table in the source database.
        col: Union[str, Tuple]
            The columns to be selected.
        universe: Tuple
            It marks the pool of instrument tickers to be loaded.
        start_date: str
            The start date of the time range, in format `YYYYmmdd`.
        end_date: str
            The end date of the time range, in format `YYYYmmdd`.
        
        Returns
        -------
        pd.DataFrame:
            The data loaded from the under layer source.
        """
        
        data = self._data_handler(table_name, col, universe, start_date, end_date)

        return data
    
    def _data_loader(
        self,
        table_name: str,
        universe: Tuple,
        start_date: str = None,
        end_date: str = None
    ) -> pd.DataFrame:

        if not universe:
            raise ValueError("Input `universe` cannot be an empty tuple.")

        if not start_date is None:
            start_date = datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d")
        if not end_date is None:
            end_date = datetime.strptime(end_date, "%Y%m%d").strftime("%Y-%m-%d")

        if start_date is None and end_date is None:
            _start_date, _end_date = START_EXT, END
        elif not start_date is None and end_date is None:
            _start_date, _end_date = start_date, END
        elif start_date is None and not end_date is None:
            
            _start_date, _end_date = START_EXT, end_date
        else:
            _start_date, _end_date = start_date, end_date

        if self.database == "duckdb":
            query_str = f"""
                        SELECT * FROM {table_name} 
                        WHERE ts_code IN {universe} AND trade_date >= '{_start_date}' AND trade_date <= '{_end_date}' 
                        ORDER BY trade_date DESC
                        """
        if self.database == "mongodb":
            _start_date = datetime.strptime(_start_date, "%Y-%m-%d")
            _end_date = datetime.strptime(_end_date, "%Y-%m-%d")
            query_str = {
                "collection": f"{table_name}",
                "filter": {"ts_code": {"$in": list(universe)}, "trade_date": {"$gte": _start_date, "$lte": _end_date}}
            }
        data = self._query(query_str)
        self._check_validity(data)

        return data


    def _data_transformer(
        self,
        table_name: str,
        universe: Tuple = (),
        start_date: str = None,
        end_date: str = None
    ) -> pd.DataFrame:
        """
        Transform data into multi-index format
        - level_0: ts_code
        - level_1: trade_date
        """

        data = self._data_loader(table_name, universe, start_date, end_date)
        if self.database == "mongodb":
            del data["_id"]
        # drop duplicated rows
        data.drop_duplicates(keep="first", inplace=True)
        # data transformation
        data.set_index(["ts_code", "trade_date"], inplace=True)
        data.sort_index(level=0, ascending=True, inplace=True)

        return data
    

    def _data_handler(
        self,
        table_name: str,
        col: Union[str, Tuple] = None,  # if not specify col, all columns will be loaded
        universe: Tuple = (),
        start_date: str = None,
        end_date: str = None
    ) -> pd.DataFrame:
        """
        Pipeline that load stock daily data (both volumn-price 
        and fundamental) from database and transform into formatted 
        dataframe.
        """

        data = self._data_transformer(table_name, universe, start_date, end_date)

        # data selection
        if col is None:
            col = tuple(data.columns)
        data_sel = data[col] if isinstance(col, str) else data[list(col)]

        # data formalization
        if isinstance(col, str):
            return data_sel.swaplevel().unstack().ffill()
        elif isinstance(col, tuple):
            return tuple(data_sel[c].swaplevel().unstack().ffill() for c in col)
        else:
            raise TypeError("Not supportable type for columns! Only string or tuple is allowed.")


class AltDataLoader(DataLoader):
    """
    (Alt)ernative DataLoader

    DataLoader that supports loading alternative data.
    """

    def __init__(
        self,
        database: str,
    ) -> None:
        """
        Parameters
        ----------
        database: str
            The database on disk.
        """

        super().__init__(database=database)

    def load(
            self,
            table_name: str,
            universe: Tuple = (),
            start_date: str = None,
            end_date: str = None
        ) -> pd.DataFrame:
        
        if re.search(r"list|delist", table_name, re.IGNORECASE):
            data = self._basic_loader(table_name)
        
        elif re.search(r"dividend", table_name, re.IGNORECASE):
            data = self._dividend_loader(table_name, universe)
        
        elif re.search(r"suspend", table_name, re.IGNORECASE):
            data = self._suspend_loader(table_name, start_date, end_date)

        return data
    
    def _basic_loader(self, table_name) -> pd.DataFrame:

        if self.database == "duckdb":
            query_str = f"SELECT * FROM {table_name}"
        if self.database == "mongodb":
            query_str = {
                "collection": f"{table_name}",
                "filter": {}
            }
        
        raw_data = self._query(query_str)
        self._check_validity(raw_data)
        data = self._basic_transformer(raw_data)

        return data
    
    def _basic_transformer(self, raw_data: pd.DataFrame):
        if self.database == "duckdb":
            data = raw_data.copy()
        if self.database == "mongodb":
            data = raw_data.drop("_id", axis=1)
        return data

    def _dividend_loader(self, table_name, universe: Tuple = ()) -> dict:

        if not universe:
            raise ValueError("arg `universe` must not be none.")

        if self.database == "duckdb":
            query_str = f"SELECT * FROM {table_name} WHERE ts_code IN {universe}"
        if self.database == "mongodb":
            query_str = {
                "collection": f"{table_name}",
                "filter": {"ts_code": {"$in": list(universe)}}
            }
        
        raw_data = self._query(query_str)
        self._check_validity(raw_data)
        data = self._dividend_transformer(raw_data)

        return data
    
    def _dividend_transformer(self, raw_data: pd.DataFrame):
        if self.database == "duckdb":
            data = {
                    key: group.drop(["ts_code"], axis=1).sort_values(by="ex_date").reset_index(drop=True) 
                    for key, group in raw_data.groupby("ts_code")
                }
        if self.database == "mongodb":
            data = {
                    key: group.drop(["_id", "ts_code"], axis=1).sort_values(by="ex_date").reset_index(drop=True) 
                    for key, group in raw_data.groupby("ts_code")}
        return data
    
    def _suspend_loader(self, table_name, start_date, end_date) -> pd.DataFrame:

        start_temp = frozen_config.start_date if start_date is None else start_date
        end_temp = frozen_config.end_date if end_date is None else end_date

        if self.database == "duckdb":
            _start_date = pd.to_datetime(start_temp).strftime("%Y-%m-%d")
            _end_date = pd.to_datetime(end_temp).strftime("%Y-%m-%d")
            query_str = f"SELECT * FROM {table_name} WHERE trade_date>='{_start_date}' AND trade_date<='{_end_date}'"
        if self.database == "mongodb":
            _start_date = pd.to_datetime(start_temp)
            _end_date = pd.to_datetime(end_temp)
            query_str = {
                "collection": f"{table_name}",
                "filter": {"trade_date": {"$gte": _start_date, "$lte": _end_date}}
            }
        
        raw_data = self._query(query_str)
        self._check_validity(raw_data)
        data = self._suspend_transformer(raw_data)

        return data
    
    def _suspend_transformer(self, raw_data: pd.DataFrame):
        if self.database == "duckdb":
            data = raw_data.sort_values(by=["trade_date", "ts_code"], ascending=True)
            data.set_index("trade_date", inplace=True)
        if self.database == "mongodb":
            data = raw_data.drop("_id", axis=1)
            data.sort_values(by=["trade_date", "ts_code"], ascending=True, inplace=True)
            data.set_index("trade_date", inplace=True)
        return data


class DataLoadManager:

    def __init__(self, database):

        self.database = database
    
    def load_volumn_price(
            self,
            table_name: str,
            col: Union[str, Tuple] = None,
            universe: Tuple = (),
            start_date: str = None,
            end_date: str = None,
        ):

        loader = TSDataLoader(self.database)
        data = loader.load(table_name, col, universe, start_date, end_date)

        return data
    
    def load_stock_alternative(
            self,
            table_name,
            universe: Tuple = (),
            start_date: str = None,
            end_date: str = None
        ):
        
        loader = AltDataLoader(self.database)
        data = loader.load(table_name, universe, start_date, end_date)

        return data


dataloader = DataLoadManager(frozen_config.database)
