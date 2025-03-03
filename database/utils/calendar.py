import numpy as np
import pandas as pd
import datetime
import holidays
import chinese_calendar as cn_calendar
import pandas_market_calendars as mcal
from dateutil.relativedelta import relativedelta


class Calendar:
    '''The market calendar adjustment module.'''

    @staticmethod
    def period_delta(freq):
        '''
        Transform the given frequency to a timedelta 
        that can be directly applied to datetime objects.
        '''
        ONE_DAY = datetime.timedelta(days=1)
        ONE_WEEK = relativedelta(weeks=1)
        TWO_WEEK = relativedelta(weeks=2)
        ONE_MONTH = relativedelta(months=1)
        if freq == 'D':
            timedelta = ONE_DAY
        elif freq == 'W':
            timedelta = ONE_WEEK
        elif freq == '2W':
            timedelta = TWO_WEEK
        elif freq == 'M':
            timedelta = ONE_MONTH
        
        return timedelta


    def is_tradeday(self, date):
        '''Decide whether the given date is trade day.'''

        return False if date.weekday() in holidays.WEEKEND or cn_calendar.is_holiday(date) else True


    def get_trade_day(self, start_date, end_date):
        '''Generate a list of trade days between the given start date and end date.'''

        return pd.DatetimeIndex([date for date in pd.date_range(start_date, end_date) if self.is_tradeday(date)])
    

    @staticmethod
    def next_trade_day(date):
        '''Gives the following trade day after the given date.'''

        date = pd.to_datetime(date) if isinstance(date, str) else date
        
        ONE_DAY = datetime.timedelta(days=1)
        next_trade_day = date + ONE_DAY
        while next_trade_day.weekday() in holidays.WEEKEND or cn_calendar.is_holiday(next_trade_day):
            next_trade_day += ONE_DAY
        
        return next_trade_day


calendar = Calendar()