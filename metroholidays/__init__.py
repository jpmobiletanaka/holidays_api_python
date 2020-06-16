import typing as t
import logging
import os
import datetime as dt

import pandas as pd
import numpy as np

from metroholidays.utils.holidaysapi import HolidaysApi

class NotAuthException(Exception):
    pass


class MetroHolidays:
    """Work with holidays API.

    api = MetroHolidays(conf.HOLIDAYS_API_URL, conf.HOLIDAYS_API_USER, conf.HOLIDAYS_API_PASSWORD)
    today = dt.date.today()
    holidays_df = api.load_holidays(
        date_from=dt.date(2015, 1, 1),
        date_to=today.replace(year=today.year + 1)
    )

    also you can use load_holidays_raw() to get holidays as is from API.
    and exec_json() to get another endpoint (it's necessary to use auth() in this case)
    but it's better to implement separate function if you want to use the endpoint.

    NOTE: class doesn't handle wrong/broken token file - you should remove it manually if this case.
    """

    TOKEN_FILE = 'holidays_api_token.bin'
    TOKEN_EXPIRATION = 24  # 24 hours
    TOKEN_LIFETIME = dt.timedelta(hours=TOKEN_EXPIRATION - 1)

    def __init__(self, url: str, user: str, pwd: str):
        self.api = HolidaysApi(url, user, pwd)

    def load_holidays_raw(self, date_from: dt.date, date_to: dt.date) -> t.List[dict]:
        """Returns API response json"""
        return self.api.load_holidays_raw(date_from, date_to)

    def load_holidays(self, date_from: dt.date, date_to: dt.date) -> pd.DataFrame:
        """Returns prepared API response as pandas df."""
        return self.api.load_holidays(date_from, date_to)

    def load_calendar(self, date_from: dt.date, date_to: dt.date, countries: list=None, long_holidays=3) -> pd.DataFrame:
        """Generate full date range between date_from and date_to 
           with days categorized into the following types:
           - Friday
           - Saturday
           - Sunday
           - first_day (of long holidays)
           - middle_days (of long holidays)
           - last_day (of long holidays)           
        """
        
        assert isinstance(date_from, dt.date)
        assert isinstance(date_to, dt.date)

        df = self.load_holidays(date_from, date_to)
        df['date'] = pd.to_datetime(df['date'])

        if not countries:
            countries = list(df['country_code'].unique())
        
        dfp = pd.pivot_table(df, values='day_off', index=['date'],
                    columns=['country_code'], aggfunc=np.max).reset_index()
        dfp=dfp[['date'] + countries]

        df_calendar = pd.date_range(date_from, date_to, 
                            freq='D', name='date').to_frame().reset_index(drop=True)

        df_calendar = pd.merge(df_calendar, dfp, on='date', how='left').set_index('date')
        df_calendar[countries] = df_calendar[countries].fillna(0).astype(np.int8)

        cols = []
        for country in countries:
            day_type_col = f'{country}_day_type'
            cols.append(day_type_col)
            df_calendar[day_type_col] = self._categorize(df_calendar[country], long_holidays)

        return df_calendar[cols + countries]

    def _categorize(self, column: pd.Series, min_days: int):

        df_c = column.to_frame()
        df_c['wday'] = pd.to_datetime(column.index)
        df_c['wday'] = df_c['wday'].dt.weekday

        df_c['type'] = 'others'
        df_c.loc[df_c['wday']==4, 'type'] = 'Friday'
        df_c.loc[df_c['wday']==5, 'type'] = 'Saturday'
        df_c.loc[df_c['wday']==6, 'type'] = 'Sunday'
        df_c['tmp'] = 0
        df_c.loc[df_c['wday'].isin([5,6]), 'tmp'] = 1
        df_c['tmp'] += column.fillna(0)

        counts = df_c['tmp'].values
        days = df_c['type'].values

        last = int(counts[0])
        for k in range(1, len(counts)):
            v = counts[k]
            if v > 0:
                last = last + 1
                counts[k] = last
            else:
                for j in range(1, last+1):
                    counts[k-j] = last
                last = 0

        last = int(counts[0])
        for k in range(1, len(counts)):
            v = counts[k]
            if v == 0:
                if last >= min_days:
                    days[k-1] = 'last_day'
            elif v >= min_days:
                if last == 0:
                    days[k-1] = 'first_day'
                days[k] = 'middle_days'

            last = v

        return days


