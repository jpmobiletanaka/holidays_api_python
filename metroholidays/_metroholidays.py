import typing as t
import logging
import os
import datetime as dt

import pandas as pd
import numpy as np

from metroholidays.utils.holidaysapi import HolidaysApi

DEFAULT_COUNTRIES = ['jp', 'cn', 'kr', 'tw', 'us', 'th']

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

    def __init__(self, url: str, user: str, pwd: str):
        self.api = HolidaysApi(url, user, pwd)

    def load_holidays_raw(self, *args, **kwargs) -> t.List[dict]:
        """Returns API response json"""
        return self.api.load_holidays_raw(*args, **kwargs)

    def load_holidays(self, *args, **kwargs) -> pd.DataFrame:
        """Returns prepared API response as pandas df."""
        return self.api.load_holidays(*args, **kwargs)

    def load_calendar(self, date_from: dt.date, date_to: dt.date,
                      country_codes: t.Optional[t.List[str]] = None,
                      long_holidays=3,
                      weekends=False) -> pd.DataFrame:
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

        if not country_codes:
            country_codes = DEFAULT_COUNTRIES

        df = self.load_holidays(date_from, date_to, country_codes)
        df['date'] = pd.to_datetime(df['date'])

        dfp = pd.pivot_table(df, values='day_off', index=['date'],
                             columns=['country_code'], aggfunc=np.max).reset_index()
        dfp = dfp[['date'] + country_codes]

        df_calendar = pd.date_range(date_from, date_to,
                                    freq='D', name='date').to_frame().reset_index(drop=True)

        df_calendar = pd.merge(df_calendar, dfp, on='date', how='left')

        if weekends:
            df_calendar['wday'] = df_calendar['date'].dt.weekday
            df_calendar.loc[df_calendar['wday'].isin([5, 6]), country_codes] = 1

        df_calendar[country_codes] = df_calendar[country_codes].fillna(0).astype(np.int8)
        df_calendar.set_index('date', inplace=True)

        cols = []
        for country in country_codes:
            day_type_col = f'{country}_day_type'
            cols.append(day_type_col)
            df_calendar[day_type_col] = self._categorize(df_calendar[country], long_holidays)

        return df_calendar[cols + country_codes]

    def _categorize(self, column: pd.Series, min_days: int):

        df_c = column.to_frame()
        df_c['wday'] = pd.to_datetime(column.index)
        df_c['wday'] = df_c['wday'].dt.weekday

        df_c['type'] = 'others'
        df_c.loc[df_c['wday'] == 4, 'type'] = 'Friday'
        df_c.loc[df_c['wday'] == 5, 'type'] = 'Saturday'
        df_c.loc[df_c['wday'] == 6, 'type'] = 'Sunday'
        df_c['tmp'] = 0
        df_c.loc[df_c['wday'].isin([5, 6]), 'tmp'] = 1
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
                for j in range(1, last + 1):
                    counts[k - j] = last
                last = 0

        last = int(counts[0])
        for k in range(1, len(counts)):
            v = counts[k]
            if v == 0:
                if last >= min_days:
                    days[k - 1] = 'last_day'
            elif v >= min_days:
                if last == 0:
                    days[k - 1] = 'first_day'
                days[k] = 'middle_days'

            last = v

        return days
