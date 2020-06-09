import typing as t
import logging
import os
import datetime as dt
import requests
import pickle

import redo
import pandas as pd
import numpy as np
from assertpy import assert_that

from utils.logging import logtime, logerror

logger = logging.getLogger(__name__)


class NotAuthException(Exception):
    pass


class HolidaysApi:
    """Work with holidays API.

    api = HolidaysApi(conf.HOLIDAYS_API_URL, conf.HOLIDAYS_API_USER, conf.HOLIDAYS_API_PASSWORD)
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

    def __init__(self, url: str, user: str, pwd: str, tmp_path: str = '/tmp'):
        self.url = url
        self.user = user
        self.pwd = pwd
        self.tmp_path = tmp_path
        self._token = None
        self._token_time = None

    @property
    def token_file(self):
        return os.path.join(self.tmp_path, self.TOKEN_FILE)

    @property
    def token(self):
        assert self._token, 'No token, need to authenticate first'
        return self._token

    @token.setter
    def token(self, value: str):
        assert value
        self._token = value
        self._token_time = dt.datetime.now()

    @property
    def auth_header(self) -> dict:
        return {'Authorization': f'Bearer {self.token}'}

    def auth(self, force=False):
        """Check auth. Authenticate if not. Reauthenticate if force is set"""
        if force:
            logger.debug('Force authentication')

        if (not force) and self._can_use_token():
            return

        if (not force) and self._can_use_token_file():
            logger.debug('Loading auth token from file: %s', self.token_file)
            with logerror(logger, 'Cannot load token from file %s', self.token_file), open(self.token_file, 'rb') as f:
                self._token = pickle.load(f)
                self._token_time = dt.datetime.fromtimestamp(os.path.getmtime(self.token_file))
                assert self.token, f'Holidays API: No token in file {self.token_file}'
                return

        with logtime(logger, 'Authentication'), logerror(logger, 'Cannot auth as %s in %s', self.user, self.url):
            response = requests.post(self.url + 'auth', json={'email': self.user, 'password': self.pwd})

        assert_that(response.status_code, 'Holidays API: Response code from Auth').is_equal_to(200)
        r_json = response.json()

        assert_that(r_json, 'Holidays API: Response json from Auth').contains_key('token')
        self.token = r_json['token']

        with logerror(logger, 'Cannot save token to file %s', self.token_file), open(self.token_file, 'wb') as f:
            pickle.dump(self.token, f)

        logger.debug('Authenticated')

    def exec_json(self, path, params: dict):
        url = f'{self.url}{path}'
        with logerror(logger, 'Holidays API error: %s, %s', url, params), logtime(logger, f'Getting: {path}'):
            response = requests.get(url, params=params, headers=self.auth_header)

        if response.status_code == 401:
            raise NotAuthException(f'Holidays API: {path} with params({params})')
        assert_that(
            response.status_code, f'Holidays API: Response code from {path} with params({params})'
        ).is_equal_to(200)

        return response.json()

    def load_holidays_raw(self, date_from: dt.date, date_to: dt.date) -> t.List[dict]:
        """Returns API response json"""
        assert isinstance(date_from, dt.date)
        assert isinstance(date_to, dt.date)
        self.auth()

        # result = self.exec_json('holidays', {'from': date_from.isoformat(), 'to': date_to.isoformat()})
        # handle not auth
        result = redo.retry(
            self.exec_json, args=('holidays', {'from': date_from.isoformat(), 'to': date_to.isoformat()}),
            retry_exceptions=(NotAuthException,), cleanup=lambda: self.auth(force=True)
        )

        return result

    def load_holidays(self, date_from: dt.date, date_to: dt.date) -> pd.DataFrame:
        """Returns prepared API response as pandas df."""
        assert isinstance(date_from, dt.date)
        assert isinstance(date_to, dt.date)

        holidays_raw = self.load_holidays_raw(date_from, date_to)
        logger.debug('Holidays count: %s', len(holidays_raw))

        holidays_df = None
        # todo: improve performance
        for holiday in holidays_raw:
            df_holidays_buf = pd.DataFrame(index=pd.MultiIndex.from_product([
                [holiday['country_code']], [holiday['en_name']],
                [holiday['day_off']], [holiday['observed']],
                [holiday['created_at']], [holiday['updated_at']], holiday['dates'],

            ], names=['country_code', 'en_name', 'day_off', 'observed',
                      'created_at', 'updated_at', 'date'])).reset_index()

            holidays_df = pd.concat([holidays_df, df_holidays_buf], ignore_index=True, sort=False)

        logger.debug(holidays_df.head())  # todo: utils log df
        assert_that(holidays_df, 'Holidays df').is_not_none()
        return holidays_df

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
        dfp[countries] = dfp[countries].fillna(0).astype(np.int8)
        dfp=dfp[['date'] + countries]

        df_calendar = pd.date_range(date_from, date_to, 
                            freq='D', name='date').to_frame().reset_index(drop=True)

        df_calendar = pd.merge(df_calendar, dfp, on='date', how='left').set_index('date')

        cols = countries.copy()
        for country in countries:
            day_type_col = f'{country}_day_type'
            cols.append(day_type_col)
            df_calendar[day_type_col] = self._categorize(df_calendar[country], long_holidays)

        return df_calendar[cols]

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


    def _can_use_token(self):
        if not self._token_time:
            return False
        if (dt.datetime.now() - self._token_time) > self.TOKEN_LIFETIME:
            logger.debug('Object variable token expired')
            return False
        assert self.token
        return True

    def _can_use_token_file(self):
        if not os.path.exists(self.token_file):
            return False
        m_time = dt.datetime.fromtimestamp(os.path.getmtime(self.token_file))
        if (dt.datetime.now() - m_time) > self.TOKEN_LIFETIME:
            logger.debug('File token expired')
            return False
        return True
