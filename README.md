# holidays_api_python
Python client for Holidays API

## Using this Project

### Install from Jupyter notebook

1. In the top cell of the notebook add:
    ```python
    !pip install git+https://github.com/jpmobiletanaka/holidays_api_python
    ```
   
### Install via pip

1. if you are using virtual environment, activate it first
    ```python
    . venv/bin/activate
    ```

2. install module
    ```python
    pip install git+https://github.com/jpmobiletanaka/holidays_api_python
    ```
   
### Use module in your script or notebook

```python
import pandas as pd
from metroholidays import MetroHolidays

mh = MetroHolidays(
    url='http://holidays.revenue.metroengines.jp/api/v1/',
    user='user',
    pwd='*****'
)

df = mh.load_calendar(
    date_from=pd.to_datetime('2020-01-01'),
    date_to=pd.to_datetime('2021-12-31'),
    country_codes=['jp'],
    weekends=True
)
```
