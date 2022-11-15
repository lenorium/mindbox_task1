import datetime

import numpy as np
import pandas as pd
from randomtimestamp import randomtimestamp


def init_dataframe() -> pd.DataFrame:
    row_count = 1000

    customers_count = 100
    products_count = 100

    end_dt = datetime.datetime.now()
    start_dt = end_dt - datetime.timedelta(minutes=30)

    return pd.DataFrame({'customer_id': np.random.randint(0, customers_count, size=row_count),
                         'product_id': np.random.randint(0, products_count, size=row_count),
                         'timestamp': [randomtimestamp(start=start_dt, end=end_dt) for _ in range(row_count)]})


def sessionize(df: pd.DataFrame) -> pd.DataFrame:
    df.sort_values(by=['customer_id', 'timestamp'], inplace=True)
    df.reset_index(inplace=True, drop=True)

    gap = df['timestamp'].diff()
    customer_delta = df['customer_id'].diff()
    new_session = gap.gt(pd.Timedelta('3T')) | customer_delta
    df['session_id'] = new_session.cumsum()
    return df


if __name__ == '__main__':
    df = init_dataframe()
    df = sessionize(df)
    print(df)
