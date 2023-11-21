import time
import sys
import pandas as pd
import numpy as np
from io import StringIO
from requests import Session, RequestException, ConnectionError
from requests_ratelimiter import LimiterSession, LimiterMixin, MemoryQueueBucket
from pyrate_limiter import RequestRate, Limiter, Duration
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type


# ######################
#   Get Stock Symbols
# ######################


def get_stock_symbols():
    # Set rate limited session
    limiter = Limiter(RequestRate(2, Duration.SECOND))
    session = LimiterSession(limiter=limiter)

    # Get the lists
    tse_table = stock_symbol_crawler(2, session=session)  # 上市股票
    otc_table = stock_symbol_crawler(4, session=session)  # 上櫃股票

    # Refine the lists
    tse_table = tse_table[tse_table['CFICode'] == 'ESVUFR']
    otc_table = otc_table[otc_table['CFICode'] == 'ESVUFR']

    columns_to_drop = ['國際證券辨識號碼(ISIN Code)', '上市日', 'CFICode', '備註']

    tse_table.drop(columns=columns_to_drop, inplace=True)
    otc_table.drop(columns=columns_to_drop, inplace=True)

    column_mapping = {
        '有價證券代號': 'symbol',
        '名稱': 'name',
        '市場別': 'market',
        '產業別': 'industry'
    }
    tse_table.rename(columns=column_mapping, inplace=True)
    otc_table.rename(columns=column_mapping, inplace=True)

    tse_table['yf_symbol'] = tse_table['symbol'].astype(str) + '.TW'
    otc_table['yf_symbol'] = otc_table['symbol'].astype(str) + '.TWO'

    # Compose the result
    df = pd.concat([tse_table, otc_table]).reset_index(drop=True)

    return df


@retry(stop=stop_after_attempt(5), wait=wait_fixed(2),
       retry=retry_if_exception_type((ConnectionError, RequestException)))
def stock_symbol_crawler(mode, session=None):
    """
    :param mode: (int) 2 - 上市公司, 4 - 上櫃公司
    :param session: (requests session) default None
    :return: (pandas dataframe) Stock symbol list
    """

    if not (mode == 2) and not (mode == 4):
        print(f'The parameter "mode" need to be 2 or 4. Given: {mode}')
        sys.exit(1)

    if session is None:
        limiter = Limiter(RequestRate(2, Duration.SECOND))
        session = LimiterSession(limiter=limiter)

    # The url for data downloading
    url = f'https://isin.twse.com.tw/isin/C_public.jsp?strMode={mode}'

    # Retrieve raw data
    response = session.get(url)

    # Convert the raw data to pandas df
    stock_symbol_list = pd.read_html(StringIO(response.text))[0]

    # Clean data
    stock_symbol_list.columns = stock_symbol_list.iloc[0]
    stock_symbol_list = stock_symbol_list.iloc[2:] if mode < 11 else stock_symbol_list.iloc[1:]

    # Split the column
    original_col_name = stock_symbol_list.columns[0]
    split_col_names = original_col_name.split('及')

    split_df = stock_symbol_list[original_col_name].str.split('\u3000', expand=True)
    split_df.columns = [split_col_names[0], split_col_names[1]]

    # Drop the original column and add split columns
    stock_symbol_list.drop(original_col_name, axis=1, inplace=True)
    stock_symbol_list = pd.concat([split_df, stock_symbol_list], axis=1)

    # Double-check the 'symbol' column
    stock_symbol_list = stock_symbol_list.apply(split_symbol_name, axis=1)

    return stock_symbol_list


def split_symbol_name(row):
    # Split the value using the full-width space character
    if ' ' in row['有價證券代號']:
        parts = row['有價證券代號'].split(' ')
    else:
        parts = row['有價證券代號'].split('\u3000')

    # If there's more than one part, update the 'symbol' and 'name' columns
    if len(parts) > 1:
        row['有價證券代號'] = parts[0]
        row['名稱'] = parts[1]
    return row
