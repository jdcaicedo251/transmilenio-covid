from pyspark.sql.functions import *

#Pyspark
def date_format(date_col = 'fechatransaccion', date_format = 'yyyyMMdd'):
    """ Transform a colum in dateformat"""
    return to_date(unix_timestamp(col(date_col), date_format).cast("timestamp"))

#Pandas
def save_table(df, name):
    """
    Save pandas dataframe in data/output/tables folder
    Input:
    -df: Pandas dataframe
    -name: str. Name of the dataframe
    """
    path = '../data/output/tables/'
    df.to_csv(path + name, sep = ',')

def drop_dates(df):
    """
    Drops days: Data these days seems to be corrupted or non-existent
    """
    drop_dates = ['2020-03-09','2020-03-10','2020-03-12','2020-03-13',
                    '2020-04-08', '2020-04-09','2020-04-10',
                     '2020-06-02','2020-06-03','2020-06-06',
                     '2020-06-07','2020-05-01','2020-05-25',
                     '2020-06-15','2020-06-22','2020-06-29',
                     '2020-07-20','2020-08-07', '2020-08-17',
                     '2020-04-27', '2020-10-12']

    res = df[~df.date.isin(drop_dates)]
    return res
