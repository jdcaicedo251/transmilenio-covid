#!/usr/bin/env python
# coding: utf-8

# # Clean Transactions
#
# This notebook details the process to clean all the transactions data. There are two main objectives.
# - 1. Eliminate certain transactions(E.g., Transfers, double validations)
# - 2. Standarize stations.
#
# All changes are supported by a preliminary analysis of the data contain in the notebook: "estaciones_preguntas"
#
# The final product will be a function to clean transactions that can be use across all other analysis notebooks

import os
import warnings
warnings.filterwarnings('ignore')

import pyspark as ps
sc = ps.SparkContext(appName="transactions_cleaning")

from pyspark.sql import * #This enables the SparkSession object
spark = SparkSession.builder        .master("local")         .appName("transactions_cleaning")         .getOrCreate()

from os import path
import time
import random
#import pandas as pd
#import numpy as np
import glob

from pyspark.sql.functions import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import pow, col, sqrt
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType


# ### Dropping transfers
# There are 3 types of transfers that will be dropped:
#
# - Transactions that are transfers (Cost of transactions is zero)
# - Transactions that by the same card and the same station within 30 mins
# - Transactions at stations and access points that seems to be transfers as well.

def create_datetime_col(df):
    df = df.withColumn("datetime_str", concat(col('fechatransaccion'),lit(' '), col('horatransaccion')))
    return df.withColumn("dt", to_timestamp(df.datetime_str, 'yyyyMMdd HH:mm:ss'))


def drop_transactions(df, station, access_point = None):
    """ Drops the transactions done in an given station and access point(optional)
    Input:
    - df: PySpark DataFrame. Raw transactions
    - stations: 'str'. A string that helps identify the station.
    - access_point: 'str'. A string that helps identify the access point.
    """

    if access_point is not None:
        station_name = df.nombreestacion.contains(station)
        entrance = df.nombreaccesoestacion.contains(access_point)
        filters = station_name & entrance
    else:
        filters = df.nombreestacion.contains(station)
    return df.filter(~filters)


def drop_transafers(df):
    """ Drops all transfer transaction within the system. A tranfer transaction has valor = 0"""
    return df.filter(~(df.valor == 0))


def drop_duplicates(df):
    """ Drops transactions made by the same id in the same station within 30mins"""
    w = Window().partitionBy(col("card_id")).orderBy(col("dt") )
    df2 = df.select("*", lag(df.nombreestacion).over(w).alias("lag_station"))
    df2 = df2.select("*", lag(df.dt).over(w).alias("lag_dt"))
    df2 = df2.withColumn("diff_seconds", col("dt").cast("long")- col("lag_dt").cast("long"))
    df2 = df2.withColumn("same_station", col('nombreestacion') == col('lag_station'))

    mask = (df2.same_station) & (df2.diff_seconds < 1800)#1800 seconds = 30 mins
    df2 = df2.withColumn("drop_value", mask).na.fill(False)
    return df2.filter(~df2.drop_value)


def clean_transactions(transactions):
    """ Clean raw transactions data by eliminating transactions transfers and duplicates"""

    #Preprocessing
    df = create_datetime_col(transactions)

    #Dropping transactions at stations and entry point
    to_drop_transactions = {"02000" : "(09) PLATAFORMA 2 DESALIMENTACI", "10000" : "(02) DESALIME",
                        "09000" : "DESALIME", "05100" : "(MA) DESALIMENTACION CASTIL",
                        "10005" : "(04) PISO 2 DESALIME", "08000" : "(05) DESALIME",
                        "08100" : None, "40000" : None, "22000": None} #22000 is "Estacion virtual"

#     cols = ['fechaclearing','fechatransaccion','horatransaccion','nombrefase', 'nombreemisor','nombrelinea',
#             'nombreestacion','nombreaccesoestacion', 'nombredispositivo','nombreperfil','numerotarjeta',
#             'valor','nombretipotarjeta', 'dt', 'diff_seconds']
#     cols = ['dt', 'nombreestacion','nombreaccesoestacion','numerotarjeta',
#             'valor' , 'diff_seconds']

    for key, value in to_drop_transactions.items():
        df = drop_transactions(df, key, value)

    df = drop_transafers(df)
    df = drop_duplicates(df)
    return df

def clean_stations(transactions, stations):
    """ Cleans station """
    df = transactions.join(stations,
                        how = 'left',
                        on = ['nombreestacion','nombreaccesoestacion'])

    cols = ['card_id','dt','fechatransaccion','horatransaccion',
            'station_name','valor','diff_seconds']

    return df.select(cols)

def id_map(df, new_ids):
    ''' Returns a dataframe with a new colum containing new_IDs
    Input:
    - dataFrame: PySpark dataframe with a column 'numerotarjeta' as ID
    - new_ids: mapping of 'numerotarjetas' with a new (simple/shorter) id
    '''

    cols = ['card_id', 'fechatransaccion','horatransaccion','nombreestacion','nombreaccesoestacion','valor']

    return df.join(new_ids, on = 'numerotarjeta', how = 'inner').select(cols)



transactions_path = "../../../../../../gs_validaciones_tm/validaciones_tmsa/validacionTroncal/*.csv"
input_path = '../data/input/'
output_path = '../data/output/tables/'

transactions = spark.read.option("header", "true").option("sep", ";").csv(transactions_path)
id_mapping = spark.read.csv(output_path + 'id_mapping.csv', header =True, sep = ',')
stations = spark.read.csv(input_path + 'stations.csv', header =True, sep = ',')


final_transactions = transactions.transform(lambda df: id_map(df, id_mapping)) \
                                 .transform(lambda df: clean_transactions(df)) \
                                 .transform(lambda df: clean_stations(df, stations))

final_transactions.write.csv(output_path + 'transactions.csv', mode = 'overwrite', header = True)
