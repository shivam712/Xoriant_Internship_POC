import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
from pyspark.sql import*
from pyspark.sql.functions import *
import sys

def replace_null_with_zero(dff,*arg):
    if(len(arg)!=0):
        dff1 = dff.fillna('0',list(arg))    
        #dff1 = dff1.select(*(col(c).cast("int").alias(c) for c in list(arg)))
        return dff1
    else:
        return dff

def replace_null_with_updated(dff,*arg):
    for c in list(arg):
        dff = fill_forward(dff,c,'location','date_in_dateFormat')
    return dff

def fill_forward(df, fill_column, id_column = 'location', key_column = 'date_in_dateFormat'):
    # Fill null's with last *non null* value in the window
    ff = df.withColumn(
        'fill_fwd',
        last(fill_column, True) # True: fill with last non-null
        .over(
            Window.partitionBy(id_column)
            .orderBy(key_column)
            .rowsBetween(-sys.maxsize, 0))
        )

    # Drop the old column and rename the new column
    ff_out = ff.drop(fill_column).withColumnRenamed('fill_fwd', fill_column)

    return ff_out

def calculate_7_day_average(df,id_column,new_column):
    w = (Window.partitionBy("location").orderBy(col("date_in_dateFormat").cast('long')).rowsBetween(0, 7))
    df = df.withColumn(new_column, avg(id_column).over(w))
    return df


    