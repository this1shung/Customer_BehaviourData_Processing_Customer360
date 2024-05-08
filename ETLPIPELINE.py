from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import when
import pyspark.sql.functions as sf
import os
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import *
from pyspark.sql.functions import to_date, date_format

spark = SparkSession.builder.config('spark.driver.memory','4g').getOrCreate()

filenamedatapath = "d:\\LEARNING\\Study_DE\\Data\\Dataset\\log_content\\"
savepath = "D:\\KQ\\result\\output.csv"

def ReadFileAndUnion(path,from_date,to_date):

    schema = StructType([
        StructField("AppName", StringType(), True),
        StructField("Contract", StringType(), True),
        StructField("Mac", StringType(), True),
        StructField("TotalDuration", IntegerType(), True),
        StructField("Date", StringType(), True)
    ])
    
    outputData = spark.createDataFrame([], schema=schema)

    listStr = []
    listInt = list(range(from_date,to_date + 1))
    for item in listInt:
        listStr.append(str(item))
    filesname = [path + fname + ".json" for fname in listStr]

    for name in filesname:
        df = spark.read.json(name)
        df = df.select('_source.*')

        date = os.path.basename(name)
        date_str = date.split('.')[0]

        df = df.withColumn("Date",lit(date_str))

        outputData = outputData.union(df)

        print("Done")
    
    outputData = outputData.withColumn('Contract',trim(outputData.Contract))
    return outputData

def etl(data):
    data = data.withColumn("Type",when((col("AppName") == 'CHANNEL') | (col("AppName") =='DSHD')| (col("AppName") =='KPLUS')| (col("AppName") =='KPlus'), "Truyền Hình")
        .when((col("AppName") == 'VOD') | (col("AppName") =='FIMS_RES')| (col("AppName") =='BHD_RES')| 
             (col("AppName") =='VOD_RES')| (col("AppName") =='FIMS')| (col("AppName") =='BHD')| (col("AppName") =='DANET'), "Phim Truyện")
        .when((col("AppName") == 'RELAX')|(col("AppName") == 'APP'), "Giải Trí")
        .when((col("AppName") == 'CHILD'), "Thiếu Nhi")
        .when((col("AppName") == 'SPORT'), "Thể Thao")
        .otherwise("Error"))
    
    print("Chuẩn hóa AppName xong")

    data = data.groupBy('Contract','Type','Date').sum('TotalDuration').withColumnRenamed('sum(TotalDuration)','TotalDuration')
    print("GroupBy xong")

    data = data.groupBy('Contract','Date').pivot('Type').sum('TotalDuration')
    print("Pivot xong")

    data = data.withColumnsRenamed({'Giải Trí':'RelaxDuration','Phim Truyện':'MovieDuration',
                                                'Thiếu nhi':'ChildDuration','Thể Thao':'SportDuration','Truyền Hình':'TVDuration'})
    data = data.fillna(0)

    most_watch = greatest(col("RelaxDuration"),col("MovieDuration"),col("ChildDuration"),col("SportDuration"),col("TVDuration"))

    data = data.withColumn("Most_Watch",when((col("RelaxDuration") == most_watch),'Relax').
              when((col("MovieDuration") == most_watch),'Movie').
              when((col("ChildDuration") == most_watch),'Child').
              when((col("SportDuration") == most_watch),'Sport').
              when((col("TVDuration") == most_watch),'TV'))
    
    print("Đã thêm Mostwatch")
    
    cust_taste = concat_ws(',',when(col("RelaxDuration") != 0 ,'Relax'),
                       when(col("MovieDuration") != 0 ,'Movie'),
                       when(col("ChildDuration") != 0 ,'Child'),
                       when(col("SportDuration") != 0 ,'Sport'),
                       when(col("TVDuration") != 0 ,'TV'))

    data = data.withColumn('Customer_Taste', cust_taste)
    data = data.withColumn("Customer_Taste",when(col('Customer_Taste') == 'Relax,Movie,Child,Sport,TV','ALL').otherwise(col("Customer_taste")))

    print("Đã thêm Customer_Taste")

    count_active_date = data.groupby("Contract").agg(count(col("Contract")).alias("Number_Active_Date"))

    data = data.join(count_active_date,on = "Contract",how = 'left')

    data = data.withColumn('Activeness', format_number((col("Number_Active_Date") / 30) * 100, 2))

    data = data.withColumn("Date",date_format(to_date(col('Date'),'yyyyMMdd'),'yyyy-MM-dd'))

    print("Đã thêm Activeness")

    return data

def import_To_MySql(data,dbname,table_name,username,password):
    
    print("Starting Import to MySQL")
    data.write \
    .mode("append") \
    .format("jdbc") \
    .option("driver", "com.mysql.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/"+ dbname) \
    .option("dbtable", table_name) \
    .option("user", username) \
    .option("batchsize",20000) \
    .option("password", password) \
    .save()
    print("Import to MySQL completed")

def savetopath(data,path):
    data.repartition(1).write.csv(path, header=True, mode="overwrite")
    print("Save Completed")

if __name__ =='__main__':
    df = etl(ReadFileAndUnion(filenamedatapath,20220401,20220430))
    #import_To_MySql(df,"storagedb","customer_info","root",1234)
    savetopath(df,savepath)


