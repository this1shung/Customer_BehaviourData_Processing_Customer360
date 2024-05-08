from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.window import Window 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.config('spark.driver.memory','4g').getOrCreate()
Filepath = "D:\\LEARNING\\Study_DE\\Data\\Dataset\\log_search\\"
Mappath1 = "D:\\PROJECT\\Data-Engineering-and-Big-Data\\ETL excersice\\mapt6\\t6.csv"
Mappath2 = "D:\\PROJECT\\Data-Engineering-and-Big-Data\\ETL excersice\\mapt7\\t7.csv"
outputpath = "D:\\OUTPUTDATA"

def ReadFileAndUnion(Path,FromDate,ToDate):
    schema = StructType([
    StructField("eventID", StringType(), True),
    StructField("datetime", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("keyword", StringType(), True),
    StructField("category", StringType(), True),
    StructField("proxy_isp", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("networkType", StringType(), True),
    StructField("action", StringType(), True),
    StructField("userPlansMap", ArrayType(StringType()), True),
    ])
    data = spark.createDataFrame([],schema = schema)

    listSTR = []
    listINT = list(range(FromDate,ToDate+1))
    for item in listINT:
        listSTR.append(str(item))
    filesname = [Path + fname for fname in listSTR]
    filecount = 1

    for name in filesname:
        df = spark.read.parquet(name)
        data = data.union(df)
        print(f"Done {filecount} day")
        filecount = filecount + 1
    
    return data

def ReadMapping(Mappath):
    df = spark.read.csv(Mappath,header = True)
    return df

def process_log_search(data):
    data = data.select('user_id','keyword')
    data = data.groupBy('user_id','keyword').count()
    data = data.withColumnRenamed('count','TotalSearch')
    data = data.orderBy('user_id',ascending = False )
    window = Window.partitionBy('user_id').orderBy(col('TotalSearch').desc())
    data = data.withColumn('Rank',row_number().over(window))
    data = data.filter(col('Rank') == 1)
    data = data.withColumnRenamed('keyword','Most_Search')
    data = data.select('user_id','Most_Search')
    return data 

def TwoMonthProcess(data1,data2,map1,map2):
    data1 = data1.withColumnRenamed('Most_Search','Most_Search_T6')
    data2 = data2.withColumnRenamed('Most_Search','Most_Search_T7')

    data1 = data1.join(map1,on='Most_Search_T6',how="inner")
    print("------Map thang 6 completed------")
    data2 = data2.join(map2,on='Most_Search_T7',how="inner")
    print("------Map thang 7 completed------")
    
    data_Union = data1.join(data2, on="user_id", how="inner")
    data_Union = data_Union.orderBy('user_id',ascending = True)
    print("------Union two file completed------")

    data_Union = data_Union.withColumn('CATEGORY_T6',trim(data_Union.Category_T6))
    data_Union = data_Union.withColumn('CATEGORY_T7',trim(data_Union.Category_T7))

    data_Union = data_Union.withColumn("Trending_Type",when(col("CATEGORY_T6")==col("CATEGORY_T7"),"Unchanged").otherwise("Changed"))
    data_Union = data_Union.withColumn("Previous", when(col("Trending_Type") != "Unchanged", concat_ws(" to ", col("CATEGORY_T6"), col("CATEGORY_T7"))).otherwise("Unchanged"))
    print("------Add new columns completed------")

    data_Union.show()

    return data_Union 

def savetopath(data,path):
    data.repartition(1).write.csv(path, header=True, mode="overwrite")
    print("------Save Completed-------")

if __name__ =='__main__':
    Data_T6 = ReadFileAndUnion(Filepath,20220601,20220614)
    Data_T7 = ReadFileAndUnion(Filepath,20220701,20220714)
    print("-----READ DATA COMPLETED-----")
    Data_T6 = process_log_search(Data_T6)
    Data_T7 = process_log_search(Data_T7)
    Map1 = ReadMapping(Mappath1)
    Map2 = ReadMapping(Mappath2)
    Final_Data = TwoMonthProcess(Data_T6,Data_T7,Map1,Map2)
    print("-----PROCESS DATA COMPLETED-----")
    savetopath(Final_Data,outputpath)


    



