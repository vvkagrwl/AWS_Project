import random
import hashlib
import json
import sys
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws,col
from pyspark.sql.types import DecimalType,StringType
from pyspark.sql.functions import to_date,col,udf
import boto3
from pyspark.sql import functions as f
from datetime import date
from pyspark.sql.utils import AnalysisException


rand_int = random.randint(0,100000000)
spark = SparkSession.builder.appName('Transformation'+str(rand_int)).getOrCreate()

# For Delta

spark.sparkContext.addPyFile("/usr/lib/spark/jars/delta-core_2.12-0.8.0.jar")

from delta import *

class Configuration:
    def __init__(self,spark_config_loc,dataset):

        self.s3 = boto3.resource('s3')
        self.datasetname = dataset

        #set spark configuration
        self.setSparkConfig(spark_config_loc)

        # Extracting information from app_config file
        self.conf = self.fetchConfig()

        self.raw_bucket = self.conf["raw-bucket"]
        self.staging_bucket = self.conf["staging-bucket"]

        # Maskable columns of dataset
        self.maskData = self.conf['mask-'+dataset]
        self.df_maskColumns = self.maskData['masking-cols']
        
        #primary key values
        self.pii_cols = self.conf[self.datasetname+"-pii-cols"]

        #Data locations of the data residing in RawZone
        self.df_rawzone_src = self.maskData['source']['data-location']
        
        # Datatype conversions 
        self.df_datatypes = self.maskData['datatype-update-cols']
        
        self.stagingzone_dest = self.maskData['destination']['data-location']

        self.df_partition_cols = self.maskData['partition-cols']
        
        self.lookup_location = self.conf['lookup-dataset']['data-location']
        
    
    # Sets spark configs from location fetched from livy call
    def setSparkConfig(self,location):
  
        obj = self.s3.Object('landing-zone-vivek', location)
        body = obj.get()['Body'].read()
        json_raw = json.loads(body)
        spark_properties = json_raw['Properties']
        lst = []
        
        for i in spark_properties:
            lst.append((i,spark_properties[i]))
        conf = spark.sparkContext._conf.setAll(lst)
        conf = spark._jsc.hadoopConfiguration().set('mapreduce.fileoutputcommitter.marksuccessfuljobs', 'false')

    # fetchConfig is used to get app_config file from s3 bucket   
    def fetchConfig(self):
        path = spark.sparkContext._conf.get('spark.path')
        obj = self.s3.Object('landing-zone-vivek', path)
        body = obj.get()['Body'].read()
        confData = json.loads(body)
        
        return confData
		
		
class TransformData:

    #convertDatatype is used to covert the datatype of a column
    def convertDatatype(self,col_list,df):
        dict={'StringType':'string','IntegerType':'int','DoubleType':'double','DateType':'date','LongType':'long'}
        for item in col_list:
            a=item.split(':')
            col=a[0]
            to_type=a[1]
            df = df.withColumn(col, df[col].cast(dict[to_type]))
        return df

    # moveFiles is used to transport files around various s3 buckets
    def moveFiles(self,df,file_format,destination):
        df.write.format(file_format).save(destination)
    
    def moveFilesWithPartition(self,df,file_format,col_name,destination):
        df.write.partitionBy(col_name).mode("append").format(file_format).save(destination)
    
    
    # maskColumns is used to encrypt the columns of a specific data frame
    def maskColumns(self,col_list,df):
        for column in col_list:
            df = df.withColumn("masked_"+column,f.sha2(f.col(column),256))
        return df
        
    def convertDecimals(self,df,col_name,scale):
        df = df.withColumn(col_name,df[col_name].cast(DecimalType(scale=scale)))
        return df
        
    #Function for lookup dataset
    def lookup_dataset(self,df,lookup_location,pii_cols,dataset):
    
        source_df = df.withColumn("begin_date",f.current_date())
        source_df = source_df.withColumn("update_date",f.lit("null"))
        
        pii_cols = [i for i in pii_cols if i in df.columns]
            
        columns_needed = []
        insert_dict = {}
        
        for col in pii_cols:
            if col in df.columns:
                columns_needed += [col,"masked_"+col]
                
        columns_needed_source = columns_needed + ['begin_date','update_date']

        source_df = source_df.select(*columns_needed_source)

        try:
            targetTable = DeltaTable.forPath(spark,lookup_location+'/'+dataset)
        except AnalysisException:
            print('Table not found')
            source_df = source_df.withColumn("active_flag",f.lit("true"))
            source_df.write.format("delta").mode("overwrite").save(lookup_location+'/'+dataset)
            print('Table Created')
            targetTable = DeltaTable.forPath(spark,lookup_location+'/'+dataset)

        for i in columns_needed:
            insert_dict[i] = "updates."+i
            
        insert_dict['begin_date'] = f.current_date()
        insert_dict['active_flag'] = "true" 
        insert_dict['update_date'] = "null"

        _condition = dataset+".active_flag == true AND "+" OR ".join(["updates."+i+" <> "+ dataset+"."+i for i in [x for x in columns_needed if x.startswith("masked_")]])

        column = ",".join([dataset+"."+i for i in [x for x in pii_cols]])

        updatedColumnsToInsert = source_df.alias("updates").join(targetTable.toDF().alias(dataset), pii_cols).where(_condition)

        stagedUpdates = (
          updatedColumnsToInsert.selectExpr('NULL as mergeKey',*[f"updates.{i}" for i in source_df.columns]).union(source_df.selectExpr("concat("+','.join([x for x in pii_cols])+") as mergeKey", "*")))

        targetTable.alias(dataset).merge(stagedUpdates.alias("updates"),"concat("+str(column)+") = mergeKey").whenMatchedUpdate(
            condition = _condition,
            set = {                  # Set current to false and endDate to source's effective date."active_flag" : "False",
            "active_flag" : "False",
            "update_date" : f.current_date()
          }
        ).whenNotMatchedInsert(
          values = insert_dict
        ).execute()

        for i in pii_cols:
            df = df.drop(i).withColumnRenamed("masked_"+i, i)

        return df
		
if __name__=='__main__':

    
    dataset_to_be_processed = sys.argv[1]
    spark_config_loc = sys.argv[2]
    dataset_file = sys.argv[3]
    
    transform_obj = TransformData()
    conf_obj = Configuration(spark_config_loc,dataset_to_be_processed)

    # Loading dataset from Raw Zone for processing
    raw_zone_path = "s3://"+conf_obj.raw_bucket+"/"+dataset_file
    df = spark.read.option('header',True).parquet(raw_zone_path)

    #-----------------[Data Transformation & Masking]---------------
    # convertDatatype method is called for the conversion
    df = transform_obj.convertDatatype(conf_obj.df_datatypes,df)

    #The conversion of array to string 
    df = df.withColumn('location_source',concat_ws(',',col('location_source')))

    #Rounding the decimals to 7 points
    if dataset_to_be_processed.lower()=='actives':
        df = transform_obj.convertDecimals(df,'user_latitude',7)
        df = transform_obj.convertDecimals(df,'user_longitude',7)
    else:
        df = transform_obj.convertDecimals(df,'user_lat',7)
        df = transform_obj.convertDecimals(df,'user_long',7)

    #Converting string field to date-time field
    df = df.withColumn('date',to_date(col('date'),'yyyy-mm-dd'))
    
    #Masking columns
    df = transform_obj.maskColumns(conf_obj.df_maskColumns,df)
    
    df = transform_obj.lookup_dataset(df,conf_obj.lookup_location,conf_obj.pii_cols,dataset_to_be_processed)
    
    transform_obj.moveFilesWithPartition(df,'parquet',conf_obj.df_partition_cols,conf_obj.stagingzone_dest+"/")