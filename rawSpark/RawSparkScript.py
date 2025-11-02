import sys
from awsglue.transforms import * # pyright: ignore[reportMissingImports]
from awsglue.utils import getResolvedOptions # pyright: ignore[reportMissingImports]
from pyspark.context import SparkContext
from awsglue.context import GlueContext # pyright: ignore[reportMissingImports]
from awsglue.job import Job # pyright: ignore[reportMissingImports]

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

# Reference Query
'''
    SELECT b.date as "race_date", a.position, b.name as "race_name", 
    concat(c.forename, c.surname) as "driver_name", a.points, a.driverstandingsid, b.raceid
    FROM 
        admin_prod.tabledriver_standings a
        left join admin_prod.tableraces b
            on a.raceid = b.raceid
        left join admin_prod.tabledrivers c
            on a.driverid = c.driverid
    limit 10;
'''

try:

    print('# read the tabledriver_standings data')
    gc_df_tds = glueContext.create_dynamic_frame_from_catalog(database="admin_prod", table_name="tabledriver_standings")
    df_tds = gc_df_tds.toDF()
    
    print('# read the tabledrivers')
    gc_df_td = glueContext.create_dynamic_frame_from_catalog(database="admin_prod", table_name="tabledrivers")
    df_td = gc_df_td.toDF()
    
    print('# read the tableraces')
    gc_df_tr = glueContext.create_dynamic_frame_from_catalog(database="admin_prod", table_name="tableraces")
    df_tr = gc_df_tr.toDF()
    
    print('# Rename some generic column names')
    df_tr = df_tr.withColumnsRenamed({"date":"race_date", "name":"race_name"})
    
    print('# df_tds, left join df_td, left join with df_tr')
    df = df_tds.join(df_td, "driverid", "left")\
        .join(df_tr, "raceid", "left")
    
    print('# Select only relavent columns')
    df = df.selectExpr("race_date", "year(race_date) as race_year", "position", "race_name", "concat(forename, surname) as driver_name", "points", "driverstandingsid", "raceid")
    
    print('# Write the data into s3 with glue catalog table enabled') 
    df.write.partitionBy("race_year")\
            .option("mode", "overwrite")\
            .option("compression", "snappy")\
            .save("s3://formulaonegc/f1_world_championship/fact/")
    
    print('# Code to Create / update glue catalog')
    
except Exception as e:
    print(e)
    raise e