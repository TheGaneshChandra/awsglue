import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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

# read the tabledriver_standings data
gc_df_tds = glueContext.create_dynamic_frame_from_catalog(database="admin_prod", table_name="tabledriver_standings")
df_tds = gc_df_tds.toDF()

# read the tabledrivers
gc_df_td = glueContext.create_dynamic_frame_from_catalog(database="admin_prod", table_name="tabledrivers")
df_td = gc_df_td.toDF()

# read the tableraces
gc_df_tr = glueContext.create_dynamic_frame_from_catalog(database="admin_prod", table_name="tableraces")
df_tr = gc_df_tr.toDF()

# Rename some generic column names
df_tr.withColumnsRenamed({"date":"race_date", "name":"race_name"})

# df_tds, left join df_td, left join with df_tr
df_tds.join(df_td, df_tds["driverid"] == df_td["driverid"])\
        .join(df_tr, df_tds["raceid"] == df_tr["raceid"])

# Select only relavent columns 
df_tds.selectExpr('SELECT to_date("race_date", "MM/dd/yyyy") as "race_date", year("race_date") as "race_year", "position", "race_name", concat("forename", "surname") as "driver_name", "points", "driverstandingsid", "raceid"')

# Write the data into s3 with glue catalog table enabled 
df_tds.write.option("mode", "overwrite")\
            .option("partitionBy", "race_year")\
            .option("compression", "snappy")\
            .save("s3://formulaonegc/f1_world_championship/fact/")

# Code to Create / update glue catalog