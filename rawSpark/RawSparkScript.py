import sys
from awsglue.transforms import * # pyright: ignore[reportMissingImports]
from awsglue.utils import getResolvedOptions # pyright: ignore[reportMissingImports]
from pyspark.context import SparkContext
from awsglue.context import GlueContext # pyright: ignore[reportMissingImports]
from awsglue.job import Job # pyright: ignore[reportMissingImports]
from awsglue.dynamicframe import DynamicFrame, DynamicFrameWriter # pyright: ignore[reportMissingImports]

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

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
    
    print('# Write the data into s3 with pyspark') 
    df.write.partitionBy("race_year")\
            .mode("overwrite")\
            .option("compression", "snappy")\
            .save("s3://formulaonegc/f1_world_championship/fact/")

    print('Create a dynamic frame from pyspark df')
    dynamic_df = DynamicFrame.fromDF(df, glueContext)

    print('Using glue write from options to save to s3')
    x = glueContext.write_dynamic_frame_from_options(
        frame = dynamic_df, 
        connection_type = "s3", 
        connection_options={"path":"s3://formulaonegc/f1_dynamic_fact/",
                            "mode": "overwrite",
                            "partitionKeys": ["race_year"]},
        format = "parquet",
        format_options = {"compression": "snappy"}
    )
    
    print('# Code to dynamcic Create / update glue catalog')
    y = glueContext.write_dynamic_frame_from_catalog(
        frame = dynamic_df,
        database = "admin_prod",
        table_name = "table_f1_dynamic",
        additional_options = {"enableUpdateCatalog": True}
    )

    print('Code to write data_frame to catalog')
    z = glueContext.write_data_frame_from_catalog(
        frame = df,
        database = "admin_prod",
        table_name = "table_f1_data",
        additional_options = {"enableUpdateCatalog": True}
    )

    job.commit()
    print('Program finished')
    
except Exception as e:
    print(e)
    raise e