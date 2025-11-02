### Command line scripts worth noting

> to get table columns
``` bash
aws glue get-table --database-name admin_prod --name tabledriver_standings --query "Table.{Table:Name,Columns:StorageDescriptor.Columns[*].Name}"
```

> Spark writing for best performace
```python
    # https://anuja-shukla.medium.com/how-to-write-pyspark-dataframes-to-s3-like-a-pro-with-aws-glue-c675eb99bd75
    df.write \
    .option("useS3ListImplementation", "true") \
    .option("compression", "snappy") \
    .partitionBy("zip_mod") \
    .mode("overwrite") \
    .parquet("s3a://your-bucket/path/")
```

> Parquet compression types
```yaml
    Compression Ratio : GZIP compression uses more CPU resources than Snappy or LZO, but provides a higher compression ratio.

    General Usage : GZip is often a good choice for cold data, which is accessed infrequently. Snappy or LZO are a better choice for hot data, which is accessed frequently.

    Splittablity : If you need your compressed data to be splittable, BZip2, LZO, and Snappy formats are splittable, but GZip is not.

    GZIP compresses data 30% more as compared to Snappy and 2x more CPU when reading GZIP data compared to one that is consuming Snappy data.

    LZO focus on decompression speed at low CPU usage and higher compression at the cost of more CPU.
```