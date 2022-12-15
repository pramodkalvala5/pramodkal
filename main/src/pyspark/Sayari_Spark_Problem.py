import os
import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Sayari_Spark_Problem'
        ).config('spark.driver.host', 'localhost').getOrCreate()

if __name__ == '__main__':
    ofac_df = spark.read.json('hdfs://localhost:9000/tmp/ofac.jsonl')
    ofac_df.createOrReplaceTempView('ofac_df_table')
    ofac_df_v1 = \
        spark.sql('select INT(id) ofac_id, type from ofac_df_table')
    gbr_df = spark.read.json('hdfs://localhost:9000/tmp/gbr.jsonl')
    gbr_df.createOrReplaceTempView('gbr_df_table')
    gbr_df_v1 = spark.sql('select INT(id) uk_id, type from gbr_df_table'
                          )
    gbr_ofac_matched_entities = ofac_df_v1.join(gbr_df_v1,
            (ofac_df_v1['ofac_id'] == gbr_df_v1['uk_id'])
            & (ofac_df_v1['type'] == gbr_df_v1['type']), 'inner'
            ).drop(gbr_df_v1['type'])
    gbr_ofac_matched_entities.coalesce(1).write.csv('hdfs://localhost:9000/tmp/'
            , header='true', mode='append')
