from conformancechecking4spark.utils import create_default_spark_session
from conformancechecking4spark import log_rdd
from pyspark.sql import functions as f
import os
import config
from datetime import datetime
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
from pm4py.objects.log.obj import EventStream, EventLog

spark_session = create_default_spark_session()

def timestamp_to_iso_str(ts):
    return datetime.fromtimestamp(ts/1000).isoformat()


udf = f.udf(lambda x: timestamp_to_iso_str(x))

df = spark_session.read\
    .json(os.path.join(config.ROOT_DIR, "data/logs_leche.json"), multiLine = "true")\
    .filter(f.col("stage") == "Fabricacion")\
    .filter(f.col("timestamp").isNotNull())\
    .withColumn("timestamp", udf(f.col("timestamp")))

logs_formated = log_rdd.format_df(df, case_id="id", task_id="task", event_timestamp="timestamp").collect()

xes_exporter.apply(EventLog(EventStream(logs_formated)), os.path.join(config.ROOT_DIR, "data/logs_leche.xes"))


# x = xes_importer.apply(os.path.join(config.ROOT_DIR, "data/M2.xes"))
# print(type(x))