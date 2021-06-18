from conformancechecking4spark.log_parse_utils.from_xml import parse_xml_row
from conformancechecking4spark.log_parse_utils.from_df_row import parse_row
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

def create_from_xes(spark_session, path):
    """
    Requires config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.12.0") in spark config
    :param sc:
    :param path:
    :return:
    """
    df = spark_session.read \
        .format("com.databricks.spark.xml") \
        .option("rootTag", "log") \
        .option("rowTag", "trace") \
        .option("inferSchema", "false") \
        .load(path)

    return format_xml_rdd(df.rdd)


def format_xml_rdd(rdd):
    """
    :param rdd: RDD of rows with the format given by the library spark-xml from databricks with the format this library
    gives by default to XML files
    :return:
    """

    return rdd.map(lambda r: parse_xml_row(r))


# Group by trace => format
def format_df(data_frame: DataFrame, case_id="case:concept:name", task_id="concept:name", event_timestamp="time:timestamp"):
    return data_frame.groupBy(f.col(case_id)).agg(f.collect_list(f.struct(task_id, event_timestamp)).alias("events"))\
        .rdd.map(lambda r: parse_row(r, case_id=case_id, task_id=task_id, event_timestamp=event_timestamp))

