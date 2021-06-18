from os.path import isfile, join
from os import listdir
from hdfs import InsecureClient
from pm4py.objects.petri.importer import importer as pnml_importer
from pyspark.sql import SparkSession


def get_partial_models(directory):
    return [pnml_importer.apply(join(directory, f)) for f in listdir(directory) if isfile(join(directory, f))]


def remove_hdfs_directory(path, server, user, http_port=50070):
    client = InsecureClient(url="http://{0}:{1}".format(server, http_port), user=user)
    return client.delete(path, recursive=True)


def create_default_spark_session():
    return SparkSession.builder \
        .appName("pm4py_test") \
        .master("local[*]") \
        .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.12.0") \
        .getOrCreate()

