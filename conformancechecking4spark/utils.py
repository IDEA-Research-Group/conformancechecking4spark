from os.path import isfile, join
from os import listdir
from pm4py.objects.petri.importer import importer as pnml_importer
from pyspark.sql import SparkSession


def get_partial_models(directory):
    return [pnml_importer.apply(join(directory, f)) for f in listdir(directory) if isfile(join(directory, f))]


def create_default_spark_session():
    return SparkSession.builder \
        .appName("pm4py_test") \
        .master("local[*]") \
        .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.12.0") \
        .getOrCreate()

