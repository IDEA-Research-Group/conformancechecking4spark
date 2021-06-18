from pyspark.sql import SparkSession
from pm4py.objects.petri_net.importer.importer import deserialize as pnml_deserializer

def create_from_pnml(spark_session: SparkSession, path, variant=None, parameters=None):
    """

    :param spark_session:
    :param path:
    :return:
    """
    return spark_session.sparkContext\
        .wholeTextFiles(path)\
        .map(lambda f: _call_pnml_deserializer(f[-1], variant, parameters))


def _call_pnml_deserializer(pnml_str, variant, parameters):
    return pnml_deserializer(pnml_str, parameters=parameters) if variant is None else\
        pnml_deserializer(pnml_str, variant=variant, parameters=parameters)

