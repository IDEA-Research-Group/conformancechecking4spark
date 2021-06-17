from conformancechecking4spark.log_parse_utils.from_xml import parse_xml_row


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


def create_from_csv():
    pass


# Group by trace => format
def format_csv_rdd():
    pass







# UTILS





class LogDataFrameBuilder():


    def __init__(self):
        pass

    def set_additional_event_attributes(self):
        pass

    def set_additional_trace_attributes(self):
        pass

    def from_xes(self):
        pass

    def from_csv(self):
        pass

    def from_df(self):
        pass

    def _build_from_xes(self):
        pass

    def _build_from_csv(self):
        pass


    def build(self):
        if self._df is not None:
            return self._df
        elif self._xes is not None:
            return


class EventDataFrameBuilder():
    """
    Attributes:
        _df                 A DataFrame object containing, at least
    """


    def __init__(self):
        pass


