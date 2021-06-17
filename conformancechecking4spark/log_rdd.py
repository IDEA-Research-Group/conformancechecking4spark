from pm4py.objects.log.obj import Trace, Event
from pm4py.util.dt_parsing import parser as dt_parser
from pyspark import Row


def create_log_rdd_from_xes(sc, path, slices=None):
    """
    Requires config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.12.0")
    :param sc:
    :param path:
    :param slices:
    :return:
    """


def format_xml_rdd(rdd):
    """
    :param rdd: RDD of rows with the format given by the library spark-xml from databricks with the format this library
    gives by default to XML files
    :return:
    """

    return rdd.map(lambda r: parse_xml_row(r))








# UTILS

def extract_key_and_value_from_xml(d):
    if "_key" in d.keys():
        if "_value" in d.keys():
            return {d["_key"]: d["_value"]}
        else:
            return {d["_key"]: None}


def xml_row_to_dict(row):
    d = {}
    # first level groups attributes by tag name. For example, if there is one attribute in a <date> tag,
    # and two attributes in two <string> tags, then there will be one element inside the "date" row attribute,
    # and a list with two elements inside the "string" row attribute
    for element in row:
        # if there is more than one element witht his type of tag
        if type(element) == list:
            for att in element:
                d.update(extract_key_and_value_from_xml(att.asDict()))
        else:
            d.update(extract_key_and_value_from_xml(element.asDict()))
    return d


def from_dict_to_event(event_dict):
    timestamp_field_name = "time:timestamp"
    if timestamp_field_name in event_dict.keys():
        event_dict[timestamp_field_name] = dt_parser.get().apply(event_dict[timestamp_field_name])
    return Event(event_dict)


def from_dicts_to_trace(event_dicts, trace_info_dict):
    events = [from_dict_to_event(ed) for ed in event_dicts]
    return Trace(events, attributes=trace_info_dict)


def parse_xml_row(row: Row):
    # it must contain at least one field called event, containing a list of elements under the parent tag <event></event>
    events = [xml_row_to_dict(r) for r in row["event"]]
    # the rest of the row represent the information on the trace
    r_dict = row.asDict()
    trace_info_raw = [r_dict[i] for i in r_dict.keys() if i != "event"]
    trace_info = xml_row_to_dict(trace_info_raw)
    return from_dicts_to_trace(events, trace_info)



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


