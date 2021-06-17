from pm4py.objects.log.obj import Trace, Event
from pm4py.util.dt_parsing import parser as dt_parser


def from_dict_to_event(event_dict):
    timestamp_field_name = "time:timestamp"
    if timestamp_field_name in event_dict.keys():
        event_dict[timestamp_field_name] = dt_parser.get().apply(event_dict[timestamp_field_name])
    return Event(event_dict)


def from_dicts_to_trace(event_dicts, trace_info_dict):
    events = [from_dict_to_event(ed) for ed in event_dicts]
    return Trace(events, attributes=trace_info_dict)