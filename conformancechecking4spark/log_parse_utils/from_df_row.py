from pyspark.sql import Row
from conformancechecking4spark.log_parse_utils import dict_to_pm4py


def parse_row(row: Row, case_id, task_id, event_timestamp):
    event_dicts = [{"concept:name": e[task_id], "time:timestamp": e[event_timestamp]} for e in row["events"]]
    return dict_to_pm4py.from_dicts_to_trace(event_dicts, {"concept:name": row[case_id]})

