import math
from pm4py.algo.conformance.alignments import algorithm as alignment_alg
from pyspark.sql import SparkSession, Row
from conformancechecking4spark import log_rdd, pnml_rdd


class DistributedAlignmentConfiguration:
    """
    
    """

    def __init__(self, log_rdd, pm_rdd, log_slices, pm_slices, global_timeout=None, trace_timeout=None,
                 algorithm=None, heuristic=None, already_partitioned=False):
        # self._sc = sc
        self._log_rdd = log_rdd
        self._pm_rdd = pm_rdd
        self._log_slices = log_slices
        self._pm_slices = pm_slices
        self._global_timeout = global_timeout
        self._trace_timeout = trace_timeout
        self._algorithm = algorithm
        self._heuristic = heuristic
        self._already_partitioned = already_partitioned

    @staticmethod
    def _process_partition(iterator, heuristic=None, algorithm=None):
        partial_solutions = {}
        estimations = []

        for element in iterator:
            trace = element[0]
            model, initial_marking, final_marking = element[1]

            if heuristic is None:
                estimations.append((element[0], element[1]))  # (trace, model)
            else:
                estimation = heuristic(trace, model)
                estimations.append((element[0], element[1], estimation))  # (trace, model, estimation)

        if heuristic is not None:
            estimations.sort(key=(lambda x: x[2]))  # sort by estimation

        for element in estimations:
            trace = element[0]
            model, initial_marking, final_marking = element[1]

            trace_name = trace.attributes['concept:name']
            prev_cost = math.inf

            estimation = None
            if heuristic is not None:  # if heuristic is defined, check the estimation value
                estimation = element[2]

            if trace_name in partial_solutions.keys():
                prev_cost = partial_solutions[trace_name]['normalized_cost']
                if estimation is not None:
                    if prev_cost <= estimation:  # if previous cost is better than estimation, skip this iteration
                        continue
                if prev_cost == 0:  # if prev_cost is 0, skip this iteration
                    continue

            alignment = alignment_alg.apply_trace(trace, model, initial_marking, final_marking, parameters=algorithm)
            normalized_cost = int(alignment['cost'] / 10000)

            if normalized_cost < prev_cost:
                partial_solutions[trace_name] = {'normalized_cost': normalized_cost, 'calculated_alignment': alignment}
                if heuristic is not None:
                    partial_solutions[trace_name]['estimation'] = estimation

        return [(k, v) for k, v in partial_solutions.items()]

    @staticmethod
    def _reduce_partitions(ps1, ps2):
        if ps1['normalized_cost'] < ps2['normalized_cost']:
            return ps1
        else:
            return ps2

    @staticmethod
    def _apply(log_rdd, pm_rdd, log_slices, pm_slices, heuristic, algorithm, already_partitioned):
        if already_partitioned:
            partitions = log_rdd.cartesian(pm_rdd)
        else:
            partitions = log_rdd.repartition(log_slices).cartesian(pm_rdd.repartition(pm_slices))

        return partitions \
            .mapPartitions(lambda partition: DistributedAlignmentConfiguration._process_partition(partition, heuristic,
                                                                                                  algorithm)) \
            .reduceByKey(DistributedAlignmentConfiguration._reduce_partitions)

    def apply(self):
        # Set timeout parameters if they were defined
        if any([self._global_timeout is not None, self._trace_timeout is not None]):
            if self._algorithm is None:
                self._algorithm = {}
            if self._trace_timeout is not None:
                self._algorithm[alignment_alg.Parameters.PARAM_MAX_ALIGN_TIME_TRACE] = self._trace_timeout
            if self._global_timeout is not None:
                self._algorithm[alignment_alg.Parameters.PARAM_MAX_ALIGN_TIME] = self._global_timeout

        rdd = \
            DistributedAlignmentConfiguration \
            ._apply(self._log_rdd, self._pm_rdd, self._log_slices, self._pm_slices, self._heuristic,
                        self._algorithm, self._already_partitioned)

        return DistributedAlignmentProblem(rdd)

    class Builder:
        """
        Attributes:
            _spark_session      The SparkSession object employed for the building process. It is mandatory if neither
                                _log_rdd nor pm_rdd are specified.

            Parameters associated to the input data: (at least one way of specifying log and pm must be used)
            _log_rdd            The RDD of event logs. If a RDD is given, the parameters associated to log input data
                                are ignored
            _log_df             DataFrame containing event logs. Each row must represent a single event. The case
                                related to each event must be present.
            _log_df_config      Dictionary indicating the location of the "case_id", "task_id", and "event_timestamp"
                                columns
            _pm_rdd             The RDD of partial models. If a RDD is given, the parameters associated to pm input data
                                are ignored
            _log                The pm4py XES object
            _pm                 The a tule (pnml object, initial_marking, final_marking)
            _pms                The list of pm4py pnml objects
            _log_path           The path to the XES file
            _pm_path            The path to the pnml file
            _pnml_variant       Petri net variant. Parameter received by the pnml importer
            _pnml_arguments     Petri net arguments. Parameter received by the pnml importer

            Configuration parameters:
            _log_slices         The number of slices for the RDD of event logs
            _pm_slices          The number of slices for the RDD of partial models
            _global_timeout     The global timeout for each subproblem. Default is None
            _trace_timeout      The timeout for each trace. Default is None
            _algorithm          The name of the pm4py algorithm to employ. Default is None
            _heuristic          A function to be employed as a heuristic for estimating the alignments. Default is None

        """

        def __init__(self):
            self._spark_session = None
            self._log_rdd = None
            self._log_df = None
            self._log_df_config = None
            self._pm_rdd = None
            self._log = None
            self._pm = None
            self._pms = None
            self._log_path = None
            self._pm_path = None
            self._pnml_variant = None
            self._pnml_parameters = None
            self._log_slices = None
            self._pm_slices = None
            self._global_timeout = None
            self._trace_timeout = None
            self._algorithm = None
            self._heuristic = None
            self._already_partitioned = False
            pass

        def set_spark_session(self, spark_session):
            self._spark_session = spark_session
            return self

        def set_log_rdd(self, log_rdd):
            self._log_rdd = log_rdd
            return self

        def set_log_df(self, log_df, case_id="case:concept:name", task_id="concept:name", event_timestamp="time:timestamp"):
            self._log_df = log_df
            self._log_df_config = {"case_id": case_id, "task_id": task_id, "event_timestamp": event_timestamp}
            return self

        def set_pm_rdd(self, pm_rdd):
            self._pm_rdd = pm_rdd
            return self

        def set_log(self, log):
            self._log = log
            return self

        def set_pms(self, pms):
            self._pms = pms
            return self

        def set_pm(self, pm):
            self._pm = pm
            return self

        def set_log_path(self, log_path):
            self._log_path = log_path
            return self

        def set_pm_path(self, pm_path):
            self._pm_path = pm_path
            return self

        def set_pnml_variant(self, variant):
            self._pnml_variant = variant
            return self

        def set_pnml_parameters(self, parameters):
            self._pnml_parameters = parameters
            return self

        def set_log_slices(self, log_slices):
            self._log_slices = log_slices
            return self

        def set_pm_slices(self, pm_slices):
            self._pm_slices = pm_slices
            return self

        def set_global_timeout(self, global_timeout):
            self._global_timeout = global_timeout
            return self

        def set_trace_timeout(self, trace_timeout):
            self._trace_timeout = trace_timeout
            return self

        def set_algorithm(self, algorithm):
            self._algorithm = algorithm
            return self

        def set_heuristic(self, heuristic):
            self._heuristic = heuristic
            return self

        def _assert_spark_session(self):
            if self._spark_session is None and (self._log_rdd is None or self._pm_rdd is None):
                raise ValueError("Spark Session must be specified. You can create a default SparkSession from "
                                 "conformancechecking4spark.utils.create_default_spark_session")

        def _prepare_log_rdd(self):
            if self._log_rdd is None:
                if self._log_df is not None:
                    self._log_rdd = log_rdd.format_df(self._log_df, case_id=self._log_df_config["case_id"],
                                                      task_id=self._log_df_config["task_id"],
                                                      event_timestamp=self._log_df_config["event_timestamp"])
                elif self._log is not None:
                    self._log_rdd = DistributedAlignmentConfiguration.Builder._create_rdd(self._spark_session, self._log,
                                                                                          self._log_slices)
                    self._already_partitioned = True
                elif self._log_path is not None:
                    self._log_rdd = log_rdd.create_from_xes(self._spark_session, self._log_path)
                else:
                    raise ValueError("A path to a XES log file must be specified.")

        def _prepare_pm_rdd(self):
            if self._pm_rdd is None:
                if self._pms is not None:
                    self._pm_rdd = DistributedAlignmentConfiguration.Builder._create_rdd(self._spark_session, self._pms,
                                                                                         self._pm_slices)
                    self._already_partitioned = True
                elif self._pm is not None:
                    self._pms = [self._pm]
                    self._prepare_pm_rdd()
                elif self._pm_path is not None:
                    self._pm_rdd = pnml_rdd.create_from_pnml(self._spark_session, self._pm_path,
                                                         self._pnml_variant, self._pnml_parameters)
                # elif self._pms_directory_path is not None:
                #     self._pms = get_partial_models(self._pms_directory_path)
                #     self._prepare_pm_rdd()
                else:
                    raise ValueError("A path to a PNML file or a directory must be specified.")

        @staticmethod
        def _create_rdd(spark_session: SparkSession, to_distribute, slices):
            return spark_session.sparkContext.parallelize(to_distribute, slices)

        def build(self):
            if any([self._log_slices is None, self._pm_slices is None]):
                raise ValueError("Log slices and pm slices must be specified.")

            self._assert_spark_session()
            self._prepare_log_rdd()
            self._prepare_pm_rdd()
            return DistributedAlignmentConfiguration(self._log_rdd, self._pm_rdd, self._log_slices, self._pm_slices,
                                                     self._global_timeout, self._trace_timeout, self._algorithm,
                                                     self._heuristic, self._already_partitioned)

    builder = Builder()


class DistributedAlignmentProblem:

    def __init__(self, rdd):
        self._rdd = rdd

    def rdd(self):
        return self._rdd

    def save_local(self, local_path, force_same_directory=True):
        DistributedAlignmentProblem._save_local(self._rdd, local_path, force_same_directory)

    def print(self):
        DistributedAlignmentProblem._print(self._rdd)

    def df(self):
        return DistributedAlignmentProblem._df(self._rdd)

    @staticmethod
    def _save_local(rdd, local_path, force_same_directory):
        if force_same_directory:
            rdd.coalesce(1).saveAsTextFile(local_path)
        else:
            rdd.saveAsTextFile(local_path)

    @staticmethod
    def _print(rdd):
        rdd.foreach(print)

    @staticmethod
    def _df(rdd):
        return rdd.map(lambda t: Row(case_id=t[0],
                              cost=t[-1]["calculated_alignment"]["cost"],
                              normalized_cost=t[-1]["normalized_cost"],
                              visited_states=t[-1]["calculated_alignment"]["visited_states"],
                              queued_states=t[-1]["calculated_alignment"]["queued_states"],
                              traversed_arcs=t[-1]["calculated_alignment"]["traversed_arcs"],
                              lp_solved=t[-1]["calculated_alignment"]["lp_solved"],
                              alignment=str(t[-1]["calculated_alignment"]["alignment"]))).toDF()

