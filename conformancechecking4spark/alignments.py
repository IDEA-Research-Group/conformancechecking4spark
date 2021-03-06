import math
from pm4py.algo.conformance.alignments import algorithm as alignment_alg
from pyspark import SparkContext, SparkConf
from pm4py.objects.log.importer.xes import importer as xes_importer
from pm4py.objects.petri.importer import importer as pnml_importer
from conformancechecking4spark.utils import get_partial_models


class DistributedAlignmentConfiguration:
    """
    The following parameters must be defined:
    :param SparkContext
    
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
            _sc                 The SparkContext object employed for the building process. If SparkContext is given,
                                the cluster configuration parameters are ignored.
            _master             The master node of the cluster. Default: local[*]
            _app_name           The name of the Spark application. Default: DistributedAlignmentProblem
            _spark_config       The SparkConfig from which the cluster configuration is took. Default: Creates
                                SparkConfig from thefault parameters.

            Explicit configuration parameters: if any of these is specified, the configuration of the final SparkContext
            is modified:
            _cores_executor     The number of cores for executor nodes. Default: 1
            _cores_driver       The number of cores for the driver nodes. Default: 1
            _memory_executor    The memory for executor nodes. Default: 1g
            _memory_driver      The memory for the driver node. Default: 1g

            Parameters associated to the input data: (at least one way of specifying log and pm must be used)
            _log_rdd            The RDD of event logs. If a RDD is given, the parameters associated to log input data
                                are ignored
            _pm_rdd             The RDD of partial models. If a RDD is given, the parameters associated to pm input data
                                are ignored
            _log                The pm4py XES object
            _pm                 The a tule (pnml object, initial_marking, final_marking)
            _pms                The list of pm4py pnml objects
            _log_path           The path to the XES file
            _pm_path            The path to the pnml file
            _pms_directory_path  The path to the directory where the pnml files are stored

            Configuration parameters:
            _log_slices         The number of slices for the RDD of event logs
            _pm_slices          The number of slices for the RDD of partial models
            _global_timeout     The global timeout for each subproblem. Default is None
            _trace_timeout      The timeout for each trace. Default is None
            _algorithm          The name of the pm4py algorithm to employ. Default is None
            _heuristic          A function to be employed as a heuristic for estimating the alignments. Default is None




        """

        def __init__(self):
            self._sc = None
            self._master = "local[*]"
            self._app_name = "DistributedAlignmentProblem"
            self._spark_config = None
            self._executor_cores = "1"
            self._driver_cores = "1"
            self._executor_memory = "1g"
            self._driver_memory = "1g"
            self._log_rdd = None
            self._pm_rdd = None
            self._log = None
            self._pm = None
            self._pms = None
            self._log_path = None
            self._pm_path = None
            self._pms_directory_path = None
            self._log_slices = None
            self._pm_slices = None
            self._global_timeout = None
            self._trace_timeout = None
            self._algorithm = None
            self._heuristic = None
            self._already_partitioned = False
            pass

        def set_spark_context(self, sc):
            self._sc = sc
            return self

        def set_master(self, master):
            self._master = master
            return self

        def set_app_name(self, app_name):
            self._app_name = app_name
            return self

        def set_spark_config(self, spark_config):
            self._spark_config = spark_config
            return self

        def set_executor_cores(self, executor_cores):
            self._executor_cores = executor_cores
            return self

        def set_driver_cores(self, driver_cores):
            self._driver_cores = driver_cores
            return self

        def set_executor_memory(self, executor_memory):
            self._executor_memory = executor_memory
            return self

        def set_driver_memory(self, driver_memory):
            self._driver_memory = driver_memory
            return self

        def set_log_rdd(self, log_rdd):
            self._log_rdd = log_rdd
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

        def set_pms_directory(self, pms_directory):
            self._pms_directory_path = pms_directory
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

        def _create_spark_config(self):
            self._spark_config = SparkConf() \
                .setAppName(self._app_name) \
                .setMaster(self._master) \
                .set("spark.executor.memory", self._executor_memory) \
                .set("spark.driver.memory", self._driver_memory) \
                .set("spark.executor.cores", self._executor_cores) \
                .set("spark.driver.cores", self._driver_cores)

        def _prepare_spark_context(self):
            if self._sc is None:
                if self._spark_config is None:
                    self._create_spark_config()
                self._sc = DistributedAlignmentConfiguration.Builder._create_sc(self._spark_config)

        def _prepare_log_rdd(self):
            if self._log_rdd is None:
                if self._log is not None:
                    self._log_rdd = DistributedAlignmentConfiguration.Builder._create_rdd(self._sc, self._log,
                                                                                          self._log_slices)
                    self._already_partitioned = True
                elif self._log_path is not None:
                    self._log = xes_importer.apply(self._log_path)
                    self._prepare_log_rdd()
                else:
                    raise ValueError("A path to a XES log file must be specified.")

        def _prepare_pm_rdd(self):
            if self._pm_rdd is None:
                if self._pms is not None:
                    self._pm_rdd = DistributedAlignmentConfiguration.Builder._create_rdd(self._sc, self._pms,
                                                                                         self._pm_slices)
                    self._already_partitioned = True
                elif self._pm is not None:
                    self._pms = [self._pm]
                    self._prepare_pm_rdd()
                elif self._pm_path is not None:
                    self._pm = pnml_importer.apply(self._pm_path)
                    self._prepare_pm_rdd()
                elif self._pms_directory_path is not None:
                    self._pms = get_partial_models(self._pms_directory_path)
                    self._prepare_pm_rdd()
                else:
                    raise ValueError("A path to a PNML file or a directory must be specified.")

        @staticmethod
        def _create_sc(spark_conf):
            return SparkContext(conf=spark_conf)

        @staticmethod
        def _create_rdd(sc, to_distribute, slices):
            return sc.parallelize(to_distribute, slices)

        def build(self):
            if any([self._log_slices is None, self._pm_slices is None]):
                raise ValueError("Log slices and pm slices must be specified.")

            self._prepare_spark_context()
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

    @staticmethod
    def _save_local(rdd, local_path, force_same_directory):
        if force_same_directory:
            rdd.coalesce(1).saveAsTextFile(local_path)
        else:
            rdd.saveAsTextFile(local_path)

    @staticmethod
    def _print(rdd):
        rdd.foreach(print)
