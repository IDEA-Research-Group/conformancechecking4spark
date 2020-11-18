import math
from pm4py.algo.conformance.alignments import algorithm as alignments


class DistributedAlignmentConfiguration:
    """
    The following parameters must be defined:
    :param SparkContext
    
    """

    def __init__(self, log_rdd, pm_rdd, log_slices, pm_slices, global_timeout=None, trace_timeout=None,
                 algorithm=None, heuristic=None):
        # self._sc = sc
        self._log_rdd = log_rdd
        self._pm_rdd = pm_rdd
        self._log_slices = log_slices
        self._pm_slices = pm_slices
        self._global_timeout = global_timeout
        self._trace_timeout = trace_timeout
        self._algorithm = algorithm
        self._heuristic = heuristic

    @staticmethod
    def _process_partition(iterator, heuristic):
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

            if trace_name in partial_solutions.keys():
                prev_cost = partial_solutions[trace_name]['normalized_cost']
                if heuristic is not None:  # if heuristic is defined, check the estimation value
                    estimation = element[2]
                    if prev_cost <= estimation:  # if previous cost is better than estimation, skip this iteration
                        continue
                if prev_cost == 0:  # if prev_cost is 0, skip this iteration
                    continue

            alignment = alignments.apply_trace(trace, model, initial_marking, final_marking)
            normalized_cost = int(alignment['cost'] / 10000)

            if normalized_cost < prev_cost:
                if heuristic is not None:
                    partial_solutions[trace_name] = {'normalized_cost': normalized_cost,
                                                     'estimation': estimation,
                                                     'calculated_alignment': alignment}
                else:
                    partial_solutions[trace_name] = {'normalized_cost': normalized_cost,
                                                     'calculated_alignment': alignment}

        return [(k, v) for k, v in partial_solutions.items()]

    @staticmethod
    def _reduce_partitions(ps1, ps2):
        return min(ps1['normalized_cost'], ps2['normalized_cost'])

    @staticmethod
    def _apply(log_rdd, pm_rdd, log_slices, pm_slices, heuristic):
        partitions = log_rdd.repartition(log_slices).cartesian(pm_rdd.repartition(pm_slices))

        return partitions \
            .mapPartitions(lambda partition: DistributedAlignmentConfiguration._process_partition(partition, heuristic)) \
            .reduceByKey(DistributedAlignmentConfiguration._reduce_partitions)

    def apply(self):
        rdd = \
            DistributedAlignmentConfiguration \
                ._apply(self._log_rdd, self._pm_rdd, self._log_slices, self._pm_slices, self._heuristic)

        return DistributedAlignmentProblem(rdd)

    # builder = Build()

    class Build:
        """
        Attributes:
            _sc                 The SparkContext object employed for the building process
            _spark_config       The SparkConfig from which the cluster configuration is took

            Explicit configuration parameters: if any of these is specified, the configuration of the final SparkContext
            is modified:
            _cores_executor     The number of cores for executor nodes
            _cores_driver       The number of cores for the driver nodes
            _memory_executor    The memory for executor nodes
            _memory_driver      The memory for the driver node

            Parameters associated to the input data:
            _log_rdd            The RDD of event logs
            _pm_rdd             The RDD of partial models
            _log                The pm4py XES object
            _pms                The list of pm4py pnml objects
            _log_path           The path to the XES file
            _pm_path            The path to the pnml file
            _pm_directory_path  The path to the directory where the pnml files are stored

            Configuration parameters:
            _log_slices         The number of slices for the RDD of event logs
            _pm slices          The number of slices for the RDD of partial models
            _global_timeout     The global timeout for each subproblem
            _trace_timeout      The timeout for each trace
            _algorithm          The name of the pm4py algorithm to employ
            _heuristic          A function to be employed as a heuristic for estimating the alignments




        """

        def __init__(self):
            self._sc = None
            self._ss = None
            self._spark_config = None
            self._cores_executor = None
            self._cores_driver = None
            self._memory_executor = None
            self._memory_driver = None
            self._memory_driver = None
            pass

        def build(self):
            pass


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
