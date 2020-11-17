import math
from pm4py.algo.conformance.alignments import algorithm as alignments


class DistributedAlignmentProblem:
    """
    The following parameters must be defined:
    :param SparkContext
    
    """

    def __init__(self, log_rdd, pm_rdd, log_slices, pm_slices, global_timeout=None, trace_timeout=None,
                 algorithm=None, heuristic=None):
        #self._sc = sc
        self._log_rdd = log_rdd
        self._pm_rdd = pm_rdd
        self._log_slices = log_slices
        self._pm_slices = pm_slices
        self._global_timeout = global_timeout
        self._trace_timeout = trace_timeout
        self._algorithm = algorithm
        self._heuristic = heuristic

    def _process_partition(self, iterator):
        partial_solutions = {}
        estimations = []

        for element in iterator:
            trace = element[0]
            model, initial_marking, final_marking = element[1]
            estimation = self._heuristic(trace, model)
            estimations.append((element[0], element[1], estimation))  # (trace, model, estimation)

        estimations.sort(key=(lambda x: x[2]))  # sort by estimation

        for element in estimations:
            trace = element[0]
            model, initial_marking, final_marking = element[1]

            trace_name = trace.attributes['concept:name']
            prev_cost = math.inf

            if trace_name in partial_solutions.keys():
                prev_cost = partial_solutions[trace_name]
                estimation = element[2]
                if prev_cost <= estimation:  # if previous cost is better than estimation, brak
                    break
                if prev_cost == 0:  # if prev_cost is 0, break
                    break

            alignment = alignments.apply_trace(trace, model, initial_marking, final_marking)
            cost = int(alignment['cost'] / 10000)

            if cost < prev_cost:
                partial_solutions[trace_name] = cost

        return [(k, v) for k, v in partial_solutions.items()]

    def _reduce_partitions(self, ps1, ps2):
        return min(ps1, ps2)

    def run(self):
        partitions = self._log_rdd.repartition(self._log_slices).cartesian(self._pm_rdd.repartition(self._pm_slices))

        partitions.mapPartitions(self._process_partition) \
            .reduceByKey(self._reduce_partitions) \
            .show()
        pass

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
