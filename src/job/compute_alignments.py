

class DistributedAlignmentProblem:

    """
    The following parameters must be defined:
    :param SparkContext
    
    """

    # Define several constructors



    def __init__(self, sc, trace_rdd, pm_rdd, log_slices, pm_slices, global_timeout=None, trace_timeout=None, algorithm=None, heuristic=None):
        self._pm_rdd = None
        self._log_rdd = None
        self._sc = None

    def _init_spark(self):
        pass

    def run(self):
        sc = self._sc
        log_rdd = self._log_rdd
        pm_rdd = self._pm_rdd
        
        
        pass

    builder = Build()

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





