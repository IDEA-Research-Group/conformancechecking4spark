# conformancechecking4spark

![Conformance Checking 4 Spark](logo/CC4Spark.png)

Conformance Checking for Spark is a Python library which enables users to execute alignment problems in a distributed
environment. This solution enhances the execution time of alignment problems when (1) The logs is too large, with a high
number of traces, and (2) In case the PNML model is too complex. Hence, this tool is founded in two principles:

* The distribution of event logs tends to reduce the possibility of bottlenecks, since traces are wraped in 
_subproblems_ which are distributed and executed in parallel.
* We propose a novel horizontal acyclic model decomposition. In this way, complex PNML models can be decomposed in 
partial models. This enables to build smaller subproblem, distribute them, and execute them in parallel. In order to
find out more about this decomposition technique, please visit 
[this](https://www.fernuni-hagen.de/sttp/forschung/vip_tool.shtml) website.

This library is based on [__pm4py__](https://pm4py.fit.fraunhofer.de/documentation#conformance), whose algorithms are 
employed for the computation of the alignments.

## Versions

### 0.2.0

Python 3.7 is required.

Requirements:
* pm4py==2.0.1.3
* pyspark==3.0.1
* hdfs==2.5.8 (only for certain features)

## Using the library

### Foundations

Structure of the library:

```
conformancechecking4spark
|- alignments.py (1)
|--- DistributedAlignmentConfiguration (1.1)
|--- DistributedAlignmentProblem (1.2)
|- heuristics (2)
|--- algorithm.py (2.1)
|--- utils.py (2.2)
|- utils.py
```

* (1) The `alignments.py` module includes those classes which encapsulate the logic for the distributed alignment
problems. 
* (1.1) The `DistributedAlignmentConfiguration` wraps the configuration of a distributed alignment problem. It enables
to construct problems in twofold:
>* By means of its constructor, or
>* By means of its builder, which is invoked in the following way: `DistributedAlignmentConfiguration.builder`
Once the `DistributedAlignmentConfiguration` object has been built, invoking the method `apply` will generate a RDD by 
scheduling all the necessary transformations. This RDD is wraped in a `DistributedAlignmentProblem` object.
* (1.2) The `DistributedAlignmentProblem` wraps the RDD which entails the logic of the problem distribution with the 
configuration previously established. 
* (2) The `heuristic` module includes those algorithms which might be employed as heuristics (2.1) and utility functions
 (2.2) for supporting the implementation of those functions.   

#### DistributedAlignmentConfiguration

The constructor of this class receives the minimum required parameters. These are:
* `log_rdd`. The RDD of event logs (a event in the format required by the pm4py library)
* `pm_rdd`. The RDD of partial models (a PNML model or a set of them in the format required by the pm4py library).
* `log_slices`. Number of slices in which the RDD of logs is split.
* `pm_slices`. Number of slices in which the RDD of (partial) models is split.
* `global_timeout`. Sets the `PARAM_MAX_ALIGN_TIME` parameter of the pm4py algorithm. Default is `None`.
* `trace_timeout`. Sets the `PARAM_MAX_ALIGN_TIME_TRACE` parameter of the pm4py algorithm. Default is `None`
* `algorithm`. Sets the `parapeters` property of the pm4py algorithm. It is a dictionary in which the algorithm can be 
specified please refer to the documentation of pm4py for further details. Default is `None`, so it will employ the 
default algorithm defined by that library (A*).
* `heuristic`. Sets the heuristic function to use in order to optimise the execution of the slices (partitions) of 
subproblems. Default is `None`, but setting a heuristic function is highly recommended. This library includes by default
`heuristic.algorithm.sum_of_differences`, *but it must be explicitly indicated*. 

The builder allows to instantiate `DistributedAlignmentConfiguration` objects in several ways. There exist several 
parameters which might or not be specified. Now, we describe these groups of parameters in detail:

(in construction)


## Getting Started

Please bear in mind that all dependencies must be satisfied.

#### Local deployment

The following snippet would create a local distributed alignment problem. The event log data is specified by using the
`set_log_path` method, and the directory in which the partial models are found by the method `set_pms_directory`. Then, 
the slices of each set is specified by the `set_log_slices` and `set_pm_slices` methods. The heuristic is set by the 
`set_heuristic` method, receiving the heuristic function `sum_of_differences` located in 
`conformancechecking4spark.heuristics.algorithm`.

Finally, the configuration object is built, and then it is executed with the `print` method, which will print in console 
the results.

````python
from conformancechecking4spark.alignments import DistributedAlignmentConfiguration
import os
import config
from conformancechecking4spark.heuristics.algorithm import sum_of_differences

conf = DistributedAlignmentConfiguration.builder \
    .set_log_path(os.path.join(config.ROOT_DIR, 'data/M2.xes')) \
    .set_pms_directory(os.path.join(config.ROOT_DIR, 'data/M2')) \
    .set_log_slices(500) \
    .set_pm_slices(1) \
    .set_heuristic(sum_of_differences)\
    .build()

conf.apply().print()
````


#### Deployment in Apache Spark Cluster

Requirements: You need a cluster based on Apache Spark 3.0.1. Each node must have all the required dependencies 
installed. Also, Python 3.7 is required.

First, package this project with the following command: `python setup.py bdist_egg` (note that for this purpose, the 
python module `setuptools` is required).



## Known bugs and future work

* Timeouts are not fully supported. This will be fixed in future versions. By now, you can set a timeout by using the 
`set_trace_timeout` method, but bear in mind that:
>* If a timeout is specified, it is not possible to guarantee that the alignments found are optimal (please check our 
research article.)
>* If a timeout is specified, exceptions due to null values (None type) might raise. 
* This version only supports reading logs and PNML models from local paths. Future versions will enable to read them
from HDFS. Nonetheless, users might create their own RDDs and pass them to the distributed alignment problem.
* Support for further output options will be implemented (e.g., MongoDB, SQL dabtases, HDFS). Nonetheless, by now users
can take the resulting RDD and using Spark methods for this purpose.
* Up to now, the results are stored into a RDD. In future works, Spark DataFrames will be also supported.

