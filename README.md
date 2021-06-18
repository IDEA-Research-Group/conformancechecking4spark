# conformancechecking4spark

![Conformance Checking 4 Spark](logo/CC4Spark.png)

Conformance Checking for Spark is a Python library which enables users to execute alignment problems in a distributed
environment. This solution enhances the execution time of alignment problems when (1) The event logs are too large, 
with huge numbers of events and traces, and (2) when the PNML model is too complex. 
Hence, this tool is founded in the following principles:

* When event logs are too large, storing all of them in a single `xes` file might be quite tricky. In those cases, 
  logs are usually stored on distributed systems (for example, HDFS or databases). This tool supports consuming event
  logs from distributed systems, transforming them in accordance to the `Open XES` standard, an enabling `pm4py`
  algorithms to apply its algorithms trace by trace in a distributed basis. **Note**: not all algorithms from `pm4py`
  can be applied trace by trace. **Note 2**: although this library provides native support for Conformance Checking 
  algorithms, the user is free to employ any other algorithm or process mining technique which could be applied trace
  by trace. Please check section "[Creating RDD of traces from a XES file](#creating-rdd-traces)".
* Regarding the Conformance Checking algorithms, these can be applied individually trace by trace against a PNML model
  (or partial PNML models, see next bullet). It enables to generate *subproblems* which are distributed among the 
  cluster nodes, preventing the creation of bottlenecks.
* For complex PNML models, we propose a novel horizontal acyclic model decomposition. In this way, complex PNML models 
  can be decomposed in partial models. This enables to build smaller subproblem, distribute them, and execute them in 
  parallel. In order to find out more about this decomposition technique, please visit 
[this](https://www.fernuni-hagen.de/sttp/forschung/vip_tool.shtml) website.

This library is based on [__pm4py__](https://pm4py.fit.fraunhofer.de/documentation#conformance), whose algorithms are 
employed for the computation of the alignments.

## Versions

### 0.3.0

Python 3.7 is required.

Requirements:
* pm4py==2.2.8
* pyspark==3.0.1
* numpy==1.19.2
* pandas==1.2.4
* pypandoc==1.5

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
scheduling all the necessary transformations. This RDD is wrapped in a `DistributedAlignmentProblem` object.
* (1.2) The `DistributedAlignmentProblem` wraps the RDD which entails the logic of the problem distribution with the 
configuration previously established.
* (2) The `heuristic` module includes those algorithms which might be employed as heuristics (2.1) and utility functions
 (2.2) for supporting the implementation of those functions.   

### DistributedAlignmentConfiguration

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

When the `apply` method is called, a `RDD` is created in accordance to the configuration that has been specified. Remark
that the operations are simply schedule in the `RDD`, but these are not executed yet. A `DistributedAlignmentProblem` 
object is created, wrapping that `RDD`.

### DistributedAlignmentConfiguration builder

We strongly recommend constructing the Distributed Alignment Problems by using the builder. It can be accessed in this
way: 

```python
conformancechecking4spark.alignments.DistributedAlignmentConfiguration.builder
```

The following methods are available to set the configuration of the alignment problem.

Required parameters:
* `set_spark_session`: it receives a SparkSession object. Required to generate the problem.
* `set_log_slices`: sets the number of slices for the RDD of logs. Required.
* `set_pm_slices`: sets the number of slices for the RDD of models. Required. 

Setting up the RDD of logs: it can be set, created or generated by calling just one of the following methods:
* `set_log_rdd`: set a previously created RDD of traces.
* `set_log_df`: set a DataFrame representing event logs. The RDD of logs will be automatically generated when calling
  the `build` method. It is necessary to specify the columns which form the case ids,
task ids, and event timestamps. Otherwise, default values are employed: 
```python
def set_log_df(self, log_df, case_id="case:concept:name", task_id="concept:name", event_timestamp="time:timestamp")
```
* `set_log`: set a native `pm4py` event log. The RDD of logs will be automatically generated when calling
  the `build` method.
* `set_log_path`: set a path to a XES event log file. It can be stored in any storage system supported by Apache Spark
  (HDFS, for example). The RDD of logs will be automatically generated when calling
  the `build` method.
  
Setting up the RDD of models: it can be set, created or generated by calling just one of the following methods:
* `set_pm_rdd`: set a previously created RDD of models.
* `set_pm`, `set_pms`: set a single model or a list of models. The RDD of models will be automatically generated when calling
  the `build` method.
* `set_pm_path`: set a path to either a single PNML file, or a directory containing a bunch of PNML files. It can be 
  stored in any storage system supported by Apache Spark (HDFS, for example). The RDD of models will be automatically 
  generated when calling the `build` method.

Optional setters:
* `set_pnml_variant`
* `set_pnml_parameters`
* `set_global_timeout`
* `set_trace_timeout`
* `set_algorithm`
* `set_heuristic`

When calling the `build` method, a `DistributedAlignmentProblemConfiguration` object is created, wrapping the 
configuration specified by the user.

### DistributedAlignmentProblem

These objects enable to (1) retrieve the `RDD` object with the operations that solve the alignment problems (`rdd()`),
(2) retrieve a `DataFrame` object wrapping the `RDD` with the operations that solve the alignment problems, and with a 
tabular structure (`df`) and (3) print the results of the alignment problems (`print()`).

If you retrieve the `RDD` or the `DataFrame` object, bear in mind that the alignment problems are not executed until an
_action_ is called.

### Creating RDDs of event logs

This library supports the creation of RDDs representing event logs following the `OpenXES` standard. These RDDs are 
modelled as RDD of `Trace` objects 
(see [the official pm4py repository](https://github.com/pm4py/pm4py-core/tree/release/pm4py/objects/log)).

RDD of Trace objects can be created in twofold.

#### Creating RDD of traces from a XES file 
<span id="creating-rdd-traces"></span>

Use the `log_rdd` module, and call the function `create_from_xes`. A `SparkSession` object and the path to the XES file
are required. The path to this file can be stored any storage system supported by Apache Spark (e.g., hdfs).

```python
from conformancechecking4spark import log_rdd

log_rdd.create_from_xes(spark, "hdfs://hdfs-server:8020/path/to/file.xes")
```

#### Creating RDD of traces from a Spark DataFrame

Access to a database or storage system by using the Apache Spark DataFrame API. Once created, use the `log_rdd` module 
and call the function `format_df`. It receives a DataFrame object, and the following optional parameters:
case_id="case:concept:name", task_id="concept:name", event_timestamp="time:timestamp"

* `case_id`: the name of the DataFrame column which contains the case ids (default: `case:concept:name`).
* `task_id`: the name of the DataFrame column which contains the task ids (default: `concept:name`)
* `event_timestamp`: the name of the column which contains the event timestamps (default: `time:timestamp`)

```python
from conformancechecking4spark import log_rdd

# From MongoDB (required MongoDB library)
df = spark.read.format("mongo").option("uri", "mongodb://mongo-server/db.eventLogs").load()

log_rdd.format_df(df)

# Explicitly indicating the case_id, task_id and event_timestamp columns:
log_rdd.format_df(df, case_id="user", task_id="action", event_timestamp="action_date")
```

## Step-by-step guide

Please bear in mind that all dependencies must be satisfied.

Example datasets can be found [here](http://www.idea.us.es/confcheckingbigdata/).



#### Local deployment

The following snippet would create a local distributed alignment problem. The event log data is specified by using the
`set_log_path` method, and the directory in which the partial models are found by using the method `set_pm_path`. Then, 
the slices of each set is specified by the `set_log_slices` and `set_pm_slices` methods. The heuristic is set by the 
`set_heuristic` method, receiving the heuristic function `sum_of_differences` located in 
`conformancechecking4spark.heuristics.algorithm`.

Finally, the configuration object is built, and then it is executed with the `print` method, which will print in console 
the results.

````python
from conformancechecking4spark.utils import create_default_spark_session
from conformancechecking4spark.alignments import DistributedAlignmentConfiguration
import os
import config
from conformancechecking4spark.heuristics.algorithm import sum_of_differences

spark_session = create_default_spark_session()

config = DistributedAlignmentConfiguration.builder \
    .set_spark_session(spark_session) \
    .set_log_path(os.path.join(config.ROOT_DIR, 'data/M2.xes')) \
    .set_pm_path(os.path.join(config.ROOT_DIR, 'data/M2')) \
    .set_log_slices(1) \
    .set_pm_slices(1) \
    .set_heuristic(sum_of_differences)\
    .build()

config.apply().df().show()
````

#### Deployment in Apache Spark Cluster

Requirements: You need a cluster based on Apache Spark 3.0.1. Each node must have all the required dependencies 
installed. Also, Python 3.7 is required.

First, package this project with the following command: `python setup.py bdist_egg` (note that for this purpose, the 
python module `setuptools` is required). It will produce a `.egg` file (let's call it conformancechecking4spark.egg).

Secondly, you have to create a `spark_submit.py` file. This is the entrypoint from the point of view of you Spark 
cluster. The only purpose is to create and solve the distributed alignment problem. Next, an example is provided, in 
which the M2 dataset is employed.

````python
from pyspark.sql import SparkSession
from conformancechecking4spark.heuristics.algorithm import sum_of_differences
from conformancechecking4spark.alignments import DistributedAlignmentConfiguration

# Create the SparkSession object properly configured
spark_session = SparkSession.builder\
    .appName("pm4py_test")\
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.cores", 4) \
    .config("spark.driver.cores", 1) \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.12.0")\
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .config("spark.mongodb.input.uri", "mongodb://mongo-uri:27017/cc4spark.results") \
    .config("spark.mongodb.output.uri", "mongodb://mongo-uri:27017/cc4spark.results") \
    .getOrCreate()

# Specify the path to the XES file and to the directory where the models are stored.
# Then, select the number of slices for each set
config = DistributedAlignmentConfiguration.builder \
    .set_spark_session(spark_session) \
    .set_log_path("hdfs://hdfs-uri:8020/path/to/M2.xes") \
    .set_pm_path("hdfs://hdfs-uri:8020/path/to/M2") \
    .set_log_slices(500) \
    .set_pm_slices(1) \
    .set_heuristic(sum_of_differences)\
    .build()

# Once build, get the DataFrame object with the results, and write them in MongoDB.
config.apply().df().write.format("mongo").mode("append").save()
````

Here is an example of `spark-submit` command configuration:

```shell
spark-submit --master <master-uri>  \
	--deploy-mode cluster \
	--conf spark.master.rest.enabled=true \
	--conf spark.executor.uri=<path-to>spark-3.0.1-bin-hadoop2.7.tgz \
	--conf spark.pyspark.python=<path-to-python> \
	--packages com.databricks:spark-xml_2.12:0.12.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
	--py-files http://<path-to>conformancechecking4spark-0.3.0-py3.7.egg \
	http://<path-to>/spark_submit.py
```



_IMPORTANT_:

In order to submit the application, you have to upload the following files to a path wich is visible to all the nodes of
the cluster (`spark_submit.py` and `conformancechecking4spark.egg`).

````
spark-submit --master URL_MASTER  \
	--deploy-mode cluster \
	--conf spark.master.rest.enabled=true \
	--conf spark.executor.uri=PATH/spark-3.0.1-bin-hadoop2.7.tgz \
	--conf spark.pyspark.python=/usr/bin/python3.7 \
	--py-files PATH/conformancechecking4spark.egg \
	PATH/spark_submit.py
````
 
## Known bugs and future work

* Timeouts are not fully supported. This will be fixed in future versions. By now, you can set a timeout by using the 
`set_trace_timeout` method, but bear in mind that:
>* If a timeout is specified, it is not possible to guarantee that the alignments found are optimal (please check our 
research article.)
>* If a timeout is specified, exceptions due to null values (None type) might raise.

## Licence

**Copyright (C) 2021 IDEA Research Group (IC258: Data-Centric Computing Research Hub)**

In keeping with the traditional purpose of furthering education and research, **it is
the policy of the copyright owner to permit non-commercial use and redistribution of
this software**. It has been tested carefully, but it is not guaranteed for any particular
purposes. **The copyright owner does not offer any warranties or representations, nor do
they accept any liabilities with respect to them**.

## References

**If you use this tool, we kindly ask to to reference the following research article:**

[1] Valencia-Parra, Á., Varela-Vaca, Á. J., Gómez-López, M. T., Carmona, J., & Bergenthum, R. (2021). Empowering conformance checking using Big Data through horizontal decomposition. Information Systems, 99, 101731. https://doi.org/10.1016/j.is.2021.101731

## Acknowledgements
This work has been partially funded by the Ministry of Science and Technology of Spain ECLIPSE (RTI2018-094283-B-C33) 
project, the European Regional Development Fund (ERDF/FEDER), MINECO (TIN2017-86727-C2-1-R), and by the University of 
Seville with VI Plan Propio de Investigación y Transferencia (VI PPIT-US).

