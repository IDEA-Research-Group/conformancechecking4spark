from pyspark import SparkContext, SparkConf
from pm4py.objects.log.importer.xes import importer as xes_importer
from pm4py.objects.petri.importer import importer as pnml_importer
import config
import os
from conformancechecking4spark.alignments import DistributedAlignmentConfiguration
from conformancechecking4spark.heuristics.algorithm import sum_of_differences
from conformancechecking4spark.utils import get_partial_models

path_pms = os.path.join(config.ROOT_DIR, 'data/M2')

conf = SparkConf().setAppName("test").setMaster("local[*]")
sc = SparkContext(conf=conf)

log = xes_importer.apply(os.path.join(config.ROOT_DIR, 'data/M2.xes'))
net, initial_marking, final_marking = pnml_importer.apply(os.path.join(config.ROOT_DIR, 'data/M2_petri_pnml.pnml'))

nets = [pnml_importer.apply(os.path.join(path_pms, f)) for f in get_partial_models(path_pms)]

log_rdd = sc.parallelize(log)
# pm_rdd = sc.parallelize([(net, initial_marking, final_marking)])
pm_rdd = sc.parallelize(nets)

distr_alg = DistributedAlignmentConfiguration(log_rdd, pm_rdd, 10, 1, heuristic=sum_of_differences)

distr_alg.apply().save_local(os.path.join(config.ROOT_DIR, 'data/results'))



