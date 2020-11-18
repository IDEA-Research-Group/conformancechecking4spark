from pyspark import SparkContext, SparkConf
from pm4py.objects.log.importer.xes import importer as xes_importer
from pm4py.objects.petri.importer import importer as pnml_importer
import config
import os
from src.compute_alignments import DistributedAlignmentConfiguration
from src.heuristics import calculate_min_cost

conf = SparkConf().setAppName("test").setMaster("local[*]")
sc = SparkContext(conf=conf)

log = xes_importer.apply(os.path.join(config.ROOT_DIR, 'data/M2.xes'))
net, initial_marking, final_marking = pnml_importer.apply(os.path.join(config.ROOT_DIR, 'data/M2_petri_pnml.pnml'))

log_rdd = sc.parallelize(log)
pm_rdd = sc.parallelize([(net, initial_marking, final_marking)])

distr_alg = DistributedAlignmentConfiguration(log_rdd, pm_rdd, 5, 1, heuristic=calculate_min_cost)

distr_alg.apply().print()



