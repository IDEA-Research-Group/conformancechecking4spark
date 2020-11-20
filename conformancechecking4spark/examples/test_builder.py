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
