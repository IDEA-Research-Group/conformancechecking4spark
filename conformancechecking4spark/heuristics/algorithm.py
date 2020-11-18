
from conformancechecking4spark.heuristics.utils import sum_dif_dict, get_repeated_log, get_repeated_net


def sum_of_differences(log, net):
    return sum_dif_dict(get_repeated_log(log), get_repeated_net(net))