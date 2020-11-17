
from src.utils import sum_dif_dict, get_repeated_log, get_repeated_net


def calculate_min_cost(log, net):
    return sum_dif_dict(get_repeated_log(log), get_repeated_net(net))