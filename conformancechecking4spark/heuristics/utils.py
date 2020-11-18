def get_repeated_log(log):
    events_in_logs = {}
    for event in log:
        activity = str(event['concept:name'])
        if activity not in events_in_logs.keys():
            events_in_logs[activity] = 1
        else:
            events_in_logs[activity] += 1
    return events_in_logs


def get_repeated_net(net):
    transitions_in_net = {}
    for transition in net.transitions:
        activity = str(transition)
        if activity not in transitions_in_net.keys():
            transitions_in_net[activity] = 1
        else:
            transitions_in_net[activity] += 1
    return transitions_in_net


def sum_dif_dict(dict1, dict2):
    used_variables = []
    total = 0
    for s in dict1.keys():
        num_reps_1 = dict1[s]
        num_reps_2 = 0
        if s in dict2.keys():
            num_reps_2 = dict2[s]
        total += abs(num_reps_1 - num_reps_2)
        used_variables.append(s)

    for s in dict2.keys():
        if s not in used_variables:
            total += dict2[s]

    return total