from os.path import isfile, join
from os import listdir


def get_partial_models(directory):
    return [f for f in listdir(directory) if isfile(join(directory, f))]
