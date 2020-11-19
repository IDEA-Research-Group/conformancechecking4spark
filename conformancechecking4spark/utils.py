from os.path import isfile, join
from os import listdir
from hdfs import InsecureClient
from pm4py.objects.petri.importer import importer as pnml_importer


def get_partial_models(directory):
    return [pnml_importer.apply(join(directory, f)) for f in listdir(directory) if isfile(join(directory, f))]


def remove_hdfs_directory(path, server, user, http_port=50070):
    client = InsecureClient(url="http://{0}:{1}".format(server, http_port), user=user)
    return client.delete(path, recursive=True)
