class DistributedAlignmentProblemResult:

    def __init__(self, rdd):
        self._rdd = rdd

    def rdd(self):
        return self._rdd

    def save_local(self, local_path, force_same_directory=True):
        DistributedAlignmentProblemResult._save_local(self._rdd, local_path, force_same_directory)

    @staticmethod
    def _save_local(rdd, local_path, force_same_directory):
        if force_same_directory:
            rdd.coalesce(1).saveAsTextFile(local_path)
        else:
            rdd.saveAsTextFile(local_path)
