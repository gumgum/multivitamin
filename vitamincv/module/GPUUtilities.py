import GPUtil
from tabulate import tabulate
import glog as log


class GPUUtility:
    def __init__(
        self,
        limit=1,
        priority="memory",
        minFreeLoad=0.0,
        minFreeMemory=0.0,
        minFreeMemoryMb=0.0,
        ignoreIDs=[],
        ignoreUUIDs=[],
    ):
        """Base class for any GPU-friendly objects. Simply wraps GPUtil module

        Args:
            limit (int): Max number of desired GPUs
            priority (str): How to prioritize which available GPUs to select
                Options:
                    "memory" (default): Selects qualifying GPUs with the most available memory
                    "first": Selects qualifying GPUs by ascending GPU id
                    "last": Selects qualifying GPUs by descending GPU id
                    "random": Selects qualifying GPUs randomly
                    "load": Selects qualifying GPUs with the most available load

            minFreeLoad (float): Percentage of available load needed by module
            minFreeMemory (float): Percentage of available memory needed by module
            minFreeMemoryMb (float): Megabytes of available memory needed by module
            ignoreIDs (list[int]): GPU to ignore by ID
            ignoreUUIDs (list[int]): GPUs to ignore by UUID
        """
        priority_options = set(["memory", "first", "last", "random", "load"])
        if not priority in priority_options:
            priority = "memory"

        self.limit = limit
        self.priority = priority
        self.minFreeLoad = minFreeLoad

        self.minFreeMemory = minFreeMemory
        self.minFreeMemoryMb = minFreeMemoryMb

        self.ignoreIDs = ignoreIDs
        self.ignoreUUIDs = ignoreUUIDs

    @property
    def GPUs(self):
        return self.get_gpus()

    @property
    def limit(self):
        return self._limit

    @limit.setter
    def limit(self, limit):
        self._limit = max(int(limit), 0)

    @property
    def minFreeMemory(self):
        return self._minFreeMemory

    @minFreeMemory.setter
    def minFreeMemory(self, mem):
        self._minFreeMemory = self._round_to_between_0_and_1(mem)

    @property
    def minFreeLoad(self):
        return self._minFreeLoad

    @minFreeLoad.setter
    def minFreeLoad(self, load):
        self._minFreeLoad = self._round_to_between_0_and_1(load)

    @staticmethod
    def _round_to_between_0_and_1(value):
        """Returns a value, element of [0,1]

        Args:
            value (float): Any number

        Returns:
            0 if value <= 0
            value if 0 < value < 1
            1 if value >= 1
        """
        value = max(value, 0)
        value = min(value, 1)
        return value

    def get_gpus(self, **kwargs):
        """Gets a list of qualifying GPU IDs
        """
        max_load = self._round_to_between_0_and_1(1.0 - self.minFreeLoad)
        max_mem = self._round_to_between_0_and_1(1.0 - self.minFreeMemory)

        log.debug("GPU Requirements")

        table = [
            ("order", self.priority),
            ("maxLoad", max_load),
            ("maxMemory", max_mem),
            ("excludeID", self.ignoreIDs),
            ("excludeUUID", self.ignoreUUIDs),
        ]

        table = tabulate(table, headers=["Parameter", "Value"], tablefmt="simple")
        log.debug(table)

        availableGPUids = GPUtil.getAvailable(
            order=self.priority,
            maxLoad=max_load,
            maxMemory=max_mem,
            excludeID=self.ignoreIDs,
            excludeUUID=self.ignoreUUIDs,
        )
        log.debug("GPU Util Found GPU IDs: " + str(availableGPUids))
        availableGPUids = self._filter_gpus(availableGPUids)
        log.debug("Filtered GPU IDs are: " + str(availableGPUids))

        return availableGPUids

    def _filter_gpus(self, availableGPUids):
        """Filters out GPUs by limit and available memory in megabytes

        Args:
            availableGPUids (list[int]): A list of candidate GPU IDs

        Returns:
            list[int]: A list qualifying of GPU IDs
        """
        GPUs = GPUtil.getGPUs()
        usableGPUids = []
        for GPUid in availableGPUids:
            if len(usableGPUids) == self.limit:
                break

            GPU = GPUs[GPUid]
            if GPU.memoryFree >= self.minFreeMemoryMb:
                usableGPUids.append(GPUid)

        return usableGPUids


class CupyUtility(GPUUtility):
    import numpy
    import importlib.util

    if importlib.util.find_spec("cupy"):
        import cupy
    else:
        cupy = None

    @property
    def xp(self):
        if len(self.GPUs) > 0 and not self.cupy is None:
            return self.cupy
        return self.numpy
