import warnings
from multivitamin.utils.GPUUtilities import GPUUtility, CupyUtility


def test_init():
    gpu_utility = GPUUtility()
    gpu_ids = gpu_utility.GPUs
    assert len(gpu_ids) >= 0


def test_get_gpus():
    gpu_utility = GPUUtility()
    gpu_ids = gpu_utility.get_gpus()
    assert len(gpu_ids) >= 0


def test_limit_property():
    gpu_utility = GPUUtility(limit=2.2)
    assert gpu_utility.limit == 2
    gpu_utility.limit = 3.4
    assert gpu_utility.limit == 3


def test_minFreeMemory_property():
    gpu_utility = GPUUtility(minFreeMemory=2.2)
    assert gpu_utility.minFreeMemory == 1
    gpu_utility.minFreeMemory = -2
    assert gpu_utility.minFreeMemory == 0
    gpu_utility.minFreeLoad = 0.5
    assert gpu_utility.minFreeLoad == 0.5


def test_minFreeLoad_property():
    gpu_utility = GPUUtility(minFreeLoad=2.2)
    assert gpu_utility.minFreeLoad == 1
    gpu_utility.minFreeLoad = -5
    assert gpu_utility.minFreeLoad == 0
    gpu_utility.minFreeLoad = 0.25
    assert gpu_utility.minFreeLoad == 0.25


def test_cupy_numpy_conversion():
    cupy_utility = CupyUtility(minFreeMemory=1.0)
    assert cupy_utility.xp.__name__ == "numpy"

    cupy_utility = CupyUtility()
    if not cupy_utility.xp.__name__ == "cupy":
        warnings.warn(
            RuntimeWarning(
                "Warning! No GPU with available load or memory. Verify first!"
            )
        )
