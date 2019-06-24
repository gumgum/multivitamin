import os
import sys
import importlib.util
from setuptools import setup, find_packages
from setuptools.command.install import install

from multivitamin import __version__ as VERSION  # see multivitamin/version.py


reqs = None
with open("requirements.txt") as rf:
    reqs = rf.readlines()

# Install CuPy if CUDA is installed
# if os.path.exists("/usr/local/cuda/version.txt"):
#     with open("/usr/local/cuda/version.txt") as cuda:
#         cuda_ver = cuda.readlines()[0].split()[-1].rsplit(".", 1)[0]
#
#     cupy_supported_cuda_vers = ["8.0", "9.0", "9.1", "9.2"]
#     if cuda_ver in cupy_supported_cuda_vers:
#         reqs.append("cupy-cuda" + cuda_ver.replace(".", ""))


# Install PIMS if PyAV is installed
if importlib.util.find_spec("av"):
    print("PyAV not installed. Will not install PIMS")
    reqs.append("pims")


class VerifyVersionCommand(install):
    """Custom command to verify that the git tag matches our version"""

    description = "Verify that the git tag matches the version"

    def run(self):
        tag = os.getenv("DRONE_TAG", "unknown")

        if tag != VERSION:
            info = "Git tag: {0} does not match the version of this app: {1}".format(
                tag, VERSION
            )
            sys.exit(info)


setup(
    name="multivitamin",
    version=VERSION,
    description="Python framework for serving ML models",
    url="https://github.com/gumgum/multivitamin/",
    author="GumGum ML",
    packages=find_packages(
        exclude=["docs*", "tests*", "examples*", "docker*", "tools*"]
    ),
    install_requires=reqs,
    include_package_data=True,
    zip_safe=False,
    cmdclass={"verify": VerifyVersionCommand},
)
