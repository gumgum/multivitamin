from setuptools import setup, find_packages

reqs = None
with open("requirements.txt") as rf:
    reqs = rf.readlines()

setup(
    packages=find_packages(
        exclude=["docs*", "tests*", "examples*", "docker*", "tools*"]
    ),
    setup_requires=["pbr", "setuptools", "setuptools_scm"],
    pbr=True,
    use_scm_version=True
)
