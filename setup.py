import sys
from setuptools import setup, find_packages

if sys.version_info[0] < 3:
    import __builtin__ as builtins
else:
    import builtins
with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="orch", 
    version="1.0.dev1",
    author="Juan-Carlos Maureira",
    author_email="jmaureir@gmail.com",
    description="Tasks and Data orchestrator for distributed/parallel pipelines implementation",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/jmaureir/orchestrator",
    packages=find_packages(),
    install_requires=['jsonpickle',"dill","taskit","promise"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
