# Copyright (C) 2022, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from pathlib import Path

from setuptools import find_packages
from setuptools import setup

with open(Path(__file__).with_name("requirements.txt"), "r", encoding="utf-8") as file:
    install_requires = file.read().splitlines()


setup(
    name="cdh-core-dev-tools",
    url="https://github.com/bmw-cdh/cdh-core",
    description="Central development library for the cloud data hub (CDH) core",
    version="0.0.1",
    packages=find_packages(include=["*"]),
    package_data={"cdh_core_dev_tools": ["py.typed"]},
    python_requires=">=3.9",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: Apache License 2.0",
    ],
    license="Apache License 2.0",
    install_requires=install_requires,
    scripts=[
        "cdh_core_dev_tools/dependencies/lock_dependencies.py",
        "cdh_core_dev_tools/pre_commit/import_linter.py",
        "cdh_core_dev_tools/pre_commit/liccheck_wrapper.py",
        "cdh_core_dev_tools/pre_commit/format_commit_message.py",
    ],
    author="Cloud Data Hub Team",
    author_email="clouddatahub@bmwgroup.com",
)
