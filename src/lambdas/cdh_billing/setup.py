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

import pkg_resources
from setuptools import find_packages
from setuptools import setup

with open(Path(__file__).with_name("requirements.in"), "r", encoding="utf-8") as requirements_file:
    requirements = [line for line in requirements_file.readlines() if not line.startswith("-c")]
    if any(line.startswith("-r") for line in requirements):
        raise RuntimeError("Recursively defined dependencies are currently not supported here")
    install_requires = [str(req) for req in pkg_resources.parse_requirements(requirements)]

setup(
    name="cdh-billing",
    url="https://github.com/bmw-cdh/cdh-core",
    description="Lambda to regularly update accounts with billing information",
    version="0.0.1",
    packages=find_packages(include=["*"]),
    package_data={"cdh_billing": ["py.typed"]},
    python_requires=">=3.9",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: Apache License 2.0",
    ],
    license="Apache License 2.0",
    install_requires=install_requires,
    author="Cloud Data Hub Team",
    author_email="clouddatahub@bmwgroup.com",
)
