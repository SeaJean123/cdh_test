#!/bin/bash
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

# Bundle Python code either as Lambda deployment package (--package) or as Lambda layer (--layer).

set -euo pipefail

task="${1:?Specify either --layer or --package}"
filename=$2
code_path=$(realpath $3)

ARTIFACTS_DIR="/tmp/artifacts"
mkdir -p "${ARTIFACTS_DIR}"
build_dir=$(mktemp -d)

pushd $build_dir > /dev/null

case $task in
  --layer)
    # Lambda unzips layers to /opt. The Python path contains /opt/python.
    mkdir "${build_dir}/python"
    cp -r "${code_path}/." "${build_dir}/python/"
    ;;
  --package)
    cp -r "${code_path}/." "${build_dir}/"
    config_path=$(realpath $4)
    cp "${config_path}" "${build_dir}/"
    ;;
esac


find . -name "*_test.py" -o -name '*.pyc' -delete
# The following line is necessary for deterministic zips
find . -exec touch -t 200001010000.00 {} +
zip -Xqr output.zip *

hash=($(find . ! -path "output.zip" -type f -exec sha256sum {} \; | sort -k 1 | sha256sum | cut -f 1 -d " "))
filename="${filename}-${hash}.zip"
mv output.zip "${ARTIFACTS_DIR}/${filename}"
rm -rf "$build_dir"

jq -n --arg file "${ARTIFACTS_DIR}/${filename}" '{"file":$file}'
popd > /dev/null
