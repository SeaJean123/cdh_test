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
import json
from typing import Any
from typing import Dict
from typing import Optional
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mypy_boto3_lambda import LambdaClient as BotoLambdaClient
    from mypy_boto3_lambda.type_defs import InvocationResponseTypeDef
else:
    BotoLambdaClient = object
    InvocationResponseTypeDef = object


class LambdaClient:
    """Abstracts the boto3 lambda client."""

    def __init__(self, boto_lambda_client: BotoLambdaClient):
        self._client = boto_lambda_client

    def invoke_lambda_async(self, function_name: str, payload: Optional[Dict[str, Any]] = None) -> None:
        """Invoke the given lambda in an asynchronous way."""
        if (
            response := self._client.invoke(
                FunctionName=function_name,
                InvocationType="Event",
                Payload=json.dumps(payload or {}).encode("utf-8"),
            )
        )["StatusCode"] != 202:
            raise LambdaInvokeAsyncError(function_name, response)

    def invoke_lambda_sync(self, function_name: str, payload: Optional[Dict[str, Any]] = None) -> str:
        """Invoke the given lambda in a synchronous way."""
        response = self._client.invoke(
            FunctionName=function_name,
            Payload=json.dumps(payload or {}).encode("utf-8"),
        )
        if response.get("FunctionError") or response["StatusCode"] != 200:
            raise LambdaInvokeSyncError(function_name, response)
        return str(response["Payload"].read().decode("utf-8"))


class LambdaInvokeSyncError(Exception):
    """Signals that a lambda should have been called synchronously but the invocation failed."""

    def __init__(self, name: str, response: InvocationResponseTypeDef):
        super().__init__(f"Error in sync invoke of lambda {name}: {response}")


class LambdaInvokeAsyncError(Exception):
    """Signals that a lambda should have been called asynchronously but the invocation failed."""

    def __init__(self, name: str, response: InvocationResponseTypeDef):
        super().__init__(f"Error in async invoke of lambda {name}: {response}")
