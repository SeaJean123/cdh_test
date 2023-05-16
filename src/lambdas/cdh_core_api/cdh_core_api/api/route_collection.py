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
from typing import Callable
from typing import Dict
from typing import List

from cdh_core.entities.response import Response
from cdh_core.enums.http import HttpVerb
from cdh_core.exceptions.http import MethodNotAllowedError
from cdh_core.exceptions.http import NotFoundError

AnyHandler = Callable[..., Response]


class RouteCollection:
    """Stores the routes for each endpoint."""

    def __init__(self) -> None:
        self._handlers: Dict[str, Dict[HttpVerb, AnyHandler]] = {}

    def add(self, route: str, http_verb: HttpVerb, handler: AnyHandler, force: bool = False) -> None:
        """Register a new route.

        The last one which gets registered wins it all, if force is True.
        If force is false and the same route is already registered, a 'DuplicateRoute' exception is raised.
        """
        if route not in self._handlers:
            self._handlers[route] = {}
        if http_verb is HttpVerb.OPTIONS:
            raise ValueError("Must not add handlers for OPTIONS")
        if http_verb in self._handlers[route] and not force:
            raise DuplicateRoute(route, http_verb)
        self._handlers[route][http_verb] = handler

    def get(self, route: str, http_verb: HttpVerb) -> AnyHandler:
        """Return the handler for the route/http verb combination."""
        # Normally, the following errors should already have been caught by API Gateway.
        if route not in self._handlers:
            raise NotFoundError(f"Route {route} does not exist")
        if http_verb not in self._handlers[route]:
            raise MethodNotAllowedError(f"Route {route} does not support HTTP method {http_verb.value}")
        return self._handlers[route][http_verb]

    def get_available_http_verbs(self, route: str) -> List[HttpVerb]:
        """Return a list of http verbs for the given route."""
        if route not in self._handlers:
            raise NotFoundError(f"Route {route} does not exist")
        return list(self._handlers[route].keys())


class DuplicateRoute(Exception):
    """Signals that a route has been registered already."""

    def __init__(self, route: str, http_verb: HttpVerb):
        super().__init__(f"Several handlers were defined for {http_verb.value} {route}")
