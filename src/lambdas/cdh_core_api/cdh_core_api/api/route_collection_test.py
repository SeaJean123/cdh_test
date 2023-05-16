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
from unittest.mock import Mock

import pytest
from cdh_core_api.api.route_collection import DuplicateRoute
from cdh_core_api.api.route_collection import RouteCollection

from cdh_core.enums.http import HttpVerb
from cdh_core.exceptions.http import MethodNotAllowedError
from cdh_core.exceptions.http import NotFoundError
from cdh_core_dev_tools.testing.assert_raises import assert_raises


class TestRouteCollection:
    def test_several_handlers_for_different_routes(self) -> None:
        collection = RouteCollection()
        handler1 = Mock()
        handler2 = Mock()
        collection.add("/route1", HttpVerb.GET, handler1)
        collection.add("/route2", HttpVerb.GET, handler2)
        assert collection.get("/route1", HttpVerb.GET) is handler1
        assert collection.get("/route2", HttpVerb.GET) is handler2

    def test_several_handlers_for_same_route(self) -> None:
        collection = RouteCollection()
        handler1 = Mock()
        handler2 = Mock()
        collection.add("/same_route", HttpVerb.GET, handler1)
        collection.add("/same_route", HttpVerb.POST, handler2)
        assert collection.get("/same_route", HttpVerb.GET) is handler1
        assert collection.get("/same_route", HttpVerb.POST) is handler2

    def test_missing_handler(self) -> None:
        collection = RouteCollection()
        with pytest.raises(NotFoundError):
            collection.get("/route", HttpVerb.GET)

    def test_method_not_allowed(self) -> None:
        collection = RouteCollection()
        collection.add("/route", HttpVerb.GET, handler=Mock())
        with pytest.raises(MethodNotAllowedError):
            collection.get("/route", HttpVerb.POST)

    def test_two_handlers_for_same_route_and_verb(self) -> None:
        collection = RouteCollection()
        collection.add("/same_route", HttpVerb.GET, handler=Mock())
        with assert_raises(DuplicateRoute("/same_route", HttpVerb.GET)):
            collection.add("/same_route", HttpVerb.GET, handler=Mock())

    def test_overwrite_handler_for_same_route_and_verb_with_force(self) -> None:
        collection = RouteCollection()
        handler1 = Mock()
        handler2 = Mock()
        collection.add("/same_route", HttpVerb.GET, handler1)
        assert collection.get("/same_route", HttpVerb.GET) is handler1
        collection.add("/same_route", HttpVerb.GET, handler2, force=True)
        assert collection.get("/same_route", HttpVerb.GET) is handler2

    def test_must_not_add_handlers_for_options(self) -> None:
        collection = RouteCollection()
        with pytest.raises(ValueError):
            collection.add("/route", HttpVerb.OPTIONS, handler=Mock())

    def test_get_available_http_verbs(self) -> None:
        collection = RouteCollection()
        collection.add("/route", HttpVerb.GET, handler=Mock())
        collection.add("/route", HttpVerb.POST, handler=Mock())
        assert collection.get_available_http_verbs("/route") == [HttpVerb.GET, HttpVerb.POST]

    def test_get_available_http_verbs_on_route_that_does_not_exist(self) -> None:
        collection = RouteCollection()
        with pytest.raises(NotFoundError):
            collection.get_available_http_verbs("/route")
