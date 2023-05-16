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
from __future__ import annotations

import inspect
from dataclasses import dataclass
from enum import Enum
from graphlib import CycleError
from graphlib import TopologicalSorter
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Set
from typing import TypeVar

AnyCallable = Callable[..., Any]
Factory = TypeVar("Factory", bound=AnyCallable)


def get_parameter_names(any_callable: AnyCallable) -> Set[str]:
    """Return a set with names of the given function arguments."""
    return set(inspect.signature(any_callable).parameters)


class DependencyManager:
    """Tracks dependencies for functions based on the argument names."""

    class TimeToLive(Enum):
        """Defines how long a dependency is valid."""

        FOREVER = "forever"
        PER_REQUEST = "per_request"

    class Register:
        """Stores Entries based on a name."""

        @dataclass(frozen=True)
        class Entry:
            """An element of the register."""

            time_to_live: DependencyManager.TimeToLive
            factory: AnyCallable

        def __init__(self) -> None:
            self._factory_register: Dict[str, DependencyManager.Register.Entry] = {}

        def add_new_factory(
            self, factory: AnyCallable, alias: str, time_to_live: DependencyManager.TimeToLive, force: bool
        ) -> None:
            """
            Add a new factory to the register.

            The last one which gets registered wins it all, if force is True.
            If force is false and the same factory is already registered, a 'DependencyAlreadyRegisteredError' is
            raised.
            """
            if force or alias not in self._factory_register:
                self._factory_register[alias] = DependencyManager.Register.Entry(
                    time_to_live=time_to_live, factory=factory
                )
            else:
                raise DependencyAlreadyRegisteredError()

        def build_dependency_graph(self, time_to_live: Set[DependencyManager.TimeToLive]) -> Dict[str, Set[str]]:
            """
            Build a graph based on the registered factories and their dependencies.

            Can be filtered by the time to live flag to only include the matching factories.
            """
            return {
                alias: get_parameter_names(entry.factory)
                for alias, entry in self._factory_register.items()
                if entry.time_to_live in time_to_live
            }

        def get_entry(self, key: str) -> Entry:
            """Return the registered entry if possible."""
            try:
                return self._factory_register[key]
            except KeyError as error:
                raise FactoryNotFoundError() from error

    def __init__(self) -> None:
        self._register = DependencyManager.Register()
        self._forever_builds: Dict[str, Any] = {}
        self._locked = False

    def register(self, alias: str, time_to_live: TimeToLive, force: bool = False) -> Callable[[Factory], Factory]:
        """
        Register the given function at the dependency manager.

        The last one which gets registered overrides formerly registered entries with the same alias, if force is True.
        If force is false a second register will raise an exception.
        """
        if self._locked:
            if time_to_live is DependencyManager.TimeToLive.FOREVER:
                raise LockedDependencyError(
                    f"The registration has been completed, new {DependencyManager.TimeToLive.FOREVER} are not allowed."
                )
            if time_to_live is DependencyManager.TimeToLive.PER_REQUEST:
                try:
                    if self._register.get_entry(alias).time_to_live is DependencyManager.TimeToLive.FOREVER:
                        raise LockedDependencyError(
                            f"Cannot override a {DependencyManager.TimeToLive.FOREVER}"
                            f" with {DependencyManager.TimeToLive.PER_REQUEST}."
                        )
                except FactoryNotFoundError:
                    pass

        def decorator(factory: Factory) -> Factory:
            self._register.add_new_factory(factory, alias, time_to_live, force)
            return factory

        return decorator

    def register_constant(self, alias: str, time_to_live: TimeToLive, value: Any, force: bool = False) -> None:
        """Register a single value as dependency."""
        self.register(alias=alias, time_to_live=time_to_live, force=force)(lambda: value)

    def validate_dependencies(self) -> None:
        """
        Check if the so far registered dependencies are consistent.

        This fails if:
         - a forever depends on a per request
         - a forever has an unregistered dependency
         - there is cycle within the dependencies
        """
        graph = self._register.build_dependency_graph(set(DependencyManager.TimeToLive))
        # checks there is no cycle
        try:
            TopologicalSorter(graph).prepare()
        except CycleError as error:
            raise CycleFoundError("There is a cycle in the dependencies.") from error
        # check there is no forever after a per_request
        for vertex, target_vertices in graph.items():
            if self._register.get_entry(vertex).time_to_live == DependencyManager.TimeToLive.FOREVER:
                for target in target_vertices:
                    # check every target vertex exists
                    try:
                        target_factory = self._register.get_entry(target)
                    except FactoryNotFoundError as error:
                        raise MissingDeclaredFunctionError(f"{target!r} is required but not defined.") from error
                    if target_factory.time_to_live == DependencyManager.TimeToLive.PER_REQUEST:
                        raise LifecycleInconsistencyError(f"{target!r} cannot follow {vertex!r}.")

    def _get_dependency_resolution_order(self, time_to_live_filter: Set[DependencyManager.TimeToLive]) -> List[str]:
        dependency_graph = self._register.build_dependency_graph(time_to_live_filter)
        order = list(TopologicalSorter(dependency_graph).static_order())  # includes unregistered dependencies
        return sorted(dependency_graph.keys(), key=order.index)

    def build_dependencies(self, include_per_request_dependencies: bool = True) -> Dict[str, Any]:
        """
        Build and return dependencies using the registered factories.

        This fails if dependencies for a forever dependency are not available.
        Will build all forever dependencies and as many per-request dependencies as possible.
        Once this method has been called, no new forever dependencies can be registered and existing forever
        dependencies cannot be overwritten anymore.
        """
        self.validate_dependencies()
        ttl_filter = (
            set(DependencyManager.TimeToLive)
            if include_per_request_dependencies
            else {DependencyManager.TimeToLive.FOREVER}
        )
        forever_builds: Dict[str, Any] = {**self._forever_builds}
        all_builds: Dict[str, Any] = {**self._forever_builds}
        for vertex in self._get_dependency_resolution_order(ttl_filter):
            if vertex in all_builds:
                continue
            entry = self._register.get_entry(vertex)
            try:
                kwargs = {argument: all_builds[argument] for argument in get_parameter_names(entry.factory)}
            except KeyError as error:
                if entry.time_to_live == DependencyManager.TimeToLive.PER_REQUEST:
                    continue  # cannot provide this per-request dependency at this point
                raise InconsistentDependencyError(  # should have been caught by validate_dependencies()
                    f"Failed to build {entry.time_to_live.value} dependency {vertex!r} due to a missing "
                    f"dependency {error!r}."
                ) from error
            all_builds[vertex] = entry.factory(**kwargs)
            if entry.time_to_live == DependencyManager.TimeToLive.FOREVER:
                forever_builds[vertex] = all_builds[vertex]
        self._finalize(forever_builds)
        return all_builds

    def _finalize(self, forever_builds: Dict[str, Any]) -> None:
        self._forever_builds = forever_builds
        self._locked = True

    def build_forever_dependencies(self) -> Dict[str, Any]:
        """Return all dependencies which have the time to live FOREVER."""
        return self.build_dependencies(include_per_request_dependencies=False)

    def build_dependencies_for_callable(
        self,
        any_callable: AnyCallable,
    ) -> Dict[str, Any]:
        """
        Return only the dependencies the any_callable requires.

        This fails if the handler requires a dependency which has not been registered yet.
        """
        deps = self.build_dependencies()
        required_by_handler = get_parameter_names(any_callable)
        try:
            return {keyword: deps[keyword] for keyword in required_by_handler}
        except KeyError as error:
            raise MissingDeclaredFunctionError() from error


class DependencyManagerError(Exception):
    """Signals that the DependencyManager has a problem."""


class DependencyAlreadyRegisteredError(DependencyManagerError):
    """Signals that the given dependency has been registered already."""


class FactoryNotFoundError(DependencyManagerError):
    """Signals that the requested factory cannot be found."""


class LockedDependencyError(DependencyManagerError):
    """Signals that the alias given is empty, which is not allowed."""


class InconsistentDependencyError(DependencyManagerError):
    """Signals that the dependencies have a flaw."""


class CycleFoundError(InconsistentDependencyError):
    """Signals that the dependencies form a cycle."""


class LifecycleInconsistencyError(InconsistentDependencyError):
    """Signals that forever dependency cannot depend on a per request dependency."""


class MissingDeclaredFunctionError(InconsistentDependencyError):
    """Signals that the a dependency is requested but not provided."""
