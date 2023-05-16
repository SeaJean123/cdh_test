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
# pylint: disable=invalid-name,unused-argument
import pytest

from cdh_core.manager.dependency_manager import CycleFoundError
from cdh_core.manager.dependency_manager import DependencyAlreadyRegisteredError
from cdh_core.manager.dependency_manager import DependencyManager
from cdh_core.manager.dependency_manager import LifecycleInconsistencyError
from cdh_core.manager.dependency_manager import LockedDependencyError
from cdh_core.manager.dependency_manager import MissingDeclaredFunctionError


class TestDependencyManager:
    def setup_method(self) -> None:
        self.dependency_manager = DependencyManager()

    # pylint: disable=protected-access
    def test_register(self) -> None:
        @self.dependency_manager.register("f1", DependencyManager.TimeToLive.FOREVER)
        def function1(a: int) -> None:
            return None

        @self.dependency_manager.register("f2", DependencyManager.TimeToLive.PER_REQUEST)
        def function2(b: bool) -> None:
            return None

        assert len(self.dependency_manager._register._factory_register) == 2
        assert "f1" in self.dependency_manager._register._factory_register
        assert "f2" in self.dependency_manager._register._factory_register
        assert (
            self.dependency_manager._register._factory_register[  # pylint: disable=comparison-with-callable
                "f1"
            ].factory
            == function1
        )
        assert (
            self.dependency_manager._register._factory_register["f1"].time_to_live
            == DependencyManager.TimeToLive.FOREVER
        )
        assert (
            self.dependency_manager._register._factory_register["f2"].time_to_live
            == DependencyManager.TimeToLive.PER_REQUEST
        )

    def test_validate_dependencies(self) -> None:
        @self.dependency_manager.register("a", DependencyManager.TimeToLive.FOREVER)
        def function_a(b: int, c: int) -> None:
            pass

        @self.dependency_manager.register("b", DependencyManager.TimeToLive.FOREVER)
        def function_b() -> None:
            pass

        @self.dependency_manager.register("c", DependencyManager.TimeToLive.FOREVER)
        def function_c(d: int) -> None:
            pass

        @self.dependency_manager.register("d", DependencyManager.TimeToLive.FOREVER)
        def function_d() -> None:
            pass

        self.dependency_manager.validate_dependencies()

    def test_validate_dependencies_cycle(self) -> None:
        # a -> c -> d -> a
        @self.dependency_manager.register("a", DependencyManager.TimeToLive.FOREVER)
        def function_a(b: int, c: int) -> None:
            pass

        @self.dependency_manager.register("b", DependencyManager.TimeToLive.FOREVER)
        def function_b() -> None:
            pass

        @self.dependency_manager.register("c", DependencyManager.TimeToLive.FOREVER)
        def function_c(d: int) -> None:
            pass

        @self.dependency_manager.register("d", DependencyManager.TimeToLive.FOREVER)
        def function_d(a: int) -> None:
            pass

        with pytest.raises(CycleFoundError):
            self.dependency_manager.validate_dependencies()

    def test_validate_dependencies_miss_match(self) -> None:
        @self.dependency_manager.register("a", DependencyManager.TimeToLive.FOREVER)
        def function_a(b: int) -> None:
            pass

        @self.dependency_manager.register("b", DependencyManager.TimeToLive.PER_REQUEST)
        def function_b() -> None:
            pass

        with pytest.raises(LifecycleInconsistencyError):
            self.dependency_manager.validate_dependencies()

    def test_validate_dependencies_missing_target_function_forever(self) -> None:
        @self.dependency_manager.register("a", DependencyManager.TimeToLive.FOREVER)
        def function(b: int) -> None:
            pass

        with pytest.raises(MissingDeclaredFunctionError):
            self.dependency_manager.validate_dependencies()

    def test_validate_dependencies_missing_target_function_per_request(self) -> None:
        @self.dependency_manager.register("a", DependencyManager.TimeToLive.PER_REQUEST)
        def function(b: int) -> None:
            pass

        self.dependency_manager.validate_dependencies()
        assert self.dependency_manager.build_dependencies() == {}

    def test_build_dependencies(self) -> None:
        @self.dependency_manager.register("a", DependencyManager.TimeToLive.FOREVER)
        def function_a(b: int, c: int) -> int:
            return 1 + b + c

        @self.dependency_manager.register("b", DependencyManager.TimeToLive.FOREVER)
        def function_b() -> int:
            return 2

        @self.dependency_manager.register("c", DependencyManager.TimeToLive.FOREVER)
        def function_c(d: int) -> int:
            return d + 3

        @self.dependency_manager.register("d", DependencyManager.TimeToLive.FOREVER)
        def function_d() -> int:
            return 4

        self.dependency_manager.validate_dependencies()
        assert self.dependency_manager.build_dependencies() == {"b": 2, "d": 4, "c": 7, "a": 10}
        assert self.dependency_manager.build_dependencies() == {"b": 2, "d": 4, "c": 7, "a": 10}

    def test_build_dependencies_with_dynamic_elements(self) -> None:
        @self.dependency_manager.register("a", DependencyManager.TimeToLive.PER_REQUEST)
        def function_a(b: int, c: int) -> int:
            return 1 + b + c

        @self.dependency_manager.register("b", DependencyManager.TimeToLive.FOREVER)
        def function_b() -> int:
            return 2

        @self.dependency_manager.register("c", DependencyManager.TimeToLive.PER_REQUEST)
        def function_c(d: int) -> int:
            return d + 3

        call_count = [0]

        @self.dependency_manager.register("d", DependencyManager.TimeToLive.PER_REQUEST)
        def function_d() -> int:
            call_count[0] += 1
            return 3 + call_count[0]

        self.dependency_manager.validate_dependencies()
        assert self.dependency_manager.build_dependencies() == {"b": 2, "d": 4, "c": 7, "a": 10}
        assert self.dependency_manager.build_dependencies() == {"b": 2, "d": 5, "c": 8, "a": 11}
        assert self.dependency_manager.build_dependencies() == {"b": 2, "d": 6, "c": 9, "a": 12}

    def test_build_dependencies_with_cache(self) -> None:
        call_count = [0]

        @self.dependency_manager.register("a", DependencyManager.TimeToLive.PER_REQUEST)
        def function_a(b: int) -> int:
            return 1 + b

        @self.dependency_manager.register("b", DependencyManager.TimeToLive.FOREVER)
        def function_b() -> int:
            if not call_count[0]:
                call_count[0] += 1
                return 2
            raise AssertionError()

        self.dependency_manager.validate_dependencies()
        assert self.dependency_manager.build_dependencies() == {"a": 3, "b": 2}
        assert self.dependency_manager.build_dependencies() == {"a": 3, "b": 2}

    def test_build_dependencies_with_multiple_required(self) -> None:
        call_count = [0]

        @self.dependency_manager.register("a", DependencyManager.TimeToLive.PER_REQUEST)
        def function_a(b: int) -> int:
            return 1 + b

        @self.dependency_manager.register("b", DependencyManager.TimeToLive.FOREVER)
        def function_b() -> int:
            if not call_count[0]:
                call_count[0] += 1
                return 2
            raise AssertionError()

        @self.dependency_manager.register("c", DependencyManager.TimeToLive.PER_REQUEST)
        def function_c(b: int) -> int:
            return 1 + b

        self.dependency_manager.validate_dependencies()
        assert self.dependency_manager.build_dependencies() == {"a": 3, "b": 2, "c": 3}

    def test_build_dependencies_override_existing_forever(self) -> None:
        @self.dependency_manager.register("a", DependencyManager.TimeToLive.FOREVER)
        def function_a() -> int:
            return 1

        self.dependency_manager.validate_dependencies()
        assert self.dependency_manager.build_dependencies() == {"a": 1}

        with pytest.raises(LockedDependencyError):

            @self.dependency_manager.register("a", DependencyManager.TimeToLive.PER_REQUEST)
            def function_a1() -> int:
                return 1

    def test_build_dependencies_add_forever_after_build(self) -> None:
        self.dependency_manager.validate_dependencies()
        assert self.dependency_manager.build_dependencies() == {}

        with pytest.raises(LockedDependencyError):

            @self.dependency_manager.register("a", DependencyManager.TimeToLive.FOREVER)
            def function_a() -> int:
                return 1

    def test_build_dependencies_from_lambda(self) -> None:
        self.dependency_manager.register("a", DependencyManager.TimeToLive.FOREVER)(lambda b: b + 1)
        self.dependency_manager.register("b", DependencyManager.TimeToLive.FOREVER)(lambda: 1)
        self.dependency_manager.validate_dependencies()
        assert self.dependency_manager.build_dependencies() == {"a": 2, "b": 1}

    def test_build_dependencies_for_handler(self) -> None:
        self.dependency_manager.register("a", DependencyManager.TimeToLive.FOREVER)(lambda b: b + 1)
        self.dependency_manager.register("b", DependencyManager.TimeToLive.FOREVER)(lambda: 1)
        self.dependency_manager.register("c", DependencyManager.TimeToLive.FOREVER)(lambda: 2)

        def handler(a: int, b: int) -> None:
            raise AssertionError()

        self.dependency_manager.validate_dependencies()
        assert self.dependency_manager.build_dependencies_for_callable(handler) == {"a": 2, "b": 1}

    def test_build_dependencies_for_handler_missing_dependency(self) -> None:
        def handler(a: int, b: int) -> None:
            raise AssertionError()

        self.dependency_manager.validate_dependencies()
        with pytest.raises(MissingDeclaredFunctionError):
            self.dependency_manager.build_dependencies_for_callable(handler)

    def test_build_dependencies_with_class(self) -> None:
        @self.dependency_manager.register("a", DependencyManager.TimeToLive.FOREVER)
        class ClassA:
            def __init__(self, b: int) -> None:
                self.a = b + 1

            def any_function(self, c: int) -> bool:
                return True

        @self.dependency_manager.register("b", DependencyManager.TimeToLive.FOREVER)
        def function_b() -> int:
            return 2

        self.dependency_manager.validate_dependencies()
        assert set(self.dependency_manager.build_dependencies().keys()) == {"a", "b"}
        assert self.dependency_manager.build_dependencies()["b"] == 2
        assert isinstance(self.dependency_manager.build_dependencies()["a"], ClassA)
        assert self.dependency_manager.build_dependencies()["a"].a == 3
        assert self.dependency_manager.build_dependencies()["a"].any_function(3)

    def test_register_dependencies_twice_without_force(self) -> None:
        @self.dependency_manager.register("a", DependencyManager.TimeToLive.FOREVER)
        def function_a() -> None:
            pass

        with pytest.raises(DependencyAlreadyRegisteredError):

            @self.dependency_manager.register("a", DependencyManager.TimeToLive.FOREVER)
            def function_a_new() -> None:
                pass

    def test_register_dependencies_twice_with_force(self) -> None:
        @self.dependency_manager.register("a", DependencyManager.TimeToLive.FOREVER)
        def function_a() -> int:
            return 1

        @self.dependency_manager.register("a", DependencyManager.TimeToLive.FOREVER, force=True)
        def function_a_new() -> int:
            return 2

        self.dependency_manager.validate_dependencies()
        assert self.dependency_manager.build_dependencies() == {"a": 2}

    def test_register_constant(self) -> None:
        for name, value in {"a": 1, "b": 2, "c": 3}.items():
            self.dependency_manager.register_constant(name, DependencyManager.TimeToLive.FOREVER, value)
        self.dependency_manager.validate_dependencies()
        assert self.dependency_manager.build_dependencies() == {"a": 1, "b": 2, "c": 3}

    # pylint: disable=cell-var-from-loop
    def test_register_without_constant(self) -> None:
        """DON'T DO THIS AT HOME! Use register_constant."""
        for name, value in {"a": 1, "b": 2, "c": 3}.items():
            self.dependency_manager.register(name, DependencyManager.TimeToLive.FOREVER)(lambda: value)  # noqa: B023
        self.dependency_manager.validate_dependencies()
        assert self.dependency_manager.build_dependencies() == {"a": 3, "b": 3, "c": 3}

    @pytest.mark.parametrize("error_type", [Exception, KeyError, IndexError, ValueError])
    def test_error_in_factory_is_raised(self, error_type: type) -> None:
        @self.dependency_manager.register("a", DependencyManager.TimeToLive.FOREVER)
        def i_will_raise() -> None:
            raise error_type("this was bound to fail")

        self.dependency_manager.validate_dependencies()
        with pytest.raises(error_type):
            self.dependency_manager.build_dependencies()

    def test_retry_error_in_factory(self) -> None:
        calls = []
        error = Exception("Sorry, I always fail at first")

        @self.dependency_manager.register("a", DependencyManager.TimeToLive.FOREVER)
        def fail_on_first_call() -> int:
            calls.append(1)
            if len(calls) == 1:
                raise error
            return 0

        with pytest.raises(Exception) as exc_info:
            self.dependency_manager.build_dependencies()
        assert exc_info.value == error

        dependencies = self.dependency_manager.build_dependencies()
        assert dependencies["a"] == 0

    def test_retry_error_in_factory_can_register_new_forever(self) -> None:
        calls = []
        error = Exception("Sorry, I always fail at first")

        @self.dependency_manager.register("a", DependencyManager.TimeToLive.FOREVER)
        def fail_on_first_call() -> int:
            calls.append(1)
            if len(calls) == 1:
                raise error
            return 0

        with pytest.raises(Exception) as exc_info:
            self.dependency_manager.build_dependencies()
        assert exc_info.value == error

        self.dependency_manager.register_constant("b", DependencyManager.TimeToLive.FOREVER, 1)
        dependencies = self.dependency_manager.build_dependencies()
        assert dependencies["a"] == 0
        assert dependencies["b"] == 1

    def test_retry_error_in_factory_can_overwrite_existing_not_yet_built(self) -> None:
        calls = []
        error = Exception("Sorry, I always fail at first")

        @self.dependency_manager.register("a", DependencyManager.TimeToLive.FOREVER)
        def fail_on_first_call() -> int:
            calls.append(1)
            if len(calls) == 1:
                raise error
            return 0

        @self.dependency_manager.register("b", DependencyManager.TimeToLive.FOREVER)
        def add_one(a: int) -> int:
            return a + 1

        with pytest.raises(Exception) as exc_info:
            self.dependency_manager.build_dependencies()
        assert exc_info.value == error

        @self.dependency_manager.register("b", DependencyManager.TimeToLive.FOREVER, force=True)
        def add_two(a: int) -> int:
            return a + 2

        dependencies = self.dependency_manager.build_dependencies()
        assert dependencies["a"] == 0
        assert dependencies["b"] == 2

    def test_retry_error_in_factory_rebuild_already_built(self) -> None:
        calls = []
        error = Exception("Sorry, I always fail at first")
        self.dependency_manager.register_constant("b", DependencyManager.TimeToLive.FOREVER, 1)

        @self.dependency_manager.register("a", DependencyManager.TimeToLive.FOREVER)
        def fail_on_first_call(b: int) -> int:
            calls.append(1)
            if len(calls) == 1:
                raise error
            return b

        with pytest.raises(Exception) as exc_info:
            self.dependency_manager.build_dependencies()
        assert exc_info.value == error

        self.dependency_manager.register_constant("b", DependencyManager.TimeToLive.FOREVER, 2, force=True)

        dependencies = self.dependency_manager.build_dependencies()
        assert dependencies["a"] == 2
        assert dependencies["b"] == 2
