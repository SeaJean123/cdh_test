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
import logging
import random
import time
from typing import List
from unittest.mock import Mock
from unittest.mock import patch

import pytest

from cdh_core.log import logger
from cdh_core.log.logger import configure_logging
from cdh_core.log.logger import CredentialHandlingLogger
from cdh_core.log.logger import log_exception_frame
from cdh_core.log.logger import MAX_RECORD_LENGTH
from cdh_core_dev_tools.testing.builder import Builder


class TestLogger:
    def setup_method(self) -> None:
        self.log_messages: List[str] = []
        local_messages = self.log_messages

        class LambdaLoggerHandler(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                record.aws_request_id = str(random.randint(0, 10))
                local_messages.append(self.format(record))

        self.original_handlers = logging.root.handlers
        logging.root.handlers = [LambdaLoggerHandler()]  # simulate AWS

    def teardown_method(self) -> None:
        logging.root.handlers = self.original_handlers

    def test_remove_passwords_is_used_by_aws_logger(self) -> None:
        package_name = Builder.build_random_string()
        configure_logging(package_name)
        logging.getLogger(package_name).error("https://foo:bar@abc.com/")
        assert len(self.log_messages) == 1
        assert self.log_messages[0].startswith("[ERROR]\t")
        assert self.log_messages[0].endswith("https://foo:***@abc.com/")

    def test_remove_passwords_url_without_scheme(self) -> None:
        package_name = Builder.build_random_string()
        configure_logging(package_name)
        logging.getLogger(package_name).error("foo:bar@abc")
        assert len(self.log_messages) == 1
        assert self.log_messages[0].startswith("[ERROR]\t")
        assert self.log_messages[0].endswith("foo:***@abc")

    @pytest.mark.parametrize(
        "url",
        [
            "s3://some-bucket/prefix/location",
            "https://sqs.eu-central-1.amazonaws.com/111122223333/some-sqs-queue",
            "https://someapigateway.execute-api.us-west-1.amazonaws.com/",
            "https://us-west-1.console.aws.amazon.com/codebuild/builds/cdh-core-prefix-dev--plan:some-id/view/new",
            "git::ssh://git@github.com:python/mypy.git?ref=v0.910",
            "arn:aws:sts::111122223333:assumed-role/role-name/some.one@cdh.com",
            '{"responsible":"some.one@cdh.com"}',
        ],
    )
    def test_harmless_string_intact(self, url: str) -> None:
        package_name = Builder.build_random_string()
        configure_logging(package_name)
        logging.getLogger(package_name).error(url)
        assert len(self.log_messages) == 1
        assert self.log_messages[0].startswith("[ERROR]\t")
        assert self.log_messages[0].endswith(url)

    @pytest.mark.parametrize("with_scheme", [False, True])
    def test_complex_text_and_exception_with_passwords(self, with_scheme: bool) -> None:
        package_name = Builder.build_random_string()
        configure_logging(package_name)
        url_prefix = "http://" if with_scheme else ""
        sensitive_error = Exception(url_prefix + "qqasdf:password1_.-~@proxy.example.com:8080")
        try:
            raise sensitive_error
        except Exception as error:  # pylint: disable=broad-except
            logging.getLogger(package_name).log(  # noqa: FKA01
                logging.ERROR,
                "Message: %s \rPath: %s Id: %s, \rRequest-Body: %s",
                str(error),
                "/cn/resources/s3/...",
                "d73cbb7f-c921-40df-8cee-1439747a17fc",
                None,
                exc_info=True,
            )
        assert len(self.log_messages) == 1
        assert self.log_messages[0].count(url_prefix + "qqasdf:***@proxy.example.com:8080") == 2
        assert "password!" not in self.log_messages[0]

    @patch.object(logger.CredentialHandlingLogger, "apply_all_filters")
    def test_slow_filtering_logs_error(self, filter_mock: Mock) -> None:
        package_name = Builder.build_random_string()
        configure_logging(package_name)
        log_line = Builder.build_random_string(length=250)
        filter_mock.has_already_slept = False  # noqa: W0212
        logger.MAX_LOG_DURATION = 0.01

        def mock_filtering(formatted_record: str) -> str:
            if log_line in formatted_record and not filter_mock.has_already_slept:
                time.sleep(0.02)
                filter_mock.has_already_slept = True
            return formatted_record

        filter_mock.side_effect = mock_filtering

        logging.getLogger(package_name).info(log_line)
        assert len(self.log_messages) == 2
        assert any(log_line in message for message in self.log_messages)
        assert any(
            msg.startswith("[ERROR]\t") and "Applying log filters took too long" in msg for msg in self.log_messages
        )


class TestAWSFormatter:
    def test_apply_all_filters(self) -> None:
        to_test = "testing https://foo:bar@abc.com/ and ASIASWFCFJ5OXMWJYNNK!"
        expected = "testing https://foo:***@abc.com/ and ***!"
        assert CredentialHandlingLogger.apply_all_filters(to_test) == expected

    def test_compact_json(self) -> None:
        json_object = {
            Builder.build_random_string(): [
                {Builder.build_random_string(): Builder.build_random_string() for _ in range(40)} for _ in range(1000)
            ],
            "login": "user:password@abc.com",
        }
        string_prefix = "Some message about "
        to_test = f"{string_prefix}{json.dumps(json_object, separators=(',', ':'))}"
        assert len(to_test) <= MAX_RECORD_LENGTH
        start = time.perf_counter()
        filtered = CredentialHandlingLogger.apply_all_filters(to_test)
        assert time.perf_counter() - start < 1
        assert filtered.startswith(string_prefix)
        json_parsed = json.loads(filtered[len(string_prefix) :])
        assert json_parsed == json_object | {"login": "user:***@abc.com"}

    @patch("cdh_core.log.logger._USER_PASSWORD_AT")
    def test_apply_all_filters_has_no_leak_in_case_of_error(self, regex_mock: Mock) -> None:
        class RegexError(Exception):
            ...

        regex_mock.finditer.side_effect = RegexError("something went wrong")
        assert CredentialHandlingLogger.apply_all_filters("any_url").startswith(
            "Something went wrong while formatting the log string"
        )

    def test_log_truncated(self) -> None:
        # should be somewhat arbitrary / normal data, "s" * 1000000 does not complete in time
        to_test = "abc-,/123 " * 10**5 + " this should be removed from the output "
        expected = "abc-,/123 " * 99904 + " ... (truncated by core logger)"
        assert CredentialHandlingLogger.apply_all_filters(to_test) == expected

    def test_log_truncated_trailing_password(self) -> None:
        # should be somewhat arbitrary / normal data, "s" * 1000000 does not complete in time
        to_test = (
            "user:password@web.de"
            + ("abc-,/123 " * 99997)
            + "user:pwd222@web.de"
            + (" this should be removed from the output" * 1000)
        )
        expected = "user:***@web.de" + ("abc-,/123 " * 99997) + "user:** ... (truncated by core logger)"
        assert CredentialHandlingLogger.apply_all_filters(to_test) == expected

    @pytest.mark.parametrize(
        "to_test,expected",
        [
            pytest.param("asdf", "asdf", id="no url"),
            pytest.param("https://foo:bar@abc.com/", "https://foo:***@abc.com/", id="only url with password"),
            pytest.param("foo:bar@abc.com/", "foo:***@abc.com/", id="only url with password no scheme"),
            pytest.param("www.example.com", "www.example.com", id="only url without password no scheme"),
            pytest.param("https://abc.com/", "https://abc.com/", id="only url without password"),
            pytest.param(
                "This is an error message for 'http://mynutzer:thepwd_01281@someurl.com:123'",
                "This is an error message for 'http://mynutzer:***@someurl.com:123'",
                id="text with url and password",
            ),
            pytest.param(
                "https://foo:bar@abc.com/ asdf dfd https://bar:baz@123.com/ \n"
                " sadfe https://abc.com/ de https://foo:asdf@abc.com/",
                "https://foo:***@abc.com/ asdf dfd https://bar:***@123.com/ \n"
                " sadfe https://abc.com/ de https://foo:***@abc.com/",
                id="multiple urls with passwords and password is the user of another url and text and newline",
            ),
        ],
    )
    def test_remove_passwords_from_urls(self, to_test: str, expected: str) -> None:
        assert CredentialHandlingLogger.remove_passwords_from_urls(to_test) == expected

    @pytest.mark.parametrize(
        "to_test,expected",
        [
            pytest.param("'asdf'", "'asdf'", id="no credentials"),
            pytest.param("'ASIASWFCFJ5OXMWJYNNK'", "'***'", id="only access key id"),
            pytest.param("'K8hep9RCvwWP9TazNsknXV9G5drceEyL/roX62po'", "'***'", id="only secret access key"),
            pytest.param(
                "'credentials': {'aws_access_key_id': 'ASIASWFCFJ5OXMWJYNNK', 'aws_secret_access_key': "
                "'K8hep9RCvwWP9TazNsknXV9G5drceEyL/roX62po', 'aws_session_token': 'FwoGZXIvYXdzEJv//////////wEaDHs+aSm"
                "5l2BZ5Ja0XSK0AZmw2W0tnCV5nN7lXMOBFeONwyFq7oqgeuz/0G01m3bvgBXv3h8as10AnGt9ERhUYCuzbvibpCJ/z014GUrRbBnq"
                "bSz0LesmzMY1TI0u8spHgymJ9u1XBnMKY1DHX4y0QcADzsoiSRbklg56D8Tz8QxQMq8bTM4/ZG+GexSo0g75FlO1qPRKjcYsm6KgO"
                "iImftxiDBCi20lVR6anaBrP0mlPN1oTQ8DMDfV0Qj9DgZGhZa/aTSji1+iCBjIt07McIhn5FZ/VE5Exc307fTqI+onzWHE/MppwOT"
                "2F12GK1zvVfOnkDYEAG+u0'}, ",
                "'credentials': {'aws_access_key_id': '***', 'aws_secret_access_key': '***', 'aws_session_token': "
                "'***'}, ",
                id="access key id, secret access key, and session token",
            ),
        ],
    )
    def test_remove_aws_credentials(self, to_test: str, expected: str) -> None:
        assert CredentialHandlingLogger.remove_aws_credentials(to_test) == expected

    @pytest.mark.parametrize(
        "to_test,expected",
        [
            pytest.param('"asdf"', '"asdf"', id="no jwt token"),
            pytest.param(
                ': "jwt_prod=eyJ0eXAiOiJKVzI1NiJ9.eyJpYXQiOmNvbXBhbnkiOiJibZCI6ZmFsc2V9Cg.bnkiOiJibZCI6", "next"',
                ': "jwt_prod=***", "next"',
                id="jwt token",
            ),
        ],
    )
    def test_remove_jwt_token(self, to_test: str, expected: str) -> None:
        assert CredentialHandlingLogger.remove_jwt_token(to_test) == expected


class TestLogExceptionFrame:
    # pylint: disable=invalid-name, broad-except
    def test_log_exception_frame(self) -> None:
        log = Mock(logging.Logger)

        def level_1() -> None:
            a = 1
            level_2(a)

        def level_2(a: int) -> None:
            b = 2
            level_3(a, b)

        def level_3(a: int, b: int) -> None:
            c = 3
            raise Exception(a, b, c)

        try:
            level_1()
        except Exception:
            log_exception_frame(log)

        log.error.assert_called_once_with({"a": 1, "b": 2, "c": 3})
