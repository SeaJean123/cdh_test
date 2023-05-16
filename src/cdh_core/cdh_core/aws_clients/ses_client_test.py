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
import random
import re
from base64 import b64decode
from typing import cast
from typing import List
from typing import Tuple
from unittest.mock import DEFAULT
from unittest.mock import Mock
from unittest.mock import patch

import boto3
import pytest

from cdh_core.aws_clients.ses_client import AWS_DAILY_QUOTA_EXCEEDED_ERROR_MSG
from cdh_core.aws_clients.ses_client import AWS_MAXIMUM_RATE_EXCEEDED_ERROR_MSG
from cdh_core.aws_clients.ses_client import AWS_RECIPIENT_LIMIT
from cdh_core.aws_clients.ses_client import DailyEmailQuotaExceeded
from cdh_core.aws_clients.ses_client import Email
from cdh_core.aws_clients.ses_client import EmailsPerSecond
from cdh_core.aws_clients.ses_client import FailedToSendMail
from cdh_core.aws_clients.ses_client import MaximalSendRateExceeded
from cdh_core.aws_clients.ses_client import RawMessageBuilder
from cdh_core.aws_clients.ses_client import SesClient
from cdh_core.aws_clients.ses_client import TemplateEmail
from cdh_core.enums.aws_test import build_region
from cdh_core_dev_tools.testing.builder import Builder


@patch.object(RawMessageBuilder, "build", return_value={"Data": Builder.build_random_string()})
class TestSesClient:
    def setup_method(self) -> None:
        self.recipients = [Builder.build_random_email() for _ in range(AWS_RECIPIENT_LIMIT + 1)]
        self.sender = Builder.build_random_email()
        self.email_template = Mock(TemplateEmail)
        self.max_rate = EmailsPerSecond(9001)
        self.boto_client = Mock()
        self.client = SesClient(self.boto_client)

    @pytest.mark.parametrize("include_full_recipients_list", [True, False])
    @pytest.mark.usefixtures("mock_ses")
    def test_send_mail_successful(  # pylint: disable=unused-argument
        self, mock_message_builder: Mock, include_full_recipients_list: bool
    ) -> None:
        boto_client = boto3.client("ses", region_name=build_region().value)
        boto_client.verify_email_address(EmailAddress=self.sender)
        client = SesClient(boto_client)

        client.send_mail(
            recipients=self.recipients,
            sender=self.sender,
            email_template=self.email_template,
            max_rate=self.max_rate,
            include_full_recipients_list=include_full_recipients_list,
        )

        assert boto_client.get_send_statistics()["SendDataPoints"][0]["DeliveryAttempts"] == len(self.recipients)

    def test_recipient_always_in_to_field(self, mock_message_builder: Mock) -> None:
        self.client.send_mail(
            recipients=self.recipients,
            sender=self.sender,
            email_template=self.email_template,
            max_rate=self.max_rate,
            include_full_recipients_list=True,
        )

        message_builder_calls = mock_message_builder.call_args_list
        boto_calls = self.boto_client.send_raw_email.call_args_list
        assert len(message_builder_calls) == len(boto_calls)
        assert all(len(builder_call.args[0]) <= AWS_RECIPIENT_LIMIT for builder_call in message_builder_calls)
        assert all(
            boto_call.kwargs["Destinations"][0] in builder_call.args[0]
            for builder_call, boto_call in zip(message_builder_calls, boto_calls)
        )

    def test_generating_messages_fails(self, mock_message_builder: Mock) -> None:  # pylint: disable=unused-argument
        error = Exception(Builder.build_random_string())
        number_of_recipients, number_of_failures, _ = self.setup_errors(self.email_template.apply, error)

        with pytest.raises(FailedToSendMail) as exc:
            self.client.send_mail(
                recipients=self.recipients,
                sender=self.sender,
                email_template=self.email_template,
                max_rate=self.max_rate,
            )

        assert self.email_template.apply.call_count == number_of_recipients
        actual_error = exc.value
        assert str(number_of_recipients) in str(actual_error)
        assert str(number_of_failures) in str(actual_error)

    def test_sending_messages_fails(self, mock_message_builder: Mock) -> None:  # pylint: disable=unused-argument
        error = Builder.build_client_error(Builder.build_random_string())
        number_of_recipients, number_of_failures, _ = self.setup_errors(self.boto_client.send_raw_email, error)

        with pytest.raises(FailedToSendMail) as exc:
            self.client.send_mail(
                recipients=self.recipients,
                sender=self.sender,
                email_template=self.email_template,
                max_rate=self.max_rate,
            )

        assert self.boto_client.send_raw_email.call_count == number_of_recipients
        actual_error = exc.value
        assert str(number_of_recipients) in str(actual_error)
        assert str(number_of_failures) in str(actual_error)

    def test_daily_limit_reached(self, mock_message_builder: Mock) -> None:  # pylint: disable=unused-argument
        error = Builder.build_client_error("ThrottlingException", message=AWS_DAILY_QUOTA_EXCEEDED_ERROR_MSG)
        _, _, position_of_first_failure = self.setup_errors(self.boto_client.send_raw_email, error)

        with pytest.raises(DailyEmailQuotaExceeded):
            self.client.send_mail(
                recipients=self.recipients,
                sender=self.sender,
                email_template=self.email_template,
                max_rate=self.max_rate,
            )

        assert self.boto_client.send_raw_email.call_count == position_of_first_failure

    def test_max_send_rate_exceeded(self, mock_message_builder: Mock) -> None:  # pylint: disable=unused-argument
        error = Builder.build_client_error("ThrottlingException", message=AWS_MAXIMUM_RATE_EXCEEDED_ERROR_MSG)
        _, _, position_of_first_failure = self.setup_errors(self.boto_client.send_raw_email, error)

        with pytest.raises(MaximalSendRateExceeded):
            self.client.send_mail(
                recipients=self.recipients,
                sender=self.sender,
                email_template=self.email_template,
                max_rate=self.max_rate,
            )

        assert self.boto_client.send_raw_email.call_count == position_of_first_failure

    def test_message_rejected(self, mock_message_builder: Mock) -> None:  # pylint: disable=unused-argument
        error = Builder.build_client_error("MessageRejected", message="Email address is not verified")
        number_of_recipients, _, _ = self.setup_errors(self.boto_client.send_raw_email, error)

        self.client.send_mail(
            recipients=self.recipients,
            sender=self.sender,
            email_template=self.email_template,
            max_rate=self.max_rate,
        )

        assert self.boto_client.send_raw_email.call_count == number_of_recipients

    def setup_errors(self, failing_function: Mock, error: Exception) -> Tuple[int, int, int]:
        number_of_recipients = len(self.recipients)
        number_of_failures = random.randint(1, number_of_recipients)
        positions_of_failures = random.sample(range(number_of_recipients), number_of_failures)
        position_of_first_failure = min(positions_of_failures) + 1
        failing_function.side_effect = (
            error if i in positions_of_failures else DEFAULT for i in range(number_of_recipients)
        )
        return number_of_recipients, number_of_failures, position_of_first_failure


class TestRawMessageBuilder:
    TEXT_REGEX = r'(Content-Type: text/plain; charset="utf-8"\nMIME-Version: 1.0\nContent-Transfer-Encoding: base64\n\n)(([-A-Za-z0-9+\/=]+\n)*)'  # noqa: E501
    HTML_REGEX = r'(Content-Type: text/html; charset="utf-8"\nMIME-Version: 1.0\nContent-Transfer-Encoding: base64\n\n)(([-A-Za-z0-9+\/=]+\n)*)'  # noqa: E501

    def setup_method(self) -> None:
        self.recipients = [Builder.build_random_email() for _ in range(3)]
        self.sender = Builder.build_random_email()
        self.common_string = Builder.build_random_string()
        self.plain_string = Builder.build_random_string()
        self.html_string = Builder.build_random_string()
        self.unused_string = Builder.build_random_string()
        self.email = Mock(
            Email,
            plain=self.common_string + self.plain_string,
            html=self.common_string + self.html_string,
            subject=Builder.build_random_string(),
        )

    def assert_body_contains(
        self,
        body: str,
        strings: List[str],
        strings_not_in_html: List[str],
        strings_only_in_html: List[str],
        strings_nowhere: List[str],
    ) -> None:
        text_content_encoded = re.search(self.TEXT_REGEX, body).group(2)  # type: ignore
        text_content = b64decode(text_content_encoded).decode("utf-8")
        html_content_encoded = re.search(self.HTML_REGEX, body).group(2)  # type: ignore
        html_content = b64decode(html_content_encoded).decode("utf-8")
        for string in strings:
            assert string in text_content, f"{string!r} not found in text mail"
            assert string in html_content, f"{string!r} not found in HTML mail"
        for string in strings_not_in_html:
            assert string in text_content, f"{string!r} not found in text mail"
            assert string not in html_content, f"{string!r} found in HTML mail"
        for string in strings_only_in_html:
            assert string not in text_content, f"{string!r} found in text mail"
            assert string in html_content, f"{string!r} not found in HTML mail"
        for string in strings_nowhere:
            assert string not in text_content, f"{string!r} found in text mail"
            assert string not in html_content, f"{string!r} found in HTML mail"

    def test_base_content(self) -> None:
        body = cast(str, RawMessageBuilder.build(self.recipients, self.sender, self.email)["Data"])

        assert "X-Priority" not in body
        self.assert_body_contains(
            body=body,
            strings=[self.common_string],
            strings_not_in_html=[self.plain_string],
            strings_only_in_html=[self.html_string],
            strings_nowhere=[self.unused_string],
        )

    def test_set_high_priority(self) -> None:
        body = cast(
            str, RawMessageBuilder.build(self.recipients, self.sender, self.email, set_high_priority=True)["Data"]
        )

        assert "X-Priority: 2" in body
        self.assert_body_contains(
            body=body,
            strings=[self.common_string],
            strings_not_in_html=[self.plain_string],
            strings_only_in_html=[self.html_string],
            strings_nowhere=[self.unused_string],
        )
