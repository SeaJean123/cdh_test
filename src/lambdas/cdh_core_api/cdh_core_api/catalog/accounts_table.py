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
from abc import abstractmethod
from datetime import datetime
from typing import Any
from typing import Dict
from typing import Generic
from typing import List
from typing import Optional
from typing import Type
from typing import TypeVar

from cdh_core_api.catalog.base import BaseTable
from cdh_core_api.catalog.base import conditional_check_failed
from cdh_core_api.catalog.base import create_model
from cdh_core_api.catalog.base import DateTimeAttribute
from cdh_core_api.catalog.base import DynamoItemIterator
from cdh_core_api.catalog.base import LastEvaluatedKey
from cdh_core_api.generic_types import GenericAccount
from pynamodb.attributes import JSONAttribute
from pynamodb.attributes import ListAttribute
from pynamodb.attributes import MapAttribute
from pynamodb.attributes import NumberAttribute
from pynamodb.attributes import UnicodeAttribute
from pynamodb.exceptions import DoesNotExist
from pynamodb.exceptions import PutError
from pynamodb.exceptions import UpdateError
from pynamodb.expressions.update import Action
from pynamodb.models import Model
from pynamodb_attributes import UnicodeEnumAttribute

from cdh_core.entities.accounts import Account
from cdh_core.entities.accounts import AccountRole
from cdh_core.entities.accounts import AccountRoleType
from cdh_core.entities.arn import Arn
from cdh_core.enums.accounts import AccountType
from cdh_core.enums.accounts import Affiliation
from cdh_core.enums.dataset_properties import BusinessObject
from cdh_core.enums.dataset_properties import Layer
from cdh_core.enums.hubs import Hub
from cdh_core.enums.resource_properties import Stage
from cdh_core.optionals import apply_if_not_none
from cdh_core.primitives.account_id import AccountId

GenericModel = TypeVar("GenericModel")


class _AccountAttributeContainer(Model):
    class AccountRoleAttribute(MapAttribute[str, str]):
        name = UnicodeAttribute()
        path = UnicodeAttribute()
        type = UnicodeEnumAttribute(AccountRoleType)
        friendly_name = UnicodeAttribute()

    account_id = UnicodeAttribute(hash_key=True)
    admin_roles: ListAttribute[str] = ListAttribute()
    affiliation = UnicodeEnumAttribute(Affiliation)
    business_objects: ListAttribute[BusinessObject] = ListAttribute()
    cost_history: MapAttribute[str, Any] = MapAttribute(null=True)  # type: ignore[no-untyped-call]
    data: JSONAttribute = JSONAttribute()
    estimated_cost: NumberAttribute = NumberAttribute(null=True)
    forecasted_cost: NumberAttribute = NumberAttribute(null=True)
    friendly_name: UnicodeAttribute = UnicodeAttribute()
    group = UnicodeAttribute(null=True)
    hub = UnicodeEnumAttribute(Hub)
    layers: ListAttribute[Layer] = ListAttribute()
    provider_role_arns: MapAttribute[str, str] = MapAttribute()  # type: ignore[no-untyped-call]
    responsibles: ListAttribute[str] = ListAttribute()
    request_id: UnicodeAttribute = UnicodeAttribute(null=True)
    roles: ListAttribute[AccountRoleAttribute] = ListAttribute(of=AccountRoleAttribute)
    stages: ListAttribute[Stage] = ListAttribute()
    visible_in_hubs: ListAttribute[Hub] = ListAttribute()
    type = UnicodeEnumAttribute(AccountType)
    creation_date = DateTimeAttribute()
    update_date = DateTimeAttribute()


class GenericAccountModel(Generic[GenericAccount], _AccountAttributeContainer):
    """Generic model for accounts.

    Inherit from this class to create a model for a specific account class.
    """

    @abstractmethod
    def to_account(self) -> GenericAccount:
        """Convert an account model to an account instance."""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def from_account(cls: Type[GenericModel], account: GenericAccount) -> GenericModel:
        """Convert an account instance to an account model."""
        raise NotImplementedError


class GenericAccountsTable(Generic[GenericAccount], BaseTable):
    """Generic table for accounts.

    Inherit from this class to create a table for a specific account class.
    """

    def __init__(self, model_cls: Type[GenericAccountModel[GenericAccount]], prefix: str = "") -> None:
        self._model = create_model(table=f"{prefix}cdh-accounts", model=model_cls, module=__name__)

    def get_all_accounts(self, consistent_read: bool = True) -> List[GenericAccount]:
        """Get all accounts from the table."""
        result = self._model.scan(consistent_read=consistent_read)
        return [item.to_account() for item in result]

    def get(self, account_id: AccountId) -> GenericAccount:
        """Get one account from the table."""
        return self._get_model(account_id).to_account()

    def get_accounts_iterator(
        self, consistent_read: bool = True, last_evaluated_key: Optional[LastEvaluatedKey] = None
    ) -> DynamoItemIterator[GenericAccount]:
        """Get paginated Accounts."""
        result_iterator = self._model.scan(
            consistent_read=consistent_read,
            last_evaluated_key=last_evaluated_key,
        )
        return DynamoItemIterator(
            items=(model.to_account() for model in result_iterator),
            get_last_evaluated_key=lambda: apply_if_not_none(LastEvaluatedKey)(result_iterator.last_evaluated_key),
        )

    def _get_model(self, account_id: AccountId) -> GenericAccountModel[GenericAccount]:
        try:
            return self._model.get(account_id, consistent_read=True)
        except DoesNotExist as error:
            raise AccountNotFound(account_id) from error

    def create(self, account: GenericAccount) -> None:
        """Create an account in the table."""
        model = self._model.from_account(account)
        try:
            model.save(self._model.account_id.does_not_exist())
        except PutError as error:
            if conditional_check_failed(error):
                raise AccountAlreadyExists(account.id) from error

            raise error

    def delete(self, account_id: AccountId) -> None:
        """Delete an account from the table.

        This function shall only be called when no left-over datasets/resources are guaranteed.
        """
        model = self._get_model(account_id)
        model.delete()

    def update(  # pylint: disable=too-many-arguments,too-many-locals
        self,
        account_id: AccountId,
        update_date: datetime,
        admin_roles: Optional[List[str]] = None,
        affiliation: Optional[Affiliation] = None,
        business_objects: Optional[List[BusinessObject]] = None,
        cost_history: Optional[Dict[str, float]] = None,
        data: Optional[Dict[str, str]] = None,
        estimated_cost: Optional[float] = None,
        forecasted_cost: Optional[float] = None,
        friendly_name: Optional[str] = None,
        group: Optional[str] = None,
        hub: Optional[Hub] = None,
        layers: Optional[List[Layer]] = None,
        responsibles: Optional[List[str]] = None,
        roles: Optional[List[AccountRole]] = None,
        stages: Optional[List[Stage]] = None,
        type: Optional[AccountType] = None,  # pylint: disable=redefined-builtin
        visible_in_hubs: Optional[List[Hub]] = None,
    ) -> GenericAccount:
        """Update an account."""
        model = self._get_model(account_id)
        actions = self._get_update_actions(
            admin_roles=admin_roles,
            affiliation=affiliation,
            business_objects=business_objects,
            cost_history=cost_history,
            data=data,
            estimated_cost=estimated_cost,
            forecasted_cost=forecasted_cost,
            friendly_name=friendly_name,
            group=group,
            hub=hub,
            layers=layers,
            responsibles=responsibles,
            roles=roles,
            stages=stages,
            type=type,
            visible_in_hubs=visible_in_hubs,
        )
        self._apply_actions(model, actions=actions, update_date=update_date)
        return model.to_account()

    def _get_update_actions(  # pylint: disable=too-many-arguments,too-many-locals,too-many-branches
        self,
        admin_roles: Optional[List[str]] = None,
        affiliation: Optional[Affiliation] = None,
        business_objects: Optional[List[BusinessObject]] = None,
        cost_history: Optional[Dict[str, float]] = None,
        data: Optional[Dict[str, str]] = None,
        estimated_cost: Optional[float] = None,
        forecasted_cost: Optional[float] = None,
        friendly_name: Optional[str] = None,
        group: Optional[str] = None,
        hub: Optional[Hub] = None,
        layers: Optional[List[Layer]] = None,
        responsibles: Optional[List[str]] = None,
        roles: Optional[List[AccountRole]] = None,
        stages: Optional[List[Stage]] = None,
        type: Optional[AccountType] = None,  # pylint: disable=redefined-builtin
        visible_in_hubs: Optional[List[Hub]] = None,
    ) -> List[Action]:
        actions: List[Action] = []
        if admin_roles is not None:
            actions.append(self._model.admin_roles.set(admin_roles))
        if affiliation is not None:
            actions.append(self._model.affiliation.set(affiliation))
        if business_objects is not None:
            actions.append(
                self._model.business_objects.set([business_object.value for business_object in business_objects])
            )
        if cost_history is not None:
            actions.append(self._model.cost_history.set(cost_history))
        if data is not None:
            actions.append(self._model.data.set(data))
        if estimated_cost is not None:
            actions.append(self._model.estimated_cost.set(estimated_cost))
        if forecasted_cost is not None:
            actions.append(self._model.forecasted_cost.set(forecasted_cost))
        if friendly_name:
            actions.append(self._model.friendly_name.set(friendly_name))
        if group:
            actions.append(self._model.group.set(group))
        if hub is not None:
            actions.append(self._model.hub.set(hub))
        if layers is not None:
            actions.append(self._model.layers.set([layer.value for layer in layers]))
        if responsibles is not None:
            actions.append(AccountModel.responsibles.set(responsibles))
        if roles is not None:
            actions.append(
                AccountModel.roles.set(
                    [
                        {
                            "name": role.name,
                            "path": role.path,
                            "type": role.type,
                            "friendly_name": role.friendly_name,
                        }
                        for role in roles
                    ]
                )
            )
        if stages is not None:
            actions.append(AccountModel.stages.set([stage.value for stage in stages]))
        if type is not None:
            actions.append(AccountModel.type.set(type))
        if visible_in_hubs is not None:
            actions.append(AccountModel.visible_in_hubs.set([hub.value for hub in visible_in_hubs]))

        return actions

    def exists(self, account_id: AccountId) -> bool:
        """Check if an account exists."""
        try:
            self._get_model(account_id)
            return True
        except AccountNotFound:
            return False

    @staticmethod
    def _apply_actions(
        model: GenericAccountModel[GenericAccount], actions: List[Action], update_date: datetime
    ) -> None:
        if actions:
            actions.append(AccountModel.update_date.set(update_date))
            try:
                model.update(actions=actions, condition=AccountModel.account_id.exists())
            except UpdateError as error:
                if conditional_check_failed(error):
                    raise AccountNotFound(model.account_id) from error
                raise error


class AccountModel(GenericAccountModel[Account]):
    """Model for accounts."""

    def to_account(self) -> Account:
        """Convert an account model to an account instance."""
        return Account(
            id=AccountId(self.account_id),
            admin_roles=self.admin_roles,
            business_objects=[BusinessObject(item) for item in self.business_objects],
            cost_history=None
            if self.cost_history is None
            else {
                str(key): float(value)
                for key, value in self.cost_history.as_dict().items()  # type: ignore[no-untyped-call]
            },
            data=self.data,
            estimated_cost=self.estimated_cost,
            forecasted_cost=self.forecasted_cost,
            friendly_name=self.friendly_name,
            group=self.group,
            hub=Hub(self.hub),
            layers=[Layer(item) for item in self.layers],
            provider_role_arns=(
                {
                    AccountId(acc_id): Arn(arn)
                    for acc_id, arn in self.provider_role_arns.as_dict().items()  # type: ignore[no-untyped-call]
                }
            ),
            responsibles=self.responsibles,
            request_id=self.request_id,
            roles=[
                AccountRole(
                    name=role.name, path=role.path, type=AccountRoleType(role.type), friendly_name=role.friendly_name
                )
                for role in self.roles
            ],
            stages=[Stage(item) for item in self.stages],
            type=self.type,
            affiliation=self.affiliation,
            visible_in_hubs=[Hub(hub) for hub in self.visible_in_hubs],
            creation_date=self.creation_date,
            update_date=self.update_date,
        )

    @classmethod
    def from_account(cls, account: Account) -> "AccountModel":
        """Convert an account instance to an account model."""
        return cls(
            account_id=account.id,
            admin_roles=account.admin_roles,
            affiliation=account.affiliation,
            business_objects=[bo.value for bo in account.business_objects],
            cost_history=account.cost_history,
            data=account.data,
            estimated_cost=account.estimated_cost,
            forecasted_cost=account.forecasted_cost,
            friendly_name=account.friendly_name,
            group=account.group,
            hub=account.hub,
            layers=[layer.value for layer in account.layers],
            provider_role_arns={account_id: str(arn) for account_id, arn in account.provider_role_arns.items()},
            responsibles=account.responsibles,
            request_id=account.request_id,
            roles=[
                cls.AccountRoleAttribute(
                    name=role.name,
                    path=role.path,
                    type=role.type,
                    friendly_name=role.friendly_name,
                )  # type: ignore[no-untyped-call]
                for role in account.roles
            ],
            visible_in_hubs=[hub.value for hub in account.visible_in_hubs],
            stages=[stage.value for stage in account.stages],
            type=account.type,
            creation_date=account.creation_date,
            update_date=account.update_date,
        )


class AccountsTable(GenericAccountsTable[Account]):
    """Table for accounts."""

    def __init__(self, prefix: str = "") -> None:
        super().__init__(model_cls=AccountModel, prefix=prefix)


class AccountNotFound(Exception):
    """Exception to be thrown when an account is not found."""

    def __init__(self, account_id: str):
        super().__init__(f"Account {account_id} was not found")


class AccountAlreadyExists(Exception):
    """Exception to be thrown when an account exists already."""

    def __init__(self, account_id: str):
        super().__init__(f"Account {account_id} already exists")
