"""
Helper for ASAM ODS HTTP API conI session

Example:

    ```
    with ConI(
        url="http://localhost:8087/api",
        auth=("sa", "sa")
    ) as con_i:
        units = con_i.query_data({"AoUnit": {}})
    ```
"""

import logging
from typing import List

import requests
import requests.auth
from google.protobuf.json_format import MessageToJson
from google.protobuf.message import Message
from pandas import DataFrame

import odsbox.proto.ods_pb2 as ods
from odsbox.datamatrices_to_pandas import to_pandas
from odsbox.jaquel import jaquel_to_ods
from odsbox.model_cache import ModelCache
from odsbox.transaction import Transaction


class ConI:
    """
    This is a helper to hold an ASAM ODS HTTP API ConI session.

    Example:

        ```
        with ConI(
            url="http://localhost:8087/api",
            auth=("sa", "sa")
        ) as con_i:
            units = con_i.query_data({"AoUnit": {}})
        ```
    """

    __log: logging.Logger = logging.getLogger(__name__)
    __session: requests.Session = None
    __con_i: str = None
    mc: ModelCache = None

    def __init__(
        self,
        url: str = "http://localhost:8080/api",
        auth: requests.auth.AuthBase = ("sa", "sa"),
        context_variables: ods.ContextVariables | dict | None = None,
        verify_certificate: bool = True,
    ):
        """
        Create a session object keeping track of ASAM ODS session URL named `conI`.

        Example:

            ```
            with ConI(
                url="http://localhost:8087/api",
                auth=("sa", "sa")
            ) as con_i:
                units = con_i.query_data({"AoUnit": {}})
            ```

        :param str url: base url of the ASAM ODS API of a given server. An example is "http://localhost:8080/api".
        :param requests.auth.AuthBase auth: An auth object to be used for the used requests package.
            For basic auth `("USER", "PASSWORD")` can be used.
        :param ods.ContextVariables | dict | None context_variables: If context variables are necessary for the
            connection they are passed here. It defaults to None.
        :param bool verify_certificate: If no certificate is provided for https insecure access can be enabled.
            It defaults to True.
        :raises requests.HTTPError: If connection to ASAM ODS server fails.
        """
        session = requests.Session()
        session.auth = auth
        session.headers.update(
            {
                "Content-Type": "application/x-asamods+protobuf",
                "Accept": "application/x-asamods+protobuf",
            }
        )
        session.verify = verify_certificate
        session.timeout = 600.0

        _context_variables = None
        if isinstance(context_variables, ods.ContextVariables):
            _context_variables = context_variables
        else:
            _context_variables = ods.ContextVariables()
            if isinstance(context_variables, dict):
                for key, value in context_variables.items():
                    _context_variables.variables[key].string_array.values.append(value)

        response = session.post(url + "/ods", data=_context_variables.SerializeToString())
        if 201 == response.status_code:
            con_i = response.headers["location"]
            self.__log.debug("ConI: %s", con_i)
            self.__session = session
            self.__con_i = con_i
        self.__check_result(response)
        # lets cache the model
        self.model_read()

    def __del__(self):
        if None is not self.__session:
            self.logout()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.logout()

    def con_i_url(self) -> str:
        """
        Get the ASAM ODS session URL used to work with this session.

        :return str: The ASAM ODS session URL
        """
        return self.__con_i

    def logout(self):
        """
        Close the attached session at the ODS server by calling delete on the session URL
        and closing the requests session.

        :raises requests.HTTPError: If delete the ASAM ODS session fails.
        """
        if None is not self.__session:
            response = self.__session.delete(
                self.__con_i,
            )
            self.__session.close()
            self.__session = None
            self.__con_i = None
            self.__check_result(response)

    def query_data(self, query: str | dict | ods.SelectStatement) -> DataFrame:
        """
        Query ods server for content and return the results as Pandas DataFrame

        :param str | dict | ods.SelectStatement query: Query given as JAQueL query (dict or str)
            or as an ASAM ODS SelectStatement.
        :raises requests.HTTPError: If query fails.
        :return DataFrame: The DataMatrices as Pandas.Dataframe. The columns are named as `ENTITY_NAME.ATTRIBUTE_NAME`.
            `IsNull` values are not marked invalid.
        """
        if isinstance(query, ods.SelectStatement):
            return to_pandas(self.data_read(self.data_read_jaquel(query)))
        else:
            return to_pandas(self.data_read_jaquel(query))

    def model(self) -> ods.Model:
        """
        Get the cache ODS server model. This model will return the cached
        application model related to your session.

        :return ods.Model: The application model of the ASAM ODS server.
        """
        return self.mc.model()

    def data_read_jaquel(self, jaquel: str | dict) -> ods.DataMatrices:
        """
        Query ods server for content.

        :param str | dict  jaquel: Query given as JAQueL query (dict or str).
        :raises requests.HTTPError: If query fails.
        :return ods.DataMatrices: The DataMatrices representing the result.
            It will contain one ods.DataMatrix for each returned entity type.
        """
        _ods_entity, ods_query = jaquel_to_ods(self.model(), jaquel)
        return self.data_read(ods_query)

    def data_read(self, select_statement: ods.SelectStatement) -> ods.DataMatrices:
        """
        Query ods server for content.

        :param ods.SelectStatement  select_statement: Query given as ASAM ODS SelectStatement.
        :raises requests.HTTPError: If query fails.
        :return ods.DataMatrices: The DataMatrices representing the result.
            It will contain one ods.DataMatrix for each returned entity type.
        """
        response = self.ods_post_request("data-read", select_statement)
        return_value = ods.DataMatrices()
        return_value.ParseFromString(response.content)
        return return_value

    def data_create(self, data: ods.DataMatrices) -> List[int]:
        """
        Create new ASAM ODS instances or write bulk data.

        :param ods.DataMatrices data: Matrices containing columns for instances to be created.
        :raises requests.HTTPError: If creation fails.
        :return List[int]: list of ids created from your request.
        """
        response = self.ods_post_request("data-create", data)
        return_value = ods.DataMatrices()
        return_value.ParseFromString(response.content)
        return return_value.matrices[0].columns[0].longlong_array.values

    def data_update(self, data: ods.DataMatrices) -> None:
        """
        Update existing instances.

        :param ods.DataMatrices data: Matrices containing columns for instances to be updated.
            The `id` column is used to identify the instances to be updated.
        :raises requests.HTTPError: If update fails.
        """
        self.ods_post_request("data-update", data)

    def data_delete(self, data: ods.DataMatrices) -> None:
        """
        Delete existing instances.

        :param ods.DataMatrices data: Matrices containing columns for instances to be deleted.
            The `id` column is used to identify the instances to be deleted.
        :raises requests.HTTPError: If delete fails.
        """
        self.ods_post_request("data-delete", data)

    def data_copy(self, copy_request: ods.CopyRequest) -> ods.Instance:
        """
        Copy an Instance and its related children.

        :param ods.CopyRequest copy_request: Define instance to be copied.
        :raises requests.HTTPError: If copy fails.
        :return ods.Instance: Newly created instance
        """
        response = self.ods_post_request("data-copy", copy_request)
        return_value = ods.Instance()
        return_value.ParseFromString(response.content)
        return return_value

    def n_m_relation_read(self, identifier: ods.NtoMRelationIdentifier) -> ods.NtoMRelatedInstances:
        """
        Read n-m relations for a defined instance.

        :param ods.NtoMRelationIdentifier identifier: identify n to m relation to be read.
        :raises requests.HTTPError: If read fails.
        :return ods.NtoMRelatedInstances: Return n to m related instances that were queried.
        """
        response = self.ods_post_request("n-m-relation-read", identifier)
        return_value = ods.NtoMRelatedInstances()
        return_value.ParseFromString(response.content)
        return return_value

    def n_m_relation_write(self, related_instances: ods.NtoMWriteRelatedInstances) -> None:
        """
        Update, delete or create n-m relations for given instance pairs.

        :raises requests.HTTPError: If write fails.
        :param ods.NtoMWriteRelatedInstances related_instances: related instances to be
            updated, deleted or created.
        """
        self.ods_post_request("n-m-relation-write", related_instances)

    def transaction(self) -> Transaction:
        """
        Open a transaction object to be used in a with clause

        Example:
        ```
        with con_i.transaction() as transaction:
            # do writing
            transaction.commit()
        ```

        :raises requests.HTTPError: If creation of transaction fails.
        :return Transaction: transaction object that will abort automatically if commit is not called.
        """
        return Transaction(self)

    def transaction_create(self) -> None:
        """
        Open a transaction for writing.
        :raises requests.HTTPError: If creation of transaction fails.
        """
        self.ods_post_request("transaction-create")

    def transaction_commit(self) -> None:
        """
        Commit transaction created before.
        :raises requests.HTTPError: If creation of transaction fails.
        """
        self.ods_post_request("transaction-commit")

    def transaction_abort(self) -> None:
        """
        Abort transaction created before.
        :raises requests.HTTPError: If creation of transaction fails.
        """
        self.ods_post_request("transaction-abort")

    def valuematrix_read(self, request: ods.ValueMatrixRequestStruct) -> ods.DataMatrices:
        """
        Read bulk data from a submatrix or measurement.
        Submatrix access can also be done using data-read.

        :param ods.ValueMatrixRequestStruct request: Define measurement or submatrix to
            create ASAM ODS ValueMatrix for.
        :raises requests.HTTPError: If ValueMatrix access fails.
        :return ods.DataMatrices: DataMatrices containing the bulk data for the request.
        """
        response = self.ods_post_request("valuematrix-read", request)
        return_value = ods.DataMatrices()
        return_value.ParseFromString(response.content)
        return return_value

    def model_read(self) -> ods.Model:
        """
        Read the model from server and update cached version.

        :raises requests.HTTPError: If model read fails.
        :return ods.Model: The application model of the server.
        """
        response = self.ods_post_request("model-read")
        model = ods.Model()
        model.ParseFromString(response.content)
        self.mc = ModelCache(model)
        return model

    def model_update(self, model_parts: ods.Model, update_model: bool = True) -> None:
        """
        Update application model content. This method is used to modify existing items or
        create new ones.

        :param ods.Model model_parts: parts of the model to be updated or created.
        :param bool update_model: determine if the model cache of this ConI instance should be
            updated by reading the whole model again. It defaults to True.
        :raises requests.HTTPError: If model update fails.
        """
        self.ods_post_request("model-update", model_parts)
        if update_model:
            # cache again if successfully changed
            self.model_read()

    def model_delete(self, model_parts: ods.Model, update_model: bool = True) -> None:
        """
        Delete application model content.

        :param ods.Model model_parts: define model parts to be deleted.
        :param bool update_model: determine if the model cache of this ConI instance should be
            updated by reading the whole model again. It defaults to True.
        :raises requests.HTTPError: If model update fails.
        """
        self.ods_post_request("model-delete", model_parts)
        if update_model:
            # cache again if successfully changed
            self.model_read()

    def model_check(self) -> None:
        """
        Check if stored application model is consistent.

        :raises requests.HTTPError: If model contains errors.
        """
        self.ods_post_request("model-check")

    def basemodel_read(self) -> ods.BaseModel:
        """
        Read the ODS base model version used by the server.

        :raises requests.HTTPError: If reading base model fails.
        :return ods.BaseModel: used server base model.
        """
        response = self.ods_post_request("basemodel-read")
        base_model = ods.BaseModel()
        base_model.ParseFromString(response.content)
        return base_model

    def asampath_create(self, instance: ods.Instance) -> ods.AsamPath:
        """
        Create an persistent string representing the instance.

        :param ods.Instance instance: Instance to be get AsamPath for.
        :raises requests.HTTPError: If creation fails.
        :return ods.AsamPath: The AsamPath that represents the instance.
        """
        response = self.ods_post_request("asampath-create", instance)
        return_value = ods.AsamPath()
        return_value.ParseFromString(response.content)
        return return_value

    def asampath_resolve(self, asam_path: ods.AsamPath) -> ods.Instance:
        """
        Use the persistent string to get back the instance.

        :param ods.AsamPath asam_path: AsamPath to be resolved.
        :raises requests.HTTPError: If path could not be resolved.
        :return ods.Instance: Instance represented by AsamPath.
        """
        response = self.ods_post_request("asampath-resolve", asam_path)
        return_value = ods.Instance()
        return_value.ParseFromString(response.content)
        return return_value

    def context_read(self, pattern_or_filter: ods.ContextVariablesFilter | str = "*") -> ods.ContextVariables:
        """
        Read the session context variables.

        :param ods.ContextVariablesFilter | str pattern_or_filter: Context variable filter as str
            or ContextVariablesFilter. It defaults to "*" to return all variables.
        :raises requests.HTTPError: If something went wrong.
        :return ods.ContextVariables: ContextVariables where the name matches the filter.
        """
        context_variables_filter = (
            pattern_or_filter
            if isinstance(pattern_or_filter, ods.ContextVariablesFilter)
            else ods.ContextVariablesFilter(pattern=pattern_or_filter)
        )
        response = self.ods_post_request("context-read", context_variables_filter)
        return_value = ods.ContextVariables()
        return_value.ParseFromString(response.content)
        return return_value

    def context_update(self, context_variables: ods.ContextVariables):
        """
        Set context variables for current session. This will set context variables for the given session.
        If new session is created they will fall back to their default.

        :param ods.ContextVariables context_variables: ContextVariables to be set or updated.
        :raises requests.HTTPError: If something went wrong.
        """
        self.ods_post_request("context-update", context_variables)

    def password_update(self, password_update: ods.PasswordUpdate) -> None:
        """
        Update the password of the defined user.

        :param ods.PasswordUpdate password_update: Defines for which user the password should eb updated.
        :raises requests.HTTPError: If something went wrong.
        """
        self.ods_post_request("password-update", password_update)

    def ods_post_request(self, relative_url_part: str, message: Message | None = None) -> requests.Response:
        """
        Do ODS post call with the given relative URL.

        :param str relative_url_part: url part that is joined to conI URL using `/`.
        :param Message | None message: protobuf message to be send, defaults to None
        :raises requests.HTTPError: If status code is not 200 or 201.
        :return requests.Response: requests response if successful.
        """

        response = self.__session.post(
            self.__con_i + "/" + relative_url_part,
            data=message.SerializeToString() if message is not None else None,
        )
        self.__check_result(response)
        return response

    def __check_result(self, response: requests.Response):
        if response.status_code not in (200, 201):
            response.headers
            if (
                "Content-Type" in response.headers
                and "application/x-asamods+protobuf" == response.headers["Content-Type"]
            ):
                error_info = ods.ErrorInfo()
                error_info.ParseFromString(response.content)
                raise requests.HTTPError(
                    MessageToJson(error_info),
                    response=response,
                )
            response.raise_for_status()
