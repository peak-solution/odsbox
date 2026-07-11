"""
Helper for ASAM ODS HTTP API conI session

Example::

    from odsbox.con_i import ConI

    with ConI(
        url="http://localhost:8087/api",
        auth=("sa", "sa")
    ) as con_i:
        units = con_i.query_data({"AoUnit": {}})

"""

from __future__ import annotations

import logging
import os
from typing import Any

import requests
import requests.auth
from google.protobuf.json_format import MessageToJson
from google.protobuf.message import Message
from pandas import DataFrame

import odsbox.proto.ods_pb2 as ods
from odsbox.bulk_reader import BulkReader
from odsbox.datamatrices_to_pandas import to_pandas
from odsbox.jaquel import Jaquel
from odsbox.model_cache import ModelCache
from odsbox.security import Security
from odsbox.transaction import Transaction


class ConI:
    """
    This is a helper to hold an ASAM ODS HTTP API ConI session.

    Example::

        from odsbox.con_i import ConI

        with ConI(
            url="http://localhost:8087/api",
            auth=("sa", "sa")
        ) as con_i:
            units = con_i.query_data({"AoUnit": {}})

    """

    __log: logging.Logger = logging.getLogger(__name__)
    __default_http_headers: dict[str, str] = {
        "Content-Type": "application/x-asamods+protobuf",
        "Accept": "application/x-asamods+protobuf",
    }

    def __init__(
        self,
        url: str = "http://localhost:8080/api",
        auth: requests.auth.AuthBase | tuple[str, str] | None = ("sa", "sa"),
        context_variables: ods.ContextVariables | dict[str, str] | None = None,
        verify_certificate: bool = True,
        load_model: bool = True,
        allow_redirects: bool = False,
        connection_timeout: float = 60.0,
        request_timeout: float = 600.0,
        custom_session: requests.Session | None = None,
    ) -> None:
        """
        Create a session object keeping track of ASAM ODS session URL named `conI`.

        Example::

            from odsbox.con_i import ConI

            # basic auth
            with ConI(
                url="http://localhost:8087/api",
                auth=("sa", "sa")
            ) as con_i:
                units = con_i.query_data({"AoUnit": {}})

        Example::

            import requests
            from odsbox.con_i import ConI

            class BearerAuth(requests.auth.AuthBase):
                def __init__(self, token):
                    self.token = token
                def __call__(self, r):
                    r.headers["authorization"] = "Bearer " + self.token
                    return r

            # bearer auth
            with ConI(
                url="http://localhost:8087/api",
                auth=BearerAuth("YOUR_BEARER_TOKEN")
            ) as con_i:
                units = con_i.query_data({"AoUnit": {}})


        Args:
            url: Base URL of the ASAM ODS API of a given server.
                An example is "http://localhost:8080/api".
            auth: Auth object for the requests package.
                For basic auth `("USER", "PASSWORD")` can be used.
                Ignored if `custom_session` is provided.
            context_variables: Context variables for the connection. Defaults to None.
            verify_certificate: If no certificate is provided for https, insecure access
                can be enabled. Defaults to True. Ignored if `custom_session` is provided.
            load_model: Whether to read the model after connection is established. Defaults to True.
            allow_redirects: Whether redirects should be allowed in requests calls. Defaults to False.
            connection_timeout: Timeout in seconds for establishing connections. Defaults to 60.0.
            request_timeout: Timeout in seconds for individual requests. Defaults to 600.0.
            custom_session: A preconfigured requests.Session to use.
                If provided, `auth` and `verify_certificate` parameters are ignored. Defaults to None.
                e.g. Some OAuth packages provide custom `requests.Session` implementations.

        Raises:
            requests.HTTPError: If connection to ASAM ODS server fails.
        """
        self.__session: requests.Session | None = None
        self.__con_i: str | None = None
        self.__security: Security | None = None
        self.__mc: ModelCache | None = None
        self.__allow_redirects: bool = allow_redirects
        self.__bulk_reader: BulkReader | None = None
        self.__connection_timeout: float = connection_timeout
        self.__request_timeout: float = request_timeout

        session = custom_session
        if session is None:
            session = requests.Session()
            session.auth = auth
            session.verify = verify_certificate

        _context_variables = None
        if isinstance(context_variables, ods.ContextVariables):
            _context_variables = context_variables
        else:
            _context_variables = ods.ContextVariables()
            if isinstance(context_variables, dict):
                for key, value in context_variables.items():
                    _context_variables.variables[key].string_array.values.append(value)

        response = session.post(
            url + "/ods",
            data=_context_variables.SerializeToString(),
            timeout=self.__connection_timeout,
            headers=self.__default_http_headers,
            allow_redirects=self.__allow_redirects,
        )
        if 201 == response.status_code:
            con_i = response.headers["location"]
            self.__log.debug("ConI: %s", con_i)
            self.__session = session
            self.__con_i = con_i
        self.check_requests_response(response)
        if load_model:
            # lets cache the model
            self.model_read()

    def __del__(self) -> None:
        self.close()

    def __enter__(self) -> ConI:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: object,
    ) -> None:
        self.close()

    def close(self) -> None:
        """
        Close the attached session at the ODS server by calling delete on the session URL
        and closing the requests session. No exception is raised if logout fails.
        """
        try:
            self.logout()
        except Exception as e:
            self.__log.exception("Exception during logout in close: %s", e)

    def con_i_url(self) -> str:
        """
        Get the ASAM ODS session URL used to work with this session.

        Returns:
            The ASAM ODS session URL.
        """
        if self.__con_i is None:
            raise ValueError("ConI already closed")
        return self.__con_i

    def logout(self) -> None:
        """
        Close the attached session at the ODS server by calling delete on the session URL
        and closing the requests session.

        Raises:
            requests.HTTPError: If deleting the ASAM ODS session fails.
        """
        if self.__session is not None:
            try:
                if self.__con_i is not None:
                    response = self.__session.delete(
                        self.__con_i,
                        timeout=self.__connection_timeout,
                        headers={"Accept": "application/x-asamods+protobuf"},
                        allow_redirects=self.__allow_redirects,
                    )
                    self.check_requests_response(response)
            finally:
                self.__con_i = None

                self.__session.close()
                self.__session = None
                self.__security = None
                self.__bulk_reader = None
                self.__mc = None

    def query(
        self,
        jaquel_query: str | dict[str, Any],
        enum_as_string: bool = True,
        date_as_timestamp: bool = True,
        is_null_to_nan: bool = True,
        result_naming_mode: str = "query",  # "query" or "model"
        **kwargs: Any,
    ) -> DataFrame:
        """
        Query ods server for content using JAQueL query and return the results as Pandas DataFrame.

        This method combines the JAQUEL query language with pandas DataFrames for convenient data access.
        Result column names can be controlled via the `result_naming_mode` parameter to match either
        your query specification (JAQUEL mode) or the schema entity names (model mode).

        Example - Basic Query::

            result = con_i.query({"AoUnit": {}})
            print(result.columns)
            # Output: Index(['Name', 'Id', 'PhysDimension'], ...)

        Example - Query with Column Selection::

            # Select specific columns using query-based column names (default)
            query = {
                "AoUnit": {},
                "$attributes": {
                    "name": 1,
                    "id": 1,
                    "phys_dimension.name": 1
                }
            }
            result = con_i.query(query)
            print(result.columns)
            # Output: Index(['name', 'id', 'phys_dimension.name'], ...)

        Example - Same Query with Model Column Names::

            # Same query but with model/schema column names
            result = con_i.query(query, result_naming_mode="model")
            print(result.columns)
            # Output: Index(['Unit.Name', 'Unit.Id', 'PhysDimension.Name'], ...)


        Args:
            jaquel_query: JAQueL query as dict or str.
            enum_as_string: If True, the model_cache is used to map DT_ENUM/DS_ENUM int values
                to corresponding string values. Defaults to True.
            date_as_timestamp: If True, DT_DATE/DS_DATE strings are converted to pandas Timestamp.
                Defaults to True.
            is_null_to_nan: If True, is_null flags set corresponding values to pd.NA using
                pandas native nullable data types. Defaults to True.
            result_naming_mode: Controls how result column names are generated.
                "query" (default): Uses column names from the JAQUEL query
                (e.g., 'name', 'phys_dimension.name').
                "model": Uses column names from the ods.Model schema
                (e.g., 'Unit.Name', 'PhysDimension.Name').
            **kwargs: Additional arguments passed to `to_pandas`.

        Returns:
            The DataMatrices as Pandas DataFrame with columns named according to `result_naming_mode`.

        Raises:
            requests.HTTPError: If query fails.
        """
        if result_naming_mode not in ("query", "model"):
            raise ValueError(f"result_naming_mode must be 'query' or 'model', got '{result_naming_mode}'")

        jaquel = Jaquel(self.model(), jaquel_query)
        data_matrices = self.data_read(jaquel.select_statement)
        return to_pandas(
            data_matrices,
            model_cache=self.mc,
            enum_as_string=enum_as_string,
            date_as_timestamp=date_as_timestamp,
            is_null_to_nan=is_null_to_nan,
            jaquel_conversion_result=jaquel if result_naming_mode == "query" else None,
            **kwargs,
        )

    def query_data(
        self,
        query: str | dict[str, Any] | ods.SelectStatement,
        enum_as_string: bool = False,
        date_as_timestamp: bool = False,
        is_null_to_nan: bool = False,
        result_naming_mode: str = "model",
        **kwargs: Any,
    ) -> DataFrame:
        """
        Query ods server for content and return the results as Pandas DataFrame.

        This is a lower-level variant of query() with different defaults:
        - Defaults to model column names (result_naming_mode="model")
        - No automatic enum/date/null conversions by default
        - Can accept raw ASAM ODS SelectStatement objects

        Args:
            query: Query given as JAQueL query (dict or str) or as an ASAM ODS SelectStatement.
            enum_as_string: If True, the model_cache is used to map DT_ENUM/DS_ENUM int values
                to corresponding string values. Defaults to False.
            date_as_timestamp: If True, DT_DATE/DS_DATE strings are converted to pandas Timestamp.
                Defaults to False.
            is_null_to_nan: If True, is_null flags set corresponding values to pd.NA using
                pandas native nullable data types. Defaults to False.
            result_naming_mode: Controls how result column names are generated.
                "query": Uses column names from the JAQUEL query.
                "model" (default): Uses column names from the ods.Model schema.
            **kwargs: Additional arguments passed to `to_pandas`.

        Returns:
            The DataMatrices as Pandas DataFrame with columns named according to `result_naming_mode`.

        Raises:
            requests.HTTPError: If query fails.
        """
        if result_naming_mode not in ("query", "model"):
            raise ValueError(f"result_naming_mode must be 'query' or 'model', got '{result_naming_mode}'")

        if isinstance(query, ods.SelectStatement):
            jaquel = None
            select_statement = query
        else:
            jaquel = Jaquel(self.model(), query)
            select_statement = jaquel.select_statement

        data_matrices = self.data_read(select_statement)

        return to_pandas(
            data_matrices,
            model_cache=self.mc,
            enum_as_string=enum_as_string,
            date_as_timestamp=date_as_timestamp,
            is_null_to_nan=is_null_to_nan,
            jaquel_conversion_result=jaquel if result_naming_mode == "query" else None,
            **kwargs,
        )

    def model(self) -> ods.Model:
        """
        Get the cache ODS server model. This model will return the cached
        application model related to your session.

        Returns:
            The application model of the ASAM ODS server.
        """
        return self.mc.model()

    def data_read_jaquel(self, query: str | dict[str, Any]) -> ods.DataMatrices:
        """
        Query ods server for content.

        Args:
            query: Query given as JAQueL query (dict or str).

        Returns:
            The DataMatrices representing the result.
            It will contain one ods.DataMatrix for each returned entity type.

        Raises:
            requests.HTTPError: If query fails.
        """
        jaquel = Jaquel(self.model(), query)
        return self.data_read(jaquel.select_statement)

    def data_read(self, select_statement: ods.SelectStatement) -> ods.DataMatrices:
        """
        Query ods server for content.

        Args:
            select_statement: Query given as ASAM ODS SelectStatement.

        Returns:
            The DataMatrices representing the result.
            It will contain one ods.DataMatrix for each returned entity type.

        Raises:
            requests.HTTPError: If query fails.
        """
        if not isinstance(select_statement, ods.SelectStatement):
            raise TypeError(f"data_read expects 'ods.SelectStatement', got '{type(select_statement).__name__}'")
        response = self.ods_post_request("data-read", select_statement)
        return_value = ods.DataMatrices()
        return_value.ParseFromString(response.content)
        return return_value

    def data_create(self, data: ods.DataMatrices) -> list[int]:
        """
        Create new ASAM ODS instances or write bulk data.

        Args:
            data: Matrices containing columns for instances to be created.

        Returns:
            List of ids created from your request.

        Raises:
            requests.HTTPError: If creation fails.
        """
        if not isinstance(data, ods.DataMatrices):
            raise TypeError(f"data_create expects 'ods.DataMatrices', got '{type(data).__name__}'")
        response = self.ods_post_request("data-create", data)
        return_value = ods.DataMatrices()
        return_value.ParseFromString(response.content)
        return list(return_value.matrices[0].columns[0].longlong_array.values)

    def data_update(self, data: ods.DataMatrices) -> None:
        """
        Update existing instances.

        Args:
            data: Matrices containing columns for instances to be updated.
                The `id` column is used to identify the instances to be updated.

        Raises:
            requests.HTTPError: If update fails.
        """
        if not isinstance(data, ods.DataMatrices):
            raise TypeError(f"data_update expects 'ods.DataMatrices', got '{type(data).__name__}'")
        self.ods_post_request("data-update", data)

    def data_delete(self, data: ods.DataMatrices, timeout: float | None = None) -> None:
        """
        Delete existing instances.

        Args:
            data: Matrices containing columns for instances to be deleted.
                The `id` column is used to identify the instances to be deleted.
            timeout: Maximal time to wait for response. Delete might take longer time.
                Uses the request_timeout from constructor if None.

        Raises:
            requests.HTTPError: If delete fails.
        """
        if not isinstance(data, ods.DataMatrices):
            raise TypeError(f"data_delete expects 'ods.DataMatrices', got '{type(data).__name__}'")
        self.ods_post_request("data-delete", data, timeout=timeout)

    def data_copy(self, copy_request: ods.CopyRequest) -> ods.Instance:
        """
        Copy an Instance and its related children.

        Args:
            copy_request: Define instance to be copied.

        Returns:
            Newly created instance.

        Raises:
            requests.HTTPError: If copy fails.
        """
        if not isinstance(copy_request, ods.CopyRequest):
            raise TypeError(f"data_copy expects 'ods.CopyRequest', got '{type(copy_request).__name__}'")
        response = self.ods_post_request("data-copy", copy_request)
        return_value = ods.Instance()
        return_value.ParseFromString(response.content)
        return return_value

    def n_m_relation_read(self, identifier: ods.NtoMRelationIdentifier) -> ods.NtoMRelatedInstances:
        """
        Read n-m relations for a defined instance.

        Args:
            identifier: Identify n to m relation to be read.

        Returns:
            The n to m related instances that were queried.

        Raises:
            requests.HTTPError: If read fails.
        """
        if not isinstance(identifier, ods.NtoMRelationIdentifier):
            raise TypeError(
                f"n_m_relation_read expects 'ods.NtoMRelationIdentifier', got '{type(identifier).__name__}'"
            )
        response = self.ods_post_request("n-m-relation-read", identifier)
        return_value = ods.NtoMRelatedInstances()
        return_value.ParseFromString(response.content)
        return return_value

    def n_m_relation_write(self, related_instances: ods.NtoMWriteRelatedInstances) -> None:
        """
        Update, delete or create n-m relations for given instance pairs.

        Args:
            related_instances: Related instances to be updated, deleted or created.

        Raises:
            requests.HTTPError: If write fails.
        """
        if not isinstance(related_instances, ods.NtoMWriteRelatedInstances):
            raise TypeError(
                f"n_m_relation_write expects 'ods.NtoMWriteRelatedInstances', got '{type(related_instances).__name__}'"
            )
        self.ods_post_request("n-m-relation-write", related_instances)

    def transaction(self) -> Transaction:
        """
        Open a transaction object to be used in a with clause.

        Example::

            with con_i.transaction() as transaction:
                # do writing
                transaction.commit()

        Returns:
            Transaction object that will abort automatically if commit is not called.

        Raises:
            requests.HTTPError: If creation of transaction fails.
        """
        return Transaction(self)

    def transaction_create(self) -> None:
        """
        Open a transaction for writing.

        Raises:
            requests.HTTPError: If creation of transaction fails.
        """
        self.ods_post_request("transaction-create")

    def transaction_commit(self) -> None:
        """
        Commit transaction created before.

        Raises:
            requests.HTTPError: If commit of transaction fails.
        """
        self.ods_post_request("transaction-commit")

    def transaction_abort(self) -> None:
        """
        Abort transaction created before.

        Raises:
            requests.HTTPError: If abort of transaction fails.
        """
        self.ods_post_request("transaction-abort")

    def valuematrix_read(self, request: ods.ValueMatrixRequestStruct) -> ods.DataMatrices:
        """
        Read bulk data from a submatrix or measurement.
        Submatrix access can also be done using data-read.

        Args:
            request: Define measurement or submatrix to create ASAM ODS ValueMatrix for.

        Returns:
            DataMatrices containing the bulk data for the request.

        Raises:
            requests.HTTPError: If ValueMatrix access fails.
        """
        if not isinstance(request, ods.ValueMatrixRequestStruct):
            raise TypeError(f"valuematrix_read expects 'ods.ValueMatrixRequestStruct', got '{type(request).__name__}'")
        response = self.ods_post_request("valuematrix-read", request)
        return_value = ods.DataMatrices()
        return_value.ParseFromString(response.content)
        return return_value

    def model_read(self) -> ods.Model:
        """
        Read the model from server and update cached version.

        Returns:
            The application model of the server.

        Raises:
            requests.HTTPError: If model read fails.
        """
        response = self.ods_post_request("model-read")
        model = ods.Model()
        model.ParseFromString(response.content)
        self.__mc = ModelCache(model)
        return model

    def model_update(self, model_parts: ods.Model, update_model: bool = True) -> None:
        """
        Update application model content. This method is used to modify existing items or
        create new ones.

        Args:
            model_parts: Parts of the model to be updated or created.
            update_model: Whether the model cache should be updated by reading the whole
                model again. Defaults to True.

        Raises:
            requests.HTTPError: If model update fails.
        """
        if not isinstance(model_parts, ods.Model):
            raise TypeError(f"model_update expects 'ods.Model', got '{type(model_parts).__name__}'")
        self.ods_post_request("model-update", model_parts)
        if update_model:
            # cache again if successfully changed
            self.model_read()

    def model_delete(self, model_parts: ods.Model, update_model: bool = True) -> None:
        """
        Delete application model content.

        Args:
            model_parts: Define model parts to be deleted.
            update_model: Whether the model cache should be updated by reading the whole
                model again. Defaults to True.

        Raises:
            requests.HTTPError: If model delete fails.
        """
        if not isinstance(model_parts, ods.Model):
            raise TypeError(f"model_delete expects 'ods.Model', got '{type(model_parts).__name__}'")
        self.ods_post_request("model-delete", model_parts)
        if update_model:
            # cache again if successfully changed
            self.model_read()

    def model_check(self) -> None:
        """
        Check if stored application model is consistent.

        Raises:
            requests.HTTPError: If model contains errors.
        """
        self.ods_post_request("model-check")

    def basemodel_read(self) -> ods.BaseModel:
        """
        Read the ODS base model version used by the server.

        Returns:
            The server base model.

        Raises:
            requests.HTTPError: If reading base model fails.
        """
        response = self.ods_post_request("basemodel-read")
        base_model = ods.BaseModel()
        base_model.ParseFromString(response.content)
        return base_model

    def asampath_create(self, instance: ods.Instance) -> ods.AsamPath:
        """
        Create a persistent string representing the instance.

        Args:
            instance: Instance to get AsamPath for.

        Returns:
            The AsamPath that represents the instance.

        Raises:
            requests.HTTPError: If creation fails.
        """
        if not isinstance(instance, ods.Instance):
            raise TypeError(f"asampath_create expects 'ods.Instance', got '{type(instance).__name__}'")
        response = self.ods_post_request("asampath-create", instance)
        return_value = ods.AsamPath()
        return_value.ParseFromString(response.content)
        return return_value

    def asampath_resolve(self, asam_path: ods.AsamPath) -> ods.Instance:
        """
        Use the persistent string to get back the instance.

        Args:
            asam_path: AsamPath to be resolved.

        Returns:
            Instance represented by AsamPath.

        Raises:
            requests.HTTPError: If path could not be resolved.
        """
        if not isinstance(asam_path, ods.AsamPath):
            raise TypeError(f"asampath_resolve expects 'ods.AsamPath', got '{type(asam_path).__name__}'")
        response = self.ods_post_request("asampath-resolve", asam_path)
        return_value = ods.Instance()
        return_value.ParseFromString(response.content)
        return return_value

    def context_read(self, pattern_or_filter: ods.ContextVariablesFilter | str = "*") -> ods.ContextVariables:
        """
        Read the session context variables.

        Args:
            pattern_or_filter: Context variable filter as str or ContextVariablesFilter.
                Defaults to "*" to return all variables.

        Returns:
            ContextVariables where the name matches the filter.

        Raises:
            requests.HTTPError: If something went wrong.
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

    def context_update(self, context_variables: ods.ContextVariables) -> None:
        """
        Set context variables for current session. This will set context variables for the given session.
        If new session is created they will fall back to their default.

        Args:
            context_variables: ContextVariables to be set or updated.

        Raises:
            requests.HTTPError: If something went wrong.
        """
        if not isinstance(context_variables, ods.ContextVariables):
            raise TypeError(f"context_update expects 'ods.ContextVariables', got '{type(context_variables).__name__}'")
        self.ods_post_request("context-update", context_variables)

    def password_update(self, password_update: ods.PasswordUpdate) -> None:
        """
        Update the password of the defined user.

        Args:
            password_update: Defines for which user the password should be updated.

        Raises:
            requests.HTTPError: If something went wrong.
        """
        if not isinstance(password_update, ods.PasswordUpdate):
            raise TypeError(f"password_update expects 'ods.PasswordUpdate', got '{type(password_update).__name__}'")
        self.ods_post_request("password-update", password_update)

    def file_access(self, file_identifier: ods.FileIdentifier) -> str:
        """
        Get file access URL for file content.

        Args:
            file_identifier: Define content to be accessed.
                Might be an AoFile or a DT_BLOB attribute.

        Returns:
            The server file URL.

        Raises:
            requests.HTTPError: If something went wrong.
            ValueError: If no file location provided by server.
        """
        if not isinstance(file_identifier, ods.FileIdentifier):
            raise TypeError(f"file_access expects 'ods.FileIdentifier', got '{type(file_identifier).__name__}'")
        response = self.ods_post_request("file-access", file_identifier)
        server_file_url = response.headers.get("location")
        if server_file_url is None:
            raise ValueError("No file location provided by server!")
        return server_file_url

    def file_access_download(
        self,
        file_identifier: ods.FileIdentifier,
        target_file_or_folder: str,
        overwrite_existing: bool = False,
        default_filename: str = "download.bin",
        chunk_size: int = 8192,
    ) -> str:
        """
        Read file content from server.

        Args:
            file_identifier: Define content to be read. Might be an AoFile or a DT_BLOB attribute.
            target_file_or_folder: Path to save the file content to. If pointing to an existing
                folder, original filename will be used. Full path is returned.
            overwrite_existing: Whether existing files should be overwritten. Defaults to False.
            default_filename: Default filename if no filename is provided by server.
                Defaults to "download.bin".
            chunk_size: Size of chunks in bytes to stream. Defaults to 8192 (8KB).

        Returns:
            File path of saved file.

        Raises:
            requests.HTTPError: If something went wrong.
            FileExistsError: If file already exists and 'overwrite_existing' is False.
            ValueError: If no open session.
        """
        if not isinstance(file_identifier, ods.FileIdentifier):
            raise TypeError(
                f"file_access_download expects 'ods.FileIdentifier', got '{type(file_identifier).__name__}'"
            )
        server_file_url = self.file_access(file_identifier)

        if self.__session is None:
            raise ValueError("No open session!")
        file_response = self.__session.get(
            server_file_url,
            headers={
                "Accept": "application/octet-stream, application/x-asamods+protobuf, */*",
            },
            timeout=self.__request_timeout,
            allow_redirects=self.__allow_redirects,
            stream=True,
        )
        self.check_requests_response(file_response)

        target_file_path = target_file_or_folder
        if os.path.isdir(target_file_path):
            content_disposition = file_response.headers.get(
                "Content-Disposition", f'attachment; filename="{default_filename}"'
            )
            filename = (
                content_disposition.split("filename=")[1].strip('"')
                if "filename=" in content_disposition
                else default_filename
            )
            target_file_path = os.path.join(target_file_path, filename)

        if not overwrite_existing and os.path.exists(target_file_path):
            raise FileExistsError(f"File '{target_file_path}' already exists and 'overwrite_existing' is False.")

        with open(target_file_path, "wb") as file:
            for chunk in file_response.iter_content(chunk_size=chunk_size):
                if chunk:  # filter out keep-alive new chunks
                    file.write(chunk)

        return target_file_path

    def file_access_upload(
        self,
        file_identifier: ods.FileIdentifier,
        source_file_path: str,
    ) -> None:
        """
        Upload file content to server.

        Args:
            file_identifier: Define content to be written.
                Might be an AoFile or a DT_BLOB attribute.
            source_file_path: Path to the file to be uploaded.

        Raises:
            requests.HTTPError: If something went wrong.
            FileNotFoundError: If source file was not found.
            ValueError: If no open session.
        """
        if not isinstance(file_identifier, ods.FileIdentifier):
            raise TypeError(f"file_access_upload expects 'ods.FileIdentifier', got '{type(file_identifier).__name__}'")
        if not os.path.isfile(source_file_path):
            raise FileNotFoundError(f"File '{source_file_path}' not found.")

        server_file_url = self.file_access(file_identifier)

        with open(source_file_path, "rb") as file:
            if self.__session is None:
                raise ValueError("No open session!")
            put_response = self.__session.put(
                server_file_url,
                data=file,
                headers={
                    "Content-Type": "application/octet-stream",
                    "Accept": "application/x-asamods+protobuf",
                },
                timeout=self.__request_timeout,
                allow_redirects=self.__allow_redirects,
            )
            self.check_requests_response(put_response)

    def file_access_delete(
        self,
        file_identifier: ods.FileIdentifier,
    ) -> None:
        """
        Delete file content from server.

        Args:
            file_identifier: Define content to be deleted.
                Might be an AoFile or a DT_BLOB attribute.

        Raises:
            requests.HTTPError: If something went wrong.
            ValueError: If no open session.
        """
        if not isinstance(file_identifier, ods.FileIdentifier):
            raise TypeError(f"file_access_delete expects 'ods.FileIdentifier', got '{type(file_identifier).__name__}'")
        server_file_url = self.file_access(file_identifier)

        if self.__session is None:
            raise ValueError("No open session!")
        delete_response = self.__session.delete(
            server_file_url,
            headers={"Accept": "application/x-asamods+protobuf"},
            timeout=self.__request_timeout,
            allow_redirects=self.__allow_redirects,
        )
        self.check_requests_response(delete_response)

    def ods_post_request(
        self,
        relative_url_part: str,
        message: Message | None = None,
        timeout: float | None = None,
        headers: dict[str, str] | None = None,
    ) -> requests.Response:
        """
        Do ODS post call with the given relative URL.

        Args:
            relative_url_part: URL part that is joined to conI URL using `/`.
            message: Protobuf message to be sent. Defaults to None.
            timeout: Maximal time to wait for response.
                If None, uses the request_timeout from constructor.
            headers: Custom HTTP headers. If None, uses default protobuf headers.

        Returns:
            Requests response if successful.

        Raises:
            requests.HTTPError: If status code is not 200 or 201.
        """

        if self.__session is None or self.__con_i is None:
            raise ValueError("No open session!")

        response = self.__session.post(
            self.__con_i + "/" + relative_url_part,
            data=message.SerializeToString() if message is not None else None,
            timeout=timeout if timeout is not None else self.__request_timeout,
            headers=(headers if headers is not None else self.__default_http_headers),
            allow_redirects=self.__allow_redirects,
        )
        self.check_requests_response(response)
        return response

    @staticmethod
    def check_requests_response(response: requests.Response) -> None:
        if response.status_code not in (200, 201):
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

    @property
    def mc(self) -> ModelCache:
        """
        Get the model cache for the current session.

        Returns:
            ModelCache object containing the cached application model.
        """
        if self.__mc is None:
            if self.__con_i is None:
                raise ValueError("ConI already closed!")
            raise ValueError("Model not read! Call model_read() first.")
        return self.__mc

    @property
    def security(self) -> Security:
        """
        Get the security information for the current session.

        Returns:
            Security object containing permissions and roles.

        Raises:
            requests.HTTPError: If security info retrieval fails.
        """
        if self.__session is None:
            raise ValueError("No open session!")

        if self.__security is None:
            self.__security = Security(self)
        return self.__security

    @property
    def bulk(self) -> BulkReader:
        """
        Get the bulk reader for the current session.

        Example::

            from odsbox.con_i import ConI

            with ConI(
                url="https://MYSERVER/api",
                auth=("USER", "PASSWORD"),
            ) as con_i:
                submatrix_id = 1234
                df = con_i.bulk.data_read(submatrix_id, ["Time", "Co*"])

        Returns:
            BulkReader object for reading data in bulk.
        """
        if self.__session is None:
            raise ValueError("No open session!")

        if self.__bulk_reader is None:
            self.__bulk_reader = BulkReader(self)
        return self.__bulk_reader
