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
from typing import List, Tuple

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
from odsbox.security import Security


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
    __session: requests.Session | None
    __con_i: str | None
    __default_http_headers: dict[str, str] = {
        "Content-Type": "application/x-asamods+protobuf",
        "Accept": "application/x-asamods+protobuf",
    }
    __security: Security | None = None
    __mc: ModelCache | None = None

    def __init__(
        self,
        url: str = "http://localhost:8080/api",
        auth: requests.auth.AuthBase | Tuple[str, str] = ("sa", "sa"),
        context_variables: ods.ContextVariables | dict | None = None,
        verify_certificate: bool = True,
        load_model: bool = True,
    ):
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


        :param str url: base url of the ASAM ODS API of a given server. An example is "http://localhost:8080/api".
        :param requests.auth.AuthBase auth: An auth object to be used for the used requests package.
            For basic auth `("USER", "PASSWORD")` can be used.
        :param ods.ContextVariables | dict | None context_variables: If context variables are necessary for the
            connection they are passed here. It defaults to None.
        :param bool verify_certificate: If no certificate is provided for https insecure access can be enabled.
            It defaults to True.
        :param bool load_model: If the model should be read after connection is established. It defaults to True.
        :raises requests.HTTPError: If connection to ASAM ODS server fails.
        """
        self.__session = None
        self.__con_i = None

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
            timeout=60.0,
            headers=self.__default_http_headers,
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

    def __del__(self):
        if self.__session is not None:
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
        if self.__con_i is None:
            raise ValueError("ConI already closed")
        return self.__con_i

    def logout(self):
        """
        Close the attached session at the ODS server by calling delete on the session URL
        and closing the requests session.

        :raises requests.HTTPError: If delete the ASAM ODS session fails.
        """
        if self.__session is not None:
            if self.__con_i is None:
                raise ValueError("ConI already closed")
            response = self.__session.delete(
                self.__con_i, timeout=60.0, headers={"Accept": "application/x-asamods+protobuf"}
            )
            self.__session.close()
            self.__session = None
            self.__con_i = None
            self.__security = None
            self.__mc = None
            self.check_requests_response(response)

    def query_data(
        self,
        query: str | dict | ods.SelectStatement,
        **kwargs,
    ) -> DataFrame:
        """
        Query ods server for content and return the results as Pandas DataFrame

        :param str | dict | ods.SelectStatement query: Query given as JAQueL query (dict or str)
            or as an ASAM ODS SelectStatement.
        :param kwargs: additional arguments passed to `to_pandas`.
        :raises requests.HTTPError: If query fails.
        :return DataFrame: The DataMatrices as Pandas.DataFrame. The columns are named as `ENTITY_NAME.ATTRIBUTE_NAME`.
            `IsNull` values are not marked invalid.
        """
        data_matrices = (
            self.data_read(query) if isinstance(query, ods.SelectStatement) else self.data_read_jaquel(query)
        )
        return to_pandas(data_matrices, model_cache=self.mc, **kwargs)

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
        _, ods_query = jaquel_to_ods(self.model(), jaquel)
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
        return list(return_value.matrices[0].columns[0].longlong_array.values)

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

        Example::

            with con_i.transaction() as transaction:
                # do writing
                transaction.commit()

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
        self.__mc = ModelCache(model)
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

    def file_access(self, file_identifier: ods.FileIdentifier) -> str:
        """
        Get file access URL for file content.

        :param ods.FileIdentifier file_identifier: Define content to be accessed.
                                                   Might be an AoFile or a DT_BLOB attribute.
        :raises requests.HTTPError: If something went wrong.
        :raises ValueError: If no file location provided by server.
        :return str: The server file URL.
        """
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
    ) -> str:
        """
        Read file content from server.

        :param ods.FileIdentifier file_identifier: Define content to be read. Might be an AoFile or a DT_BLOB attribute.
        :param str target_file_or_folder: Path to save the file content to. If pointing to an existing folder. Original
                                          filename will be used. Full path is returned.
        :param bool overwrite_existing: If existing files should be overwritten. It defaults to False.
        :param str default_filename: Default filename if no filename is provided by server.
                                     It defaults to "download.bin".
        :raises requests.HTTPError: If something went wrong.
        :raises FileExistsError: If file already exists and 'overwrite_existing' is False.
        :raises ValueError: If no open session.
        :return str: file path of saved file.
        """
        server_file_url = self.file_access(file_identifier)

        if self.__session is None:
            raise ValueError("No open session!")
        file_response = self.__session.get(
            server_file_url,
            headers={
                "Accept": "application/octet-stream, application/x-asamods+protobuf, */*",
            },
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
            file.write(file_response.content)

        return target_file_path

    def file_access_upload(
        self,
        file_identifier: ods.FileIdentifier,
        source_file_path: str,
    ) -> None:
        """
        Upload file content to server.

        :param ods.FileIdentifier file_identifier: Define content to be written.
                                                   Might be an AoFile or a DT_BLOB attribute.
        :param str source_file_path: Path to the file to be uploaded.
        :raises requests.HTTPError: If something went wrong.
        :raises FileNotFoundError: If source file was not found.
        :raises ValueError: If no open session.
        """
        if not os.path.isfile(source_file_path):
            raise FileNotFoundError(f"File '{source_file_path}' not found.")

        server_file_url = self.file_access(file_identifier)

        with open(source_file_path, "rb") as file:
            if self.__session is None:
                raise ValueError("No open session!")
            put_response = self.__session.put(
                server_file_url,
                data=file,
                headers={"Content-Type": "application/octet-stream", "Accept": "application/x-asamods+protobuf"},
            )
            self.check_requests_response(put_response)

    def file_access_delete(
        self,
        file_identifier: ods.FileIdentifier,
    ) -> None:
        """
        Delete file content from server.

        :param ods.FileIdentifier file_identifier: Define content to be deleted.
                                                   Might be an AoFile or a DT_BLOB attribute.
        :raises requests.HTTPError: If something went wrong.
        :raises ValueError: If no open session.
        """
        server_file_url = self.file_access(file_identifier)

        if self.__session is None:
            raise ValueError("No open session!")
        delete_response = self.__session.delete(server_file_url, headers={"Accept": "application/x-asamods+protobuf"})
        self.check_requests_response(delete_response)

    def ods_post_request(
        self,
        relative_url_part: str,
        message: Message | None = None,
        timeout: float = 600.0,
        headers: dict[str, str] | None = None,
    ) -> requests.Response:
        """
        Do ODS post call with the given relative URL.

        :param str relative_url_part: url part that is joined to conI URL using `/`.
        :param Message | None message: protobuf message to be send, defaults to None.
        :param float timeout: maximal time to wait for response.
        :raises requests.HTTPError: If status code is not 200 or 201.
        :return requests.Response: requests response if successful.
        """

        if self.__session is None or self.__con_i is None:
            raise ValueError("No open session!")

        response = self.__session.post(
            self.__con_i + "/" + relative_url_part,
            data=message.SerializeToString() if message is not None else None,
            timeout=timeout,
            headers=(headers if headers is not None else self.__default_http_headers),
        )
        self.check_requests_response(response)
        return response

    @staticmethod
    def check_requests_response(response: requests.Response):
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

    @property
    def mc(self) -> ModelCache:
        """
        Get the model cache for the current session.

        :return ods.ModelCache: ModelCache object containing the cached application model.
        """
        if self.__mc is None:
            raise ValueError("Model not read! Call model_read() first.")
        return self.__mc

    @property
    def security(self) -> Security:
        """
        Get the security information for the current session.

        :raises requests.HTTPError: If security info retrieval fails.
        :return ods.Security: Security object containing permissions and roles.
        """
        if self.__session is None:
            raise ValueError("No open session!")

        if self.__security is None:
            self.__security = Security(self)
        return self.__security
