"""Helper for ASAM ODS HTTP API conI session"""

import logging
from typing import List

import requests
import requests.auth
from google.protobuf.json_format import MessageToJson
from google.protobuf.message import Message
from pandas import DataFrame

import odsbox.proto.ods_pb2 as _ods
from odsbox.datamatrices_to_pandas import to_pandas
from odsbox.jaquel import jaquel_to_ods
from odsbox.model_cache import ModelCache
from odsbox.transaction import Transaction


class ConI:
    """This is a helper to hold an ASAM ODS HTTP API ConI session"""

    __log: logging.Logger = logging.getLogger(__name__)
    __session: requests.Session = None
    __con_i: str = None
    mc: ModelCache = None

    def __init__(
        self,
        url: str = "http://localhost:8080/api",
        auth: requests.auth.AuthBase = ("sa", "sa"),
        context_variables: _ods.ContextVariables | dict | None = None,
        verify_certificate: bool = True,
    ):
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
        if isinstance(context_variables, _ods.ContextVariables):
            _context_variables = context_variables
        else:
            _context_variables = _ods.ContextVariables()
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

    def con_i_url(self):
        """Get the ASAM ODS session URL used to work with this session."""
        return self.__con_i

    def logout(self):
        """Close the attached session at the ODS server."""
        if None is not self.__session:
            response = self.__session.delete(
                self.__con_i,
            )
            self.__session.close()
            self.__session = None
            self.__con_i = None
            self.__check_result(response)

    def query_data(self, query: str | dict | _ods.SelectStatement) -> DataFrame:
        """Query ods server for content"""
        if isinstance(query, _ods.SelectStatement):
            return to_pandas(self.data_read(self.data_read_jaquel(query)))
        else:
            return to_pandas(self.data_read_jaquel(query))

    def model(self) -> _ods.Model:
        """Get the cache ODS server model."""
        return self.mc.model()

    def data_read_jaquel(self, jaquel: str | dict) -> _ods.DataMatrices:
        """Query ODS server using JAQueL syntax and return result as pandas DataFrame."""
        _ods_entity, ods_query = jaquel_to_ods(self.model(), jaquel)
        return self.data_read(ods_query)

    def data_read(self, data: _ods.SelectStatement) -> _ods.DataMatrices:
        """Query ASAM ODS server."""
        response = self.ods_post_request("data-read", data)
        return_value = _ods.DataMatrices()
        return_value.ParseFromString(response.content)
        return return_value

    def data_create(self, data: _ods.DataMatrices) -> List[int]:
        """Create new Instances."""
        response = self.ods_post_request("data-create", data)
        return_value = _ods.DataMatrices()
        return_value.ParseFromString(response.content)
        return return_value.matrices[0].columns[0].longlong_array.values

    def data_update(self, data: _ods.DataMatrices) -> None:
        """Update existing instances."""
        self.ods_post_request("data-update", data)

    def data_delete(self, data: _ods.DataMatrices) -> None:
        """Delete Instances."""
        self.ods_post_request("data-delete", data)

    def data_copy(self, data: _ods.CopyRequest) -> _ods.Instance:
        """Copy an Instance and its related children."""
        response = self.ods_post_request("data-copy", data)
        return_value = _ods.Instance()
        return_value.ParseFromString(response.content)
        return return_value

    def n_m_relation_read(self, data: _ods.NtoMRelationIdentifier) -> _ods.NtoMRelatedInstances:
        """Read n-m relations for a defined instance."""
        response = self.ods_post_request("n-m-relation-read", data)
        return_value = _ods.NtoMRelatedInstances()
        return_value.ParseFromString(response.content)
        return return_value

    def n_m_relation_write(self, data: _ods.NtoMWriteRelatedInstances) -> None:
        """Update, delete or create n-m relations for given instance pairs."""
        self.ods_post_request("n-m-relation-write", data)

    def transaction(self) -> Transaction:
        """Open a transaction object to be used in a with clause"""
        return Transaction(self)

    def transaction_create(self) -> None:
        """Open a transaction for writing."""
        self.ods_post_request("transaction-create")

    def transaction_commit(self) -> None:
        """Commit transaction created before."""
        self.ods_post_request("transaction-commit")

    def transaction_abort(self) -> None:
        """Abort transaction created before."""
        self.ods_post_request("transaction-abort")

    def valuematrix_read(self, data: _ods.ValueMatrixRequestStruct) -> _ods.DataMatrices:
        """Read bulk data from a submatrix or measurement.
        Submatrix access can also be done using data-read."""
        response = self.ods_post_request("valuematrix-read", data)
        return_value = _ods.DataMatrices()
        return_value.ParseFromString(response.content)
        return return_value

    def model_read(self) -> _ods.Model:
        """Read the model from server and update cached version."""
        response = self.ods_post_request("model-read")
        model = _ods.Model()
        model.ParseFromString(response.content)
        self.mc = ModelCache(model)
        return model

    def model_update(self, model_parts: _ods.Model, update_model: bool = True) -> None:
        """Update application model content."""
        self.ods_post_request("model-update", model_parts)
        if update_model:
            # cache again if successfully changed
            self.model_read()

    def model_delete(self, model_parts: _ods.Model, update_model: bool = True) -> None:
        """Delete application model content."""
        self.ods_post_request("model-delete", model_parts)
        if update_model:
            # cache again if successfully changed
            self.model_read()

    def model_check(self) -> None:
        """Check if stored application model is consistent."""
        self.ods_post_request("model-check")

    def basemodel_read(self) -> _ods.BaseModel:
        """Read the ODS base model version used by the server."""
        response = self.ods_post_request("basemodel-read")
        base_model = _ods.BaseModel()
        base_model.ParseFromString(response.content)
        return base_model

    def asampath_create(self, data: _ods.Instance) -> _ods.AsamPath:
        """Create an persistent string representing the instance."""
        response = self.ods_post_request("asampath-create", data)
        return_value = _ods.AsamPath()
        return_value.ParseFromString(response.content)
        return return_value

    def asampath_resolve(self, data: _ods.AsamPath) -> _ods.Instance:
        """Use the persistent string to get back the instance."""
        response = self.ods_post_request("asampath-resolve", data)
        return_value = _ods.Instance()
        return_value.ParseFromString(response.content)
        return return_value

    def context_read(self, pattern_or_filter: _ods.ContextVariablesFilter | str = "*") -> _ods.ContextVariables:
        """Read the session context variables."""
        context_variables_filter = (
            pattern_or_filter
            if isinstance(pattern_or_filter, _ods.ContextVariablesFilter)
            else _ods.ContextVariablesFilter(pattern=pattern_or_filter)
        )
        response = self.ods_post_request("context-read", context_variables_filter)
        return_value = _ods.ContextVariables()
        return_value.ParseFromString(response.content)
        return return_value

    def context_update(self, data: _ods.ContextVariables):
        """Set context variables for current session."""
        self.ods_post_request("context-update", data)

    def password_update(self, data: _ods.PasswordUpdate) -> None:
        """Update the password of the defined user."""
        self.ods_post_request("password-update", data)

    def ods_post_request(self, url: str, data: Message | None = None) -> requests.Response:
        """Do ODS post call with the given relative URL."""

        response = self.__session.post(
            self.__con_i + "/" + url,
            data=data.SerializeToString() if data is not None else None,
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
                error_info = _ods.ErrorInfo()
                error_info.ParseFromString(response.content)
                raise requests.HTTPError(
                    MessageToJson(error_info),
                    response=response,
                )
            response.raise_for_status()
