"""helper for handling transactions"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .con_i import ConI


class Transaction:
    """
    Class helps to keep track of transactions.
    If no commit is called it will abort the transaction if with sections is left.
    """

    __con_i: ConI | None = None

    def __init__(self, con_i: ConI) -> None:
        con_i.transaction_create()
        self.__con_i = con_i

    def __del__(self) -> None:
        self.abort()

    def __enter__(self) -> Transaction:
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, exc_traceback: object
    ) -> None:
        self.abort()

    def commit(self) -> None:
        """
        Commit the transaction.
        """
        if self.__con_i is not None:
            self.__con_i.transaction_commit()
            self.__con_i = None

    def abort(self) -> None:
        """
        Aborts the transaction.
        """
        if self.__con_i is not None:
            self.__con_i.transaction_abort()
            self.__con_i = None
