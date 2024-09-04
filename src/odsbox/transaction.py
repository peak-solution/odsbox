"""helper for handling transactions"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .con_i import ConI


class Transaction:
    """
    Class helps to keep track of transactions.
    If no commit is called it will abort the transaction if with sections is left.
    """

    __con_i: "ConI" = None

    def __init__(self, con_i: "ConI"):
        self.__con_i = None
        con_i.transaction_create()
        self.__con_i = con_i

    def __del__(self):
        self.abort()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.abort()

    def commit(self) -> None:
        """
        Commit the transaction.
        """
        if None is not self.__con_i:
            self.__con_i.transaction_commit()
            self.__con_i = None

    def abort(self) -> None:
        """
        Aborts the transaction.
        """
        if None is not self.__con_i:
            self.__con_i.transaction_abort()
            self.__con_i = None
