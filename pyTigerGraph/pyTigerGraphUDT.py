"""User Defined Type (UDT) Functions."""

from pyTigerGraph.pyTigerGraphSchema import pyTigerGraphSchema


class pyTigerGraphUDT(pyTigerGraphSchema):
    """User Defined Type (UDT) Functions."""

    def getUDTs(self) -> list:
        """Returns the list of User Defined Types (names only).

        For information on UDTs see https://docs.tigergraph.com/dev/gsql-ref/ddl-and-loading/system-and-language-basics#typedef-tuple .

        Returns:
            The list of names of UDTs (defined in the global scope, i.e. not in queries).
        """
        ret = []
        for udt in self._getUDTs():
            ret.append(udt["name"])
        return ret

    def getUDT(self, udtName: str) -> list:
        """Returns the details of a specific User Defined Type (defined in the global scope).

        For information on UDTs see https://docs.tigergraph.com/dev/gsql-ref/ddl-and-loading/system-and-language-basics#typedef-tuple .

        Args:
            udtName:
                The name of the User Defined Type.

        Returns:
            The metadata (the details of the fields) of the UDT.

        """
        for udt in self._getUDTs():
            if udt["name"] == udtName:
                return udt["fields"]
        return []  # UDT was not found
        # TODO Should raise exception instead?
