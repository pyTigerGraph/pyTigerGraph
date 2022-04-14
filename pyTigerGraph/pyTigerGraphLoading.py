"""Loading Job Functions."""

from pyTigerGraph.pyTigerGraphBase import pyTigerGraphBase


class pyTigerGraphLoading(pyTigerGraphBase):
    """Loading Job Functions."""

    def runLoadingJobWithFile(self, filePath: str, fileTag: str, jobName: str, sep: str = None,
            eol: str = None, timeout: int = 16000, sizeLimit: int = 128000000) -> dict:
        """Execute a loading job with the referenced file.

        The file will first be uploaded to the tigergraph server and the value of the appropriate
        FILENAME definition will be updated to point to the freshly uploaded file.

        Args:
            filePath:
                File variable name or file path for the file containing the data.
            fileTag:
                The name of file variable in the loading job (DEFINE FILENAME <fileTag>).
            jobName:
                The name of the loading job.
            sep:
                Data value separator. If your data is JSON, you do not need to specify this
                parameter. The default separator is a comma (,).
            eol:
                End-of-line character. Only one or two characters are allowed, except for the
                special case "\\r\\n". The default value is "\\n"
            timeout:
                Timeout in seconds. If set to 0, use the system-wide endpoint timeout setting.
            sizeLimit:
                Maximum size for input file in bytes.

        Endpoint:
            - `POST /ddl/{graph_name}`
                See https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_run_a_loading_job
        """
        try:
            data = open(filePath, 'rb').read()
            params = {
                "tag": jobName,
                "filename": fileTag,
            }
            if sep is not None:
                params["sep"] = sep
            if eol is not None:
                params["eol"] = eol
        except:
            return None
        return self._post(self.restppUrl + "/ddl/" + self.graphname, params=params, data=data,
            headers={"RESPONSE-LIMIT": str(sizeLimit), "GSQL-TIMEOUT": str(timeout)})

    def uploadFile(self, filePath, fileTag, jobName="", sep=None, eol=None, timeout=16000,
            sizeLimit=128000000) -> dict:
        """DEPRECATED

        Use `runLoadingJobWithFile()` instead.
        TODO Proper depreciation
        """
        self.runLoadingJobWithFile(filePath, fileTag, jobName, sep, eol, timeout, sizeLimit)

    # TODO POST /restpploader/{graph_name}
