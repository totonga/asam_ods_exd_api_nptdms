"""EXD API implementation"""
from dataclasses import dataclass
import logging
import os
from pathlib import Path
import threading
from urllib.parse import urlparse, unquote
from urllib.request import url2pathname

import grpc
import ods_external_data_pb2 as exd_api  # pylint: disable=import-error
import ods_external_data_pb2_grpc  # pylint: disable=import-error

from .external_data_file_interface import ExternalDataFileInterface  # pylint: disable=import-error
from .external_data_file_handler_registry import FileHandlerRegistry  # pylint: disable=import-error

# pylint: disable=invalid-name


@dataclass
class FileMapEntry:
    """Entry in the file map."""
    file: ExternalDataFileInterface
    ref_count: int = 0


class ExternalDataReader(ods_external_data_pb2_grpc.ExternalDataReader):
    """ASAM ODS EXD API implementation."""

    log = logging.getLogger(__name__)

    def Open(self, request: exd_api.Identifier, context: grpc.ServicerContext) -> exd_api.Handle:
        """Open a connection to an external data file."""

        file_path = Path(self.__get_path(request.url))
        if not file_path.is_file():
            context.abort(grpc.StatusCode.NOT_FOUND,
                          f"File '{request.url}' not found.")

        connection_id = self.__open_file(request)

        return exd_api.Handle(uuid=connection_id)

    def Close(self,
              request: exd_api.Handle,
              context: grpc.ServicerContext) -> exd_api.Empty:  # pylint: disable=unused-argument
        """Close the connection to an external data file."""

        self.__close_file(request)
        return exd_api.Empty()

    def GetStructure(
            self,
            request: exd_api.StructureRequest,
            context: grpc.ServicerContext) -> exd_api.StructureResult:
        """Get the structure of the external data file."""

        if request.suppress_channels or request.suppress_attributes or 0 != len(request.channel_names):
            context.abort(grpc.StatusCode.UNIMPLEMENTED,
                          'Method not implemented!')

        file, identifier = self.__get_file(request.handle)

        rv = exd_api.StructureResult(identifier=identifier)
        rv.name = Path(identifier.url).name

        file.fill_structure(rv)

        return rv

    def GetValues(self, request: exd_api.ValuesRequest, context: grpc.ServicerContext) -> exd_api.ValuesResult:
        """Get values from the external data file."""

        file, _ = self.__get_file(request.handle)
        if file is None:
            context.abort(grpc.StatusCode.NOT_FOUND,
                          f"File for handle '{request.handle.uuid}' not found.")

        return file.get_values(request)

    def GetValuesEx(self, request: exd_api.ValuesExRequest, context: grpc.ServicerContext) -> exd_api.ValuesExResult:
        """Get values from the external data file with extended options."""

        context.abort(grpc.StatusCode.UNIMPLEMENTED,
                      f'Method not implemented! request. Names: {request.channel_names}')

    def __init__(self) -> None:
        self.connect_count: int = 0
        self.connection_map: dict[str, exd_api.Identifier] = {}
        self.file_map: dict[str, FileMapEntry] = {}
        self.lock: threading.Lock = threading.Lock()

    def __get_id(self, identifier: exd_api.Identifier) -> str:
        self.connect_count += 1
        rv = str(self.connect_count)
        self.connection_map[rv] = identifier
        return rv

    def __uri_to_path(self, uri: str) -> str:
        parsed = urlparse(uri)
        host = "{0}{0}{mnt}{0}".format(os.path.sep, mnt=parsed.netloc)
        return os.path.normpath(
            os.path.join(host, url2pathname(unquote(parsed.path)))
        )

    def __get_path(self, file_url: str) -> str:
        return self.__uri_to_path(file_url)

    def __open_file(self, identifier: exd_api.Identifier) -> str:
        with self.lock:
            connection_id = self.__get_id(identifier)
            connection_url = self.__get_path(identifier.url)
            if connection_url not in self.file_map:
                self.log.info("Opening external data file '%s' as connection id '%s'.",
                              connection_url, connection_id)
                file_handle = FileHandlerRegistry.create_from_path(
                    connection_url, identifier.parameters)
                self.file_map[connection_url] = FileMapEntry(
                    file=file_handle, ref_count=0)
            self.file_map[connection_url].ref_count += 1
            return connection_id

    def __get_file(self, handle: exd_api.Handle) -> tuple[ExternalDataFileInterface, exd_api.Identifier]:
        identifier = self.connection_map.get(handle.uuid)
        if identifier is None:
            raise KeyError(f"Handle '{handle.uuid}' not found.")
        connection_url = self.__get_path(identifier.url)
        entry = self.file_map.get(connection_url)
        if entry is None:
            raise KeyError(
                f"Connection URL '{connection_url}' not found.")
        return entry.file, identifier

    def __close_file(self, handle: exd_api.Handle) -> None:
        with self.lock:
            identifier = self.connection_map.get(handle.uuid)
            if identifier is None:
                raise KeyError(f"Handle '{handle.uuid}' not found for close.")
            connection_url = self.__get_path(identifier.url)
            self.connection_map.pop(handle.uuid)
            entry = self.file_map.get(connection_url)
            if entry is None:
                raise KeyError(
                    f"Connection URL '{connection_url}' not found for close.")
            if entry.ref_count > 1:
                entry.ref_count -= 1
            else:
                entry.file.close()
                self.file_map.pop(connection_url)
