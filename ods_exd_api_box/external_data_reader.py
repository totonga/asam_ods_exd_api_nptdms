"""EXD API implementation"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
import logging
from pathlib import Path
import threading
import time
from urllib.parse import urlparse, unquote
from urllib.request import url2pathname

import grpc

from . import exd_api, exd_grpc, ExdFileInterface, FileHandlerRegistry


# pylint: disable=invalid-name


@dataclass
class FileMapEntry:
    """Entry in the file map."""
    file: ExdFileInterface
    ref_count: int = 0
    last_access_time: float = field(default_factory=time.time)


class ExternalDataReader(exd_grpc.ExternalDataReader):
    """ASAM ODS EXD API implementation."""

    log = logging.getLogger(__name__)

    def Open(self, request: exd_api.Identifier, context: grpc.ServicerContext) -> exd_api.Handle:
        """Open a connection to an external data file."""
        self.log.info("Open request for URL '%s'", request.url)

        file_path = Path(self.__get_path(request.url))
        if not file_path.is_file():
            self.log.error(
                "File not found: '%s' (resolved path: '%s')", request.url, file_path)
            context.abort(grpc.StatusCode.NOT_FOUND,
                          f"File '{request.url}' not found.")

        connection_id = self.__open_file(request)
        self.log.info(
            "Successfully opened file '%s' with connection ID '%s'", request.url, connection_id)

        return exd_api.Handle(uuid=connection_id)

    def Close(self,
              request: exd_api.Handle,
              context: grpc.ServicerContext) -> exd_api.Empty:  # pylint: disable=unused-argument
        """Close the connection to an external data file."""
        self.log.info("Close request for handle '%s'", request.uuid)

        self.__close_file(request)
        self.log.info("Successfully closed connection '%s'", request.uuid)
        return exd_api.Empty()

    def GetStructure(
            self,
            request: exd_api.StructureRequest,
            context: grpc.ServicerContext) -> exd_api.StructureResult:
        """Get the structure of the external data file."""
        self.log.debug("GetStructure request for handle '%s'",
                       request.handle.uuid)

        if request.suppress_channels or request.suppress_attributes or 0 != len(request.channel_names):
            self.log.error("GetStructure: Unsupported options "
                           "(suppress_channels=%s, suppress_attributes=%s, channel_names=%s)",
                           request.suppress_channels, request.suppress_attributes, request.channel_names)
            context.abort(grpc.StatusCode.UNIMPLEMENTED,
                          'Method not implemented!')

        file, identifier = self.__get_file(request.handle)
        self.log.debug("Retrieved file handler for handle '%s'",
                       request.handle.uuid)

        rv = exd_api.StructureResult(identifier=identifier)
        rv.name = Path(identifier.url).name
        self.log.debug("Filling structure for file '%s'", rv.name)

        file.fill_structure(rv)
        self.log.debug(
            "Structure filled successfully for handle '%s'", request.handle.uuid)

        return rv

    def GetValues(self, request: exd_api.ValuesRequest, context: grpc.ServicerContext) -> exd_api.ValuesResult:
        """Get values from the external data file."""
        self.log.debug("GetValues request for handle '%s', channels: %s",
                       request.handle.uuid, len(request.channel_ids))

        file, _ = self.__get_file(request.handle)
        if file is None:
            self.log.error("File not found for handle '%s'",
                           request.handle.uuid)
            context.abort(grpc.StatusCode.NOT_FOUND,
                          f"File for handle '{request.handle.uuid}' not found.")

        self.log.debug("Retrieving values for handle '%s'",
                       request.handle.uuid)
        result = file.get_values(request)
        self.log.debug(
            "Successfully retrieved values for handle '%s'", request.handle.uuid)
        return result

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
        host = f"{os.path.sep}{os.path.sep}{parsed.netloc}{os.path.sep}"
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
                self.log.debug("File handler created and added to file_map")
            else:
                self.log.debug(
                    "File '%s' already in file_map, reusing existing handler", connection_url)
            self.file_map[connection_url].ref_count += 1
            self.log.debug("Incremented ref_count for '%s' to %s", connection_url,
                           self.file_map[connection_url].ref_count)
            return connection_id

    def __get_file(self, handle: exd_api.Handle) -> tuple[ExdFileInterface, exd_api.Identifier]:
        identifier = self.connection_map.get(handle.uuid)
        if identifier is None:
            self.log.error(
                "Handle '%s' not found in connection_map", handle.uuid)
            raise KeyError(f"Handle '{handle.uuid}' not found.")
        connection_url = self.__get_path(identifier.url)
        entry = self.file_map.get(connection_url)
        if entry is None:
            self.log.error("Connection URL '%s' not found in file_map for handle '%s'",
                           connection_url, handle.uuid)
            raise KeyError(
                f"Connection URL '{connection_url}' not found.")
        entry.last_access_time = time.time()
        self.log.debug("Updated last_access_time for handle '%s' (file: '%s')",
                       handle.uuid, connection_url)
        return entry.file, identifier

    def __close_file(self, handle: exd_api.Handle) -> None:
        with self.lock:
            identifier = self.connection_map.get(handle.uuid)
            if identifier is None:
                self.log.error(
                    "Handle '%s' not found in connection_map for close", handle.uuid)
                raise KeyError(f"Handle '{handle.uuid}' not found for close.")
            connection_url = self.__get_path(identifier.url)
            self.connection_map.pop(handle.uuid)
            self.log.debug(
                "Removed handle '%s' from connection_map", handle.uuid)
            entry = self.file_map.get(connection_url)
            if entry is None:
                self.log.error(
                    "Connection URL '%s' not found in file_map for close", connection_url)
                raise KeyError(
                    f"Connection URL '{connection_url}' not found for close.")
            if entry.ref_count > 1:
                entry.ref_count -= 1
                self.log.debug("Decremented ref_count for '%s' to %s",
                               connection_url, entry.ref_count)
            else:
                self.log.info(
                    "Closing file '%s' (ref_count reached 0)", connection_url)
                entry.file.close()
                self.file_map.pop(connection_url)
                self.log.debug(
                    "File removed from file_map: '%s'", connection_url)
