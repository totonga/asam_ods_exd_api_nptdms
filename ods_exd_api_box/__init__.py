"""
Initialization of the ods_exd_api_box package.
Loads the protobuf stubs and makes them available at package level.
"""

__version__ = "0.1.0"

from .proto import ods, exd_api, exd_grpc

from .file_interface import ExdFileInterface
from .file_handler_registry import FileHandlerRegistry
from .external_data_reader import ExternalDataReader
from .server import serve_plugin

__all__ = [
    'ods',
    'exd_api',
    'exd_grpc',
    'FileHandlerRegistry',
    'ExternalDataReader',
    'serve_plugin',
    'ExdFileInterface',
    '__version__',
]
