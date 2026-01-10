"""gRPC server for ASAM ODS EXD-API."""
from __future__ import annotations

import argparse
from concurrent import futures
import logging
import multiprocessing
from typing import Callable

import grpc

from . import exd_grpc  # pylint: disable=import-error
from .external_data_reader import ExternalDataReader  # pylint: disable=import-error
from .file_handler_registry import FileHandlerRegistry  # pylint: disable=import-error
from .file_interface import ExternalDataFileInterface  # pylint: disable=import-error


def _get_server_config():
    parser = argparse.ArgumentParser(
        description="ASAM ODS EXD-API gRPC Server")
    parser.add_argument("--port", type=int,
                        help="Port to run gRPC server on", default=50051)
    parser.add_argument("--max-workers", type=int,
                        help="Maximum number of worker threads for the gRPC server",
                        default=2 * multiprocessing.cpu_count())
    parser.add_argument("--max-send-message-length", type=int,
                        help="Maximum send message length in MBytes", default=512)
    parser.add_argument("--max-receive-message-length", type=int,
                        help="Maximum receive message length in MBytes", default=32)
    parser.add_argument("--verbose", action="store_true",
                        help="Enable verbose logging (DEBUG level)")

    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    return args.port, args.max_workers, args.max_send_message_length, args.max_receive_message_length


def serve():
    """Starts the gRPC server and listens for incoming requests."""

    port, max_workers, max_send_message_length, max_receive_message_length = _get_server_config()

    logging.info(
        "Starting ASAM ODS EXD API gRPC server at port %s with max workers %s...", port, max_workers)
    server = grpc.server(
        futures.ThreadPoolExecutor(
            max_workers=max_workers),
        options=[
            ("grpc.max_send_message_length", max_send_message_length * 1024 * 1024),
            ("grpc.max_receive_message_length",
             max_receive_message_length * 1024 * 1024),
        ])
    exd_grpc.add_ExternalDataReaderServicer_to_server(
        ExternalDataReader(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logging.info(
        "ASAM ODS EXD API gRPC server started, listening on port %s.", port)
    server.wait_for_termination()


def serve_plugin(
    file_type_name: str,
    file_type_factory: Callable[[str, str], ExternalDataFileInterface],
    file_type_file_patterns: list[str] = None,
):
    """Starts the gRPC server for a specific external data file type plugin.

    Args:
        file_type_name: File type identifier (e.g., 'tdms')
        file_patterns: List of file extension patterns (e.g., ['*.tdms'])
        factory: Callable that creates ExternalDataFileInterface instances
    """
    FileHandlerRegistry.register(
        file_type_name=file_type_name, file_patterns=file_type_file_patterns, factory=file_type_factory)
    serve()
