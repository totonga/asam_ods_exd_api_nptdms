"""gRPC server for ASAM ODS EXD-API."""

from __future__ import annotations

import argparse
import logging
import multiprocessing
from concurrent import futures
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

from . import ExdFileInterface, ExternalDataReader, FileHandlerRegistry, exd_grpc


@dataclass(frozen=True)
class ServerConfig:
    bind_address: str
    port: int
    max_workers: int
    max_send_message_length: int
    max_receive_message_length: int
    max_concurrent_streams: int | None
    use_tls: bool
    tls_cert_file: Path | None
    tls_key_file: Path | None
    tls_client_ca_file: Path | None
    require_client_cert: bool
    health_check_enabled: bool
    health_check_bind_address: str
    health_check_port: int


def _get_server_config() -> ServerConfig:
    parser = argparse.ArgumentParser(description="ASAM ODS EXD-API gRPC Server")
    parser.add_argument("--bind-address", type=str, help="Address to bind gRPC server to", default="[::]")
    parser.add_argument("--port", type=int, help="Port to run gRPC server on", default=50051)
    parser.add_argument(
        "--max-workers",
        type=int,
        help="Maximum number of worker threads for the gRPC server",
        default=2 * multiprocessing.cpu_count(),
    )
    parser.add_argument(
        "--max-concurrent-streams", type=int, help="Maximum amount of concurrent gRPC streams", default=None
    )
    parser.add_argument(
        "--max-send-message-length", type=int, help="Maximum send message length in MBytes", default=512
    )
    parser.add_argument(
        "--max-receive-message-length", type=int, help="Maximum receive message length in MBytes", default=32
    )
    parser.add_argument(
        "--use-tls", action="store_true", help="Serve over TLS/SSL using the provided cert and key"
    )
    parser.add_argument("--tls-cert-file", type=Path, help="Path to the PEM encoded server certificate")
    parser.add_argument("--tls-key-file", type=Path, help="Path to the PEM encoded server private key")
    parser.add_argument(
        "--tls-client-ca-file", type=Path, help="CA bundle that is used to validate client certificates"
    )
    parser.add_argument(
        "--require-client-cert", action="store_true", help="Require a client certificate if TLS is enabled"
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging (DEBUG level)")
    parser.add_argument(
        "--health-check-enabled", action="store_true", help="Enable insecure health check service"
    )
    parser.add_argument(
        "--health-check-bind-address",
        type=str,
        help="Address to bind health check service to",
        default="[::]",
    )
    parser.add_argument(
        "--health-check-port", type=int, help="Port to run insecure health check service on", default=50052
    )

    args = parser.parse_args()

    if args.require_client_cert and not args.use_tls:
        parser.error("--require-client-cert requires --use-tls")
    if args.tls_client_ca_file and not args.use_tls:
        parser.error("--tls-client-ca-file requires --use-tls")
    if args.use_tls and (args.tls_cert_file is None or args.tls_key_file is None):
        parser.error("--use-tls requires both --tls-cert-file and --tls-key-file")

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    return ServerConfig(
        bind_address=args.bind_address,
        port=args.port,
        max_workers=args.max_workers,
        max_concurrent_streams=args.max_concurrent_streams,
        max_send_message_length=args.max_send_message_length,
        max_receive_message_length=args.max_receive_message_length,
        use_tls=args.use_tls,
        tls_cert_file=args.tls_cert_file,
        tls_key_file=args.tls_key_file,
        tls_client_ca_file=args.tls_client_ca_file,
        require_client_cert=args.require_client_cert,
        health_check_enabled=args.health_check_enabled,
        health_check_bind_address=args.health_check_bind_address,
        health_check_port=args.health_check_port,
    )


# helper utilities for building server options and credentials
def _build_server_options(config: ServerConfig) -> list[tuple[str, int]]:
    options: list[tuple[str, int]] = [
        ("grpc.max_send_message_length", config.max_send_message_length * 1024 * 1024),
        ("grpc.max_receive_message_length", config.max_receive_message_length * 1024 * 1024),
    ]
    if config.max_concurrent_streams is not None:
        options.append(("grpc.max_concurrent_streams", config.max_concurrent_streams))
    return options


def _create_tls_credentials(config: ServerConfig) -> grpc.ServerCredentials:
    cert_path = config.tls_cert_file
    key_path = config.tls_key_file
    if cert_path is None or key_path is None:
        raise ValueError("TLS credentials require a certificate and private key path")

    with cert_path.open("rb") as certificate_file, key_path.open("rb") as private_key_file:
        certificate_chain = certificate_file.read()
        private_key = private_key_file.read()

    root_certificates: bytes | None = None
    if config.tls_client_ca_file is not None:
        with config.tls_client_ca_file.open("rb") as ca_file:
            root_certificates = ca_file.read()

    return grpc.ssl_server_credentials(
        [(private_key, certificate_chain)],
        root_certificates=root_certificates,
        require_client_auth=config.require_client_cert,
    )


def _create_health_check_server(config: ServerConfig) -> grpc.Server | None:
    """Creates and starts a health check server.

    Returns:
        The health check server instance, or None if health check is disabled.
    """
    if not config.health_check_enabled:
        return None

    health_check_address = f"{config.health_check_bind_address}:{config.health_check_port}"
    health_check_server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=1),
        options=_build_server_options(config),
    )
    health_check_servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_check_servicer, health_check_server)

    # Mark the ExternalDataReader service as serving
    health_check_servicer.set(
        "asam.ods.ExternalDataReader",
        health_pb2.HealthCheckResponse.SERVING,  # pylint: disable=no-member
    )

    health_check_bound_port = health_check_server.add_insecure_port(health_check_address)
    if health_check_bound_port == 0:
        raise RuntimeError(f"Failed to bind health check port at {health_check_address}")

    health_check_server.start()
    logging.info(
        "Health check service started (insecure), listening on %s.",
        health_check_address,
    )

    return health_check_server


def serve(server_config: ServerConfig | None = None):
    """Starts the gRPC server and listens for incoming requests."""

    config = server_config if server_config is not None else _get_server_config()
    address = f"{config.bind_address}:{config.port}"
    logging.info(
        "Starting ASAM ODS EXD API gRPC server at %s with max workers %s...",
        address,
        config.max_workers,
    )

    # Main service server
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=config.max_workers),
        options=_build_server_options(config),
    )
    exd_grpc.add_ExternalDataReaderServicer_to_server(ExternalDataReader(), server)

    if config.use_tls:
        bound_port = server.add_secure_port(address, _create_tls_credentials(config))
        protocol_note = "TLS"
    else:
        bound_port = server.add_insecure_port(address)
        protocol_note = "insecure"

    if bound_port == 0:
        raise RuntimeError(f"Failed to bind gRPC port at {address}")

    health_check_server = _create_health_check_server(config)

    server.start()
    logging.info(
        "ASAM ODS EXD API gRPC server started (%s), listening on %s.",
        protocol_note,
        address,
    )

    try:
        server.wait_for_termination()
    finally:
        if health_check_server is not None:
            health_check_server.stop(0)
        logging.info("Servers stopped.")


def serve_plugin(
    file_type_name: str,
    file_type_factory: Callable[[str, str], ExdFileInterface],
    file_type_file_patterns: list[str] | None = None,
):
    """Starts the gRPC server for a specific external data file type plugin.

    Args:
        file_type_name: File type identifier (e.g., 'tdms')
        file_patterns: List of file extension patterns (e.g., ['*.tdms'])
        factory: Callable that creates ExternalDataFileInterface instances
    """
    config = _get_server_config()

    logging.info(
        "Registering plugin for file type '%s' with patterns %s", file_type_name, file_type_file_patterns
    )
    FileHandlerRegistry.register(
        file_type_name=file_type_name, file_patterns=file_type_file_patterns, factory=file_type_factory
    )
    logging.info("Starting gRPC server for plugin '%s'", file_type_name)
    serve(server_config=config)
