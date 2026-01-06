# Prepare python to use GRPC interface:
# python -m grpc_tools.protoc --proto_path=proto_src --pyi_out=. --python_out=. --grpc_python_out=. ods.proto ods_external_data.proto

from concurrent import futures
import logging
import multiprocessing

import grpc

import ods_external_data_pb2_grpc  # pylint: disable=import-error

from external_data_reader import ExternalDataReader  # pylint: disable=import-error


def serve():
    logging.info("Starting ASAM ODS EXD API gRPC server...")
    server = grpc.server(
        futures.ThreadPoolExecutor(
            max_workers=2 * multiprocessing.cpu_count()),
        options=[
            ("grpc.max_send_message_length", 512 * 1024 * 1024),
            ("grpc.max_receive_message_length", 32 * 1024 * 1024),
        ])
    ods_external_data_pb2_grpc.add_ExternalDataReaderServicer_to_server(
        ExternalDataReader(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    logging.info(
        "ASAM ODS EXD API gRPC server started, listening on port 50051.")
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    serve()
