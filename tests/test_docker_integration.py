import logging
import pathlib
import socket
import subprocess
import time
import unittest

import grpc
from google.protobuf.json_format import MessageToJson
from grpc_health.v1 import health_pb2, health_pb2_grpc

from ods_exd_api_box import exd_api, exd_grpc, ods


class TestDockerContainer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # build Docker-Image
        result = subprocess.run(
            ["docker", "build", "-t", "asam-ods-exd-api-nptdms", "."],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Docker build failed: {result.stderr}")

        # Clean up any existing test container from previous runs
        subprocess.run(
            ["docker", "stop", "test_container"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        subprocess.run(
            ["docker", "rm", "test_container"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )

        example_file_path = pathlib.Path.joinpath(
            pathlib.Path(__file__).parent.resolve(), "..", "data")
        data_folder = pathlib.Path(example_file_path).absolute().resolve()
        cp = subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--rm",
                "--name",
                "test_container",
                "-p",
                "50051:50051",
                "-p",
                "50052:50052",
                "-v",
                f"{data_folder}:/data",
                "asam-ods-exd-api-nptdms",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        if cp.returncode != 0:
            raise RuntimeError(f"Docker run failed: {cp.stderr}")
        cls.container_id = cp.stdout.strip()
        cls.__wait_for_port_ready(port=50051)

    @classmethod
    def tearDownClass(cls):
        # stop container
        subprocess.run(
            ["docker", "stop", "test_container"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )

    @classmethod
    def __wait_for_port_ready(cls, host="localhost", port=50051, timeout=30):
        start_time = time.time()
        while True:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex((host, port))
            sock.close()
            if result == 0:
                channel = grpc.insecure_channel(f"{host}:{port}")
                grpc.channel_ready_future(channel).result(timeout=5)
                return
            if time.time() - start_time > timeout:
                raise TimeoutError("Port did not become ready in time.")
            time.sleep(0.5)

    def test_container_health(self):
        channel = grpc.insecure_channel("localhost:50051")
        stub = exd_grpc.ExternalDataReaderStub(channel)
        self.assertIsNotNone(stub)
        grpc.channel_ready_future(channel).result(timeout=5)

    def test_structure(self):
        with grpc.insecure_channel("localhost:50051") as channel:
            service = exd_grpc.ExternalDataReaderStub(channel)

            handle = service.Open(exd_api.Identifier(
                url="/data/raw1.tdms", parameters=""), None)
            try:
                structure = service.GetStructure(
                    exd_api.StructureRequest(handle=handle), None)
                logging.info(MessageToJson(structure))

                self.assertEqual(structure.name, "raw1.tdms")
                self.assertEqual(len(structure.groups), 1)
                self.assertEqual(structure.groups[0].number_of_rows, 2000)
                self.assertEqual(len(structure.groups[0].channels), 7)
                self.assertEqual(structure.groups[0].id, 0)
                self.assertEqual(structure.groups[0].channels[0].id, 0)
                self.assertEqual(structure.groups[0].channels[1].id, 1)
                self.assertEqual(
                    structure.groups[0].channels[0].data_type, ods.DataTypeEnum.DT_DOUBLE)
                self.assertEqual(
                    structure.groups[0].channels[1].data_type, ods.DataTypeEnum.DT_DOUBLE)
            finally:
                service.Close(handle, None)

    def test_get_values(self):
        with grpc.insecure_channel("localhost:50051") as channel:
            service = exd_grpc.ExternalDataReaderStub(channel)

            handle = service.Open(exd_api.Identifier(
                url="/data/raw1.tdms", parameters=""), None)

            try:
                values = service.GetValues(
                    exd_api.ValuesRequest(handle=handle, group_id=0, channel_ids=[
                                          0, 1], start=0, limit=4),
                    None,
                )
                self.assertEqual(values.id, 0)
                self.assertEqual(len(values.channels), 2)
                self.assertEqual(values.channels[0].id, 0)
                self.assertEqual(values.channels[1].id, 1)
                logging.info(MessageToJson(values))

                self.assertEqual(
                    values.channels[0].values.data_type, ods.DataTypeEnum.DT_DOUBLE)
                self.assertSequenceEqual(
                    values.channels[0].values.double_array.values,
                    [-0.18402661214026306, 0.1480147709585864, -
                        0.24506363109225746, -0.29725028229621264],
                )
                self.assertEqual(
                    values.channels[1].values.data_type, ods.DataTypeEnum.DT_DOUBLE)
                self.assertSequenceEqual(
                    values.channels[1].values.double_array.values,
                    [1.0303048799096652, 0.6497390667439802,
                        0.7638782921842098, 0.5508590960417493],
                )

            finally:
                service.Close(handle, None)


class TestDockerContainerWithHealthCheck(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # build Docker image
        result = subprocess.run(
            ["docker", "build", "-t", "asam-ods-exd-api-nptdms", "."],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Docker build failed: {result.stderr}")

        # Clean up any existing test container from previous runs
        subprocess.run(
            ["docker", "stop", "test_container"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        subprocess.run(
            ["docker", "rm", "test_container"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )

        example_file_path = pathlib.Path.joinpath(
            pathlib.Path(__file__).parent.resolve(), "..", "data")
        data_folder = pathlib.Path(example_file_path).absolute().resolve()
        cp = subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--rm",
                "--name",
                "test_container",
                "-p",
                "50051:50051",
                "-p",
                "50052:50052",
                "-v",
                f"{data_folder}:/data",
                "asam-ods-exd-api-nptdms",
                "python3",
                "external_data_file.py",
                "--health-check-enabled",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        if cp.returncode != 0:
            raise RuntimeError(f"Docker run failed: {cp.stderr}")
        cls.container_id = cp.stdout.strip()
        cls.__wait_for_port_ready(port=50051)

    @classmethod
    def tearDownClass(cls):
        # stop container
        subprocess.run(
            ["docker", "stop", "test_container"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )

    @classmethod
    def __wait_for_port_ready(cls, host="localhost", port=50051, timeout=30):
        start_time = time.time()
        while True:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex((host, port))
            sock.close()
            if result == 0:
                channel = grpc.insecure_channel(f"{host}:{port}")
                grpc.channel_ready_future(channel).result(timeout=5)
                return
            if time.time() - start_time > timeout:
                raise TimeoutError("Port did not become ready in time.")
            time.sleep(0.5)

    def test_container_health(self):
        channel = grpc.insecure_channel("localhost:50051")
        stub = exd_grpc.ExternalDataReaderStub(channel)
        self.assertIsNotNone(stub)
        grpc.channel_ready_future(channel).result(timeout=5)

    def test_structure(self):
        with grpc.insecure_channel("localhost:50051") as channel:
            service = exd_grpc.ExternalDataReaderStub(channel)

            handle = service.Open(exd_api.Identifier(
                url="/data/raw1.tdms", parameters=""), None)
            try:
                structure = service.GetStructure(
                    exd_api.StructureRequest(handle=handle), None)
                logging.info(MessageToJson(structure))

                self.assertEqual(structure.name, "raw1.tdms")
                self.assertEqual(len(structure.groups), 1)
                self.assertEqual(structure.groups[0].number_of_rows, 2000)
                self.assertEqual(len(structure.groups[0].channels), 7)
                self.assertEqual(structure.groups[0].id, 0)
                self.assertEqual(structure.groups[0].channels[0].id, 0)
                self.assertEqual(structure.groups[0].channels[1].id, 1)
                self.assertEqual(
                    structure.groups[0].channels[0].data_type, ods.DataTypeEnum.DT_DOUBLE)
                self.assertEqual(
                    structure.groups[0].channels[1].data_type, ods.DataTypeEnum.DT_DOUBLE)
            finally:
                service.Close(handle, None)

    def test_health_check_service_available(self):
        """Test that the health check service is accessible on port 50052."""
        channel = grpc.insecure_channel("localhost:50052")
        stub = health_pb2_grpc.HealthStub(channel)
        self.assertIsNotNone(stub)
        grpc.channel_ready_future(channel).result(timeout=5)
        channel.close()

    def test_health_check_status(self):
        """Test that the health check returns SERVING status for ExternalDataReader."""
        with grpc.insecure_channel("localhost:50052") as channel:
            stub = health_pb2_grpc.HealthStub(channel)
            response = stub.Check(
                health_pb2.HealthCheckRequest(  # pylint: disable=no-member
                    service="asam.ods.ExternalDataReader"
                ),
                timeout=5,
            )
            self.assertEqual(
                response.status, health_pb2.HealthCheckResponse.SERVING  # pylint: disable=no-member
            )

    def test_health_check_watch(self):
        """Test that the health check watch stream works."""
        with grpc.insecure_channel("localhost:50052") as channel:
            stub = health_pb2_grpc.HealthStub(channel)
            responses = stub.Watch(
                health_pb2.HealthCheckRequest(  # pylint: disable=no-member
                    service="asam.ods.ExternalDataReader"
                ),
                timeout=5,
            )
            # Get first response
            first_response = next(responses)
            self.assertEqual(
                first_response.status, health_pb2.HealthCheckResponse.SERVING  # pylint: disable=no-member
            )

    def test_get_values(self):
        with grpc.insecure_channel("localhost:50051") as channel:
            service = exd_grpc.ExternalDataReaderStub(channel)

            handle = service.Open(exd_api.Identifier(
                url="/data/raw1.tdms", parameters=""), None)

            try:
                values = service.GetValues(
                    exd_api.ValuesRequest(handle=handle, group_id=0, channel_ids=[
                                          0, 1], start=0, limit=4),
                    None,
                )
                self.assertEqual(values.id, 0)
                self.assertEqual(len(values.channels), 2)
                self.assertEqual(values.channels[0].id, 0)
                self.assertEqual(values.channels[1].id, 1)
                logging.info(MessageToJson(values))

                self.assertEqual(
                    values.channels[0].values.data_type, ods.DataTypeEnum.DT_DOUBLE)
                self.assertSequenceEqual(
                    values.channels[0].values.double_array.values,
                    [-0.18402661214026306, 0.1480147709585864, -
                        0.24506363109225746, -0.29725028229621264],
                )
                self.assertEqual(
                    values.channels[1].values.data_type, ods.DataTypeEnum.DT_DOUBLE)
                self.assertSequenceEqual(
                    values.channels[1].values.double_array.values,
                    [1.0303048799096652, 0.6497390667439802,
                        0.7638782921842098, 0.5508590960417493],
                )

            finally:
                service.Close(handle, None)
