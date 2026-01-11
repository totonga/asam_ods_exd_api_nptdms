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


class TestDockerContainerTLS(unittest.TestCase):
    container_name = "test_container_tls"
    tls_port = 50052
    health_check_port = 50053

    @classmethod
    def setUpClass(cls):
        subprocess.run(
            ["docker", "build", "-t", "asam-ods-exd-api-nptdms", "."],
            check=True,
        )

        example_data_path = pathlib.Path.joinpath(
            pathlib.Path(__file__).parent.resolve(), "..", "data")
        cls.data_folder = pathlib.Path(example_data_path).absolute().resolve()
        cls.certs_folder = (pathlib.Path(__file__).parent /
                            "certs").absolute().resolve()

        cp = subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--rm",
                "--name",
                cls.container_name,
                "-p",
                f"{cls.tls_port}:{cls.tls_port}",
                "-p",
                f"{cls.health_check_port}:{cls.health_check_port}",
                "-v",
                f"{cls.data_folder}:/data",
                "-v",
                f"{cls.certs_folder}:/certs",
                "asam-ods-exd-api-nptdms",
                "python3",
                "external_data_file.py",
                "--port",
                str(cls.tls_port),
                "--health-check-enabled",
                "--health-check-port",
                str(cls.health_check_port),
                "--use-tls",
                "--tls-cert-file",
                "/certs/server.crt",
                "--tls-key-file",
                "/certs/server.key",
            ],
            stdout=subprocess.PIPE,
            check=True,
        )
        cls.container_id = cp.stdout.decode().strip()
        cls.__wait_for_port_ready(port=cls.tls_port)
        cls.__wait_for_port_ready(port=cls.health_check_port)

    @classmethod
    def tearDownClass(cls):
        subprocess.run(
            ["docker", "stop", cls.container_name],
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
                return
            if time.time() - start_time > timeout:
                raise TimeoutError("Port did not become ready in time.")
            time.sleep(0.5)

    def test_tls_structure(self):
        cert_path = pathlib.Path(__file__).parent / "certs" / "server.crt"
        with cert_path.open("rb") as cert_file:
            channel_credentials = grpc.ssl_channel_credentials(
                root_certificates=cert_file.read())

        with grpc.secure_channel(f"localhost:{self.tls_port}", channel_credentials) as channel:
            grpc.channel_ready_future(channel).result(timeout=5)
            service = exd_grpc.ExternalDataReaderStub(channel)
            handle = service.Open(exd_api.Identifier(
                url="/data/raw1.tdms", parameters=""), None)
            try:
                structure = service.GetStructure(
                    exd_api.StructureRequest(handle=handle), None)
                self.assertEqual(structure.name, "raw1.tdms")
            finally:
                service.Close(handle, None)

    def _get_tls_channel(self):
        """Helper to create a TLS-secured channel."""
        cert_path = pathlib.Path(__file__).parent / "certs" / "server.crt"
        with cert_path.open("rb") as cert_file:
            channel_credentials = grpc.ssl_channel_credentials(
                root_certificates=cert_file.read())
        return grpc.secure_channel(f"localhost:{self.tls_port}", channel_credentials)

    def test_container_health(self):
        with self._get_tls_channel() as channel:
            stub = exd_grpc.ExternalDataReaderStub(channel)
            self.assertIsNotNone(stub)
            grpc.channel_ready_future(channel).result(timeout=5)

    def test_structure(self):
        with self._get_tls_channel() as channel:
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
        with self._get_tls_channel() as channel:
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

    def test_health_check_service_available(self):
        """Test that the health check service is accessible on insecure port even with TLS enabled."""
        channel = grpc.insecure_channel(f"localhost:{self.health_check_port}")
        stub = health_pb2_grpc.HealthStub(channel)
        self.assertIsNotNone(stub)
        grpc.channel_ready_future(channel).result(timeout=5)
        channel.close()

    def test_health_check_status(self):
        """Test that the health check returns SERVING status for ExternalDataReader."""
        with grpc.insecure_channel(f"localhost:{self.health_check_port}") as channel:
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
        with grpc.insecure_channel(f"localhost:{self.health_check_port}") as channel:
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


class TestDockerContainerTLSClient(unittest.TestCase):
    container_name = "test_container_tls_client"
    tls_port = 50053

    @classmethod
    def setUpClass(cls):
        subprocess.run(
            ["docker", "build", "-t", "asam-ods-exd-api-nptdms", "."],
            check=True,
        )

        example_data_path = pathlib.Path.joinpath(
            pathlib.Path(__file__).parent.resolve(), "..", "data")
        cls.data_folder = pathlib.Path(example_data_path).absolute().resolve()
        cls.certs_folder = (pathlib.Path(__file__).parent /
                            "certs").absolute().resolve()

        cp = subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--rm",
                "--name",
                cls.container_name,
                "-p",
                f"{cls.tls_port}:{cls.tls_port}",
                "-v",
                f"{cls.data_folder}:/data",
                "-v",
                f"{cls.certs_folder}:/certs",
                "asam-ods-exd-api-nptdms",
                "python3",
                "external_data_file.py",
                "--port",
                str(cls.tls_port),
                "--use-tls",
                "--tls-cert-file",
                "/certs/server.crt",
                "--tls-key-file",
                "/certs/server.key",
                "--tls-client-ca-file",
                "/certs/client.crt",
                "--require-client-cert",
            ],
            stdout=subprocess.PIPE,
            check=True,
        )
        cls.container_id = cp.stdout.decode().strip()
        cls.__wait_for_port_ready(port=cls.tls_port)

    @classmethod
    def tearDownClass(cls):
        subprocess.run(["docker", "stop", cls.container_name], check=True)

    @classmethod
    def __wait_for_port_ready(cls, host="localhost", port=50051, timeout=30):
        start_time = time.time()
        while True:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex((host, port))
            sock.close()
            if result == 0:
                return
            if time.time() - start_time > timeout:
                raise TimeoutError("Port did not become ready in time.")
            time.sleep(0.5)

    def _get_mtls_channel(self):
        """Helper to create a mutually-TLS-secured channel with client cert."""
        server_cert_path = pathlib.Path(
            __file__).parent / "certs" / "server.crt"
        client_cert_path = pathlib.Path(
            __file__).parent / "certs" / "client.crt"
        client_key_path = pathlib.Path(
            __file__).parent / "certs" / "client.key"

        with server_cert_path.open("rb") as f:
            root_certificates = f.read()
        with client_cert_path.open("rb") as f:
            client_cert = f.read()
        with client_key_path.open("rb") as f:
            client_key = f.read()

        channel_credentials = grpc.ssl_channel_credentials(
            root_certificates=root_certificates, private_key=client_key, certificate_chain=client_cert
        )
        return grpc.secure_channel(f"localhost:{self.tls_port}", channel_credentials)

    def test_container_health(self):
        with self._get_mtls_channel() as channel:
            stub = exd_grpc.ExternalDataReaderStub(channel)
            self.assertIsNotNone(stub)
            grpc.channel_ready_future(channel).result(timeout=5)

    def test_structure(self):
        with self._get_mtls_channel() as channel:
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
        with self._get_mtls_channel() as channel:
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
