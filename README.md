# ASAM ODS EXD-API npTDMS plugin

This repository contains a [ASAM ODS EXD-API](https://www.asam.net/standards/detail/ods/) plugin that uses [npTDMS](https://pypi.org/project/npTDMS/) to read the [NI TDMS](https://www.ni.com/en/support/documentation/supplemental/06/the-ni-tdms-file-format.html) files.

> This is only a prototype to check if it works with [npTDMS](https://pypi.org/project/npTDMS/).

> Special about the TDMS format is that it allows different length channels in a single group. So there needs to be some mapping for the ASAM EXD API which
> only allows a single `number_of_rows` for a single group.

## GRPC stub

Because the repository does not contain the ASAM ODS protobuf files the generated stubs are added.
The files that match `*_pb2*` are generated suing the following command. To renew them you must put the
proto files from the ODS standard into `proto_src` and rerun the command.

```bash
curl -o /workspaces/asam_ods_exd_api_nptdms/ods.proto https://raw.githubusercontent.com/asam-ev/ASAM-ODS-Interfaces/main/ods.proto
curl -o /workspaces/asam_ods_exd_api_nptdms/ods_external_data.proto https://raw.githubusercontent.com/asam-ev/ASAM-ODS-Interfaces/main/ods_external_data.proto
mkdir -p ods_exd_api_box/proto
python3 -m grpc_tools.protoc -I. --python_out=ods_exd_api_box/proto/. --pyi_out=ods_exd_api_box/proto/. --grpc_python_out=ods_exd_api_box/proto/. ods.proto ods_external_data.proto
```

## Content

### `exd_api_server.py`

Runs the GRPC service to be accessed using http-2.

### `external_data_reader.py`

Implements the EXD-API interface to access [NI TMS file Format (*.tdms)](https://www.ni.com/en/support/documentation/supplemental/06/the-ni-tdms-file-format.html) files using [npTDMS](https://pypi.org/project/npTDMS/).

### `exd_api_test.py`

Some basic tests on example files in `data` folder.

### `example_access_exd_api.ipynb`

jupyter notebook the shows communication done by ASAM ODS server or Importer using the EXD-API plugin.

## Docker

### Docker Image Details

The Docker image for this project is available at:

`ghcr.io/totonga/asam-ods-exd-api-nptdms:latest`

This image is automatically built and pushed via a GitHub Actions workflow. To pull and run the image:

```
docker pull ghcr.io/totonga/asam-ods-exd-api-nptdms:latest
docker run -v /path/to/local/data:/data -p 50051:50051 ghcr.io/totonga/asam-ods-exd-api-nptdms:latest
```

### Using the Docker Container

To build the Docker image locally:
```
docker build -t asam-ods-exd-api .
```

To start the Docker container:
```
docker run -v /path/to/local/data:/data -p 50051:50051 asam-ods-exd-api
```

### TLS / SSL

The server can be run in TLS mode by passing `--use-tls` together with paths to a PEM encoded certificate and private key. Mount a directory that contains your certificates and start the container with the full command so you can append the TLS flags:

```
docker run \
	-v /path/to/local/data:/data \
	-v /path/to/tls:/certs \
	-p 50051:50051 \
	asam-ods-exd-api \
	python3 external_data_file.py \
		--use-tls \
		--tls-cert-file /certs/server.crt \
		--tls-key-file /certs/server.key
```

If your deployment requires mutual TLS, also provide `--tls-client-ca-file` and `--require-client-cert`:

```
docker run \
	-v /path/to/local/data:/data \
	-v /path/to/tls:/certs \
	-p 50051:50051 \
	asam-ods-exd-api \
	python3 external_data_file.py \
		--use-tls \
		--tls-cert-file /certs/server.crt \
		--tls-key-file /certs/server.key \
		--tls-client-ca-file /certs/client-ca.crt \
		--require-client-cert
```

Clients must then provide their own certificate and key when connecting.
