# ASAM ODS EXD-API for NI TDMS Files

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Type checking: mypy](https://img.shields.io/badge/type%20checking-mypy-blue.svg)](http://mypy-lang.org/)

A production-ready [ASAM ODS EXD-API](https://www.asam.net/standards/detail/ods/) implementation for reading [National Instruments TDMS](https://www.ni.com/en/support/documentation/supplemental/06/the-ni-tdms-file-format.html) files. Provides a gRPC service with full type safety and comprehensive error handling.

## Features

- 🔄 **ASAM ODS EXD-API Compliant** - Full implementation of the ASAM ODS External Data API specification
- 📊 **TDMS Support** - Read NI TDMS files with automatic channel grouping and length mapping
- 🔒 **TLS/SSL Support** - Optional transport layer security with mutual TLS support
- 🐳 **Docker Ready** - Pre-built images available on GitHub Container Registry
- 🛡️ **Type Safe** - 100% type hints with mypy validation
- 📝 **Well Documented** - Comprehensive docstrings and examples
- 🧪 **Tested** - Comprehensive test suite with Docker integration tests

## Quick Start

### Installation

**Requirements:** Python 3.12 or higher

```bash
# Clone the repository
git clone https://github.com/totonga/asam_ods_exd_api_nptdms.git
cd asam_ods_exd_api_nptdms

# Install the package
pip install -e .
```

### Running the Server

```bash
python external_data_file.py
```

The server will start listening on `localhost:50051`.

### Using Docker

```bash
# Pull the pre-built image
docker pull ghcr.io/totonga/asam-ods-exd-api-nptdms:latest

# Run the container
docker run -p 50051:50051 ghcr.io/totonga/asam-ods-exd-api-nptdms:latest
```

## Project Structure

```
ods_exd_api_nptdms/
├── ods_exd_api_box/            # ASAM ODS EXD API abstraction
├── external_data_file.py       # TDMS file handler & entry point
├── tests/                      # Comprehensive test suite
├── data/                       # Example TDMS files
├── example_access_exd_api.ipynb # Interactive tutorial
├── pyproject.toml              # Project configuration
└── README.md                   # This file
```

## Architecture

### Core Components

- **`external_data_file.py`** - TDMS-specific handler using the `npTDMS` library

### TDMS Channel Mapping

The TDMS format allows channels of different lengths within a group, but the ASAM EXD API requires uniform row counts per group. This implementation handles this by:

1. Analyzing all channels in a group by their length
2. Creating sub-groups for each unique channel length
3. Mapping channel indices transparently

## Configuration & Usage

### Server Options

```bash
python external_data_file.py --help
```

Key configuration options:

| Option | Default | Description |
|--------|---------|-------------|
| `--port` | 50051 | gRPC server port |
| `--bind-address` | `[::]` | Server bind address |
| `--max-workers` | CPU count × 2 | Thread pool size |
| `--max-concurrent-streams` | None | Max concurrent gRPC streams |
| `--use-tls` | False | Enable TLS/SSL |
| `--tls-cert-file` | - | Path to server certificate (PEM) |
| `--tls-key-file` | - | Path to server private key (PEM) |
| `--tls-client-ca-file` | - | CA bundle for client verification |
| `--require-client-cert` | False | Require valid client certificate |
| `--verbose` | False | Enable debug logging |
| `--health-check-enabled` | False | Enable health check service |

### TLS Configuration

**Basic TLS:**

```bash
python external_data_file.py \
  --use-tls \
  --tls-cert-file /path/to/server.crt \
  --tls-key-file /path/to/server.key
```

**Mutual TLS (mTLS):**

```bash
python external_data_file.py \
  --use-tls \
  --tls-cert-file /path/to/server.crt \
  --tls-key-file /path/to/server.key \
  --tls-client-ca-file /path/to/client-ca.crt \
  --require-client-cert
```

**Docker with TLS:**

```bash
docker run \
  -v /path/to/certs:/certs \
  -p 50051:50051 \
  ghcr.io/totonga/asam-ods-exd-api-nptdms:latest \
  --use-tls \
  --tls-cert-file /certs/server.crt \
  --tls-key-file /certs/server.key
```

## Development

### Setup Development Environment

```bash
# Install with development dependencies
pip install -e ".[dev]"
```

### Type Checking

```bash
mypy . --config-file=mypy.ini
```

### Running Tests

```bash
python -m pytest tests/
```

### Running Docker Integration Tests

```bash
python -m pytest tests/test_docker_integration.py
```

### Code Style

The project uses:
- **Black** for code formatting
- **isort** for import sorting
- **Pylint** for linting
- **Mypy** for static type checking

### Updating Protocol Buffers

The protobuf files are generated from the ASAM ODS standard specifications:

```bash
# Download latest proto files
curl -o ods.proto https://raw.githubusercontent.com/asam-ev/ASAM-ODS-Interfaces/main/ods.proto
curl -o ods_external_data.proto https://raw.githubusercontent.com/asam-ev/ASAM-ODS-Interfaces/main/ods_external_data.proto

# Generate Python stubs
mkdir -p ods_exd_api_box/proto
python3 -m grpc_tools.protoc \
  -I. \
  --python_out=ods_exd_api_box/proto/. \
  --pyi_out=ods_exd_api_box/proto/. \
  --grpc_python_out=ods_exd_api_box/proto/. \
  ods.proto ods_external_data.proto
```

## Examples

### Jupyter Notebook

See [example_access_exd_api.ipynb](example_access_exd_api.ipynb) for an interactive walkthrough with detailed examples.

## Docker Deployment

### Pre-built Images

Pre-built images are automatically published to GitHub Container Registry for every release:

```bash
docker pull ghcr.io/totonga/asam-ods-exd-api-nptdms:latest
docker pull ghcr.io/totonga/asam-ods-exd-api-nptdms:0.1.0  # specific version
```

### Build Custom Image

```bash
docker build -t my-tdms-server:latest .
```

### Example Deployment

**With data volume:**

```bash
docker run \
  -v /path/to/data:/data \
  -p 50051:50051 \
  ghcr.io/totonga/asam-ods-exd-api-nptdms:latest
```

**With health checks:**

```bash
docker run \
  -p 50051:50051 \
  --health-cmd="python -c 'import grpc; grpc.insecure_channel(\"localhost:50052\").close()' || exit 1" \
  --health-interval=10s \
  --health-timeout=5s \
  --health-retries=3 \
  ghcr.io/totonga/asam-ods-exd-api-nptdms:latest \
  --health-check-enabled \
  --health-check-port 50052
```

## Performance Considerations

- **File Caching** - Opened files are cached to minimize I/O operations
- **Reference Counting** - Automatic resource cleanup with reference counting
- **Thread Pool** - Configurable worker threads for parallel request handling
- **Message Size Limits** - Configurable max send/receive message sizes

## Troubleshooting

### Connection Refused

Ensure the server is running and listening on the correct port:

```bash
netstat -tlnp | grep 50051
```

### TLS Certificate Errors

Verify certificate paths and permissions:

```bash
openssl x509 -in /path/to/cert.crt -text -noout
```

### Type Checking Failures

Ensure all dependencies are installed with type stubs:

```bash
pip install -e ".[dev]"
mypy . --config-file=mypy.ini
```

## Contributing

Contributions are welcome! Please:

1. Ensure type checking passes: `mypy . --config-file=mypy.ini`
2. Run tests: `pytest tests/`
3. Follow code style (Black, isort)
4. Add tests for new features

## License

MIT License - see [LICENSE](LICENSE) file for details.

## References

- [ASAM ODS Standard](https://www.asam.net/standards/detail/ods/)
- [ASAM ODS GitHub Repository](https://github.com/asam-ev/ASAM-ODS-Interfaces)
- [NI TDMS File Format](https://www.ni.com/en/support/documentation/supplemental/06/the-ni-tdms-file-format.html)
- [npTDMS Library](https://pypi.org/project/npTDMS/)
- [gRPC Documentation](https://grpc.io/docs/)
