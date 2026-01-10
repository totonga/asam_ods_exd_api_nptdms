"""ASAM ODS protobuf stubs."""
# fmt: off
# isort: skip_file

import sys
import os

# Temporarily add proto directory to path so generated stubs can import each other
_proto_dir = os.path.dirname(__file__)
sys.path.insert(0, _proto_dir)

try:
    import ods_pb2 as ods
    import ods_external_data_pb2 as exd_api
    import ods_external_data_pb2_grpc as exd_grpc

    # Register in sys.modules so the generated code can find these modules
    # This is needed because ods_external_data.proto includes ods.proto
    sys.modules['ods_pb2'] = ods
    sys.modules['ods_external_data_pb2'] = exd_api
    sys.modules['ods_external_data_pb2_grpc'] = exd_grpc
finally:
    sys.path.pop(0)

__all__ = ['ods', 'exd_api', 'exd_grpc']
