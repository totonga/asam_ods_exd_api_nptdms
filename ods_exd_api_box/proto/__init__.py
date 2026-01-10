"""
ASAM ODS protobuf stubs.
Dynamically load the generated protobuf modules.
This is necessary to integrate grpc stubs into a package
because the generated code uses relative imports.
"""

import sys
import os
import importlib.util


_proto_dir = os.path.dirname(__file__)


def _load_proto_module(module_name):
    spec = importlib.util.spec_from_file_location(
        module_name,
        os.path.join(_proto_dir, f"{module_name}.py")
    )
    module = importlib.util.module_from_spec(spec)
    # Register in sys.modules before executing so generated code can find dependencies
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


ods = _load_proto_module("ods_pb2")
exd_api = _load_proto_module("ods_external_data_pb2")
exd_grpc = _load_proto_module("ods_external_data_pb2_grpc")

__all__ = ['ods', 'exd_api', 'exd_grpc']
