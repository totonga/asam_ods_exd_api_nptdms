# Prepare python to use GRPC interface:
# python -m grpc_tools.protoc --proto_path=proto_src --pyi_out=. --python_out=. --grpc_python_out=. ods.proto ods_external_data.proto
import sys, os
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/..")

import logging
import pathlib
import unittest
from datetime import datetime
import tempfile
from pathlib import Path

import numpy as np

import ods_pb2 as ods
import ods_external_data_pb2 as oed

from external_data_reader import ExternalDataReader
from google.protobuf.json_format import MessageToJson

from nptdms import TdmsFile, TdmsWriter, RootObject, GroupObject, ChannelObject, types


class TestDataTypes(unittest.TestCase):
    log = logging.getLogger(__name__)

    def _get_example_file_path(self, file_name):
        example_file_path = pathlib.Path.joinpath(pathlib.Path(__file__).parent.resolve(), '..', 'data', file_name)
        return pathlib.Path(example_file_path).absolute().resolve().as_uri()

    def test_datatype(self):
        with tempfile.TemporaryDirectory() as temporary_directory_name:
            file_path = os.path.join(temporary_directory_name, "all_datatypes.tdms")

            with TdmsWriter(file_path) as tdms_writer:
                tdms_writer.write_segment([ChannelObject("group_complex", "complex64_data",  np.array([1+2j, 3+4j], np.complex64))])
                tdms_writer.write_segment([ChannelObject("group_complex", "complex128_data", np.array([5+6j, 7+8j], np.complex128))])

                tdms_writer.write_segment([ChannelObject("group_int", "int8_data", np.array([-2, 4], np.int8))])
                tdms_writer.write_segment([ChannelObject("group_int", "uint8_data", np.array([2, 4], np.uint8))])
                tdms_writer.write_segment([ChannelObject("group_int", "int16_data", np.array([-2, 4], np.int16))])
                tdms_writer.write_segment([ChannelObject("group_int", "uint16_data", np.array([2, 4], np.uint16))])
                tdms_writer.write_segment([ChannelObject("group_int", "int32_data", np.array([-2, 4], np.int32))])
                tdms_writer.write_segment([ChannelObject("group_int", "uint32_data", np.array([2, 4], np.uint32))])
                tdms_writer.write_segment([ChannelObject("group_int", "int64_data", np.array([-2, 4], np.int64))])
                tdms_writer.write_segment([ChannelObject("group_int", "uint64_data", np.array([2, 4], np.uint64))])

                tdms_writer.write_segment([ChannelObject("group_date", "date_data", np.array([datetime(2017, 7, 9, 12, 35, 0), datetime(2017, 7, 9, 12, 36, 0)], np.datetime64))])

                tdms_writer.write_segment([ChannelObject("group_real", "float32_data", np.array([1.1, 1.2], np.float32))])
                tdms_writer.write_segment([ChannelObject("group_real", "float64_data", np.array([2.1, 2.2], np.float64))])

                tdms_writer.write_segment([ChannelObject("group_string", "string_data", ["abc", "def"])])


            service = ExternalDataReader()
            handle = service.Open(oed.Identifier(
                url = Path(file_path).resolve().as_uri(),
                parameters = ""), None)
            try:
                structure = service.GetStructure(oed.StructureRequest(handle=handle), None)
                self.log.info(MessageToJson(structure))

                self.assertEqual(structure.name, 'all_datatypes.tdms')
                self.assertEqual(len(structure.groups), 5)
                self.assertEqual(structure.groups[0].number_of_rows, 2)
                self.assertEqual(len(structure.groups[0].channels), 2)
                self.assertEqual(structure.groups[1].number_of_rows, 2)
                self.assertEqual(len(structure.groups[1].channels), 8)
                self.assertEqual(structure.groups[2].number_of_rows, 2)
                self.assertEqual(len(structure.groups[2].channels), 1)
                self.assertEqual(structure.groups[3].number_of_rows, 2)
                self.assertEqual(len(structure.groups[3].channels), 2)
                self.assertEqual(structure.groups[4].number_of_rows, 2)
                self.assertEqual(len(structure.groups[4].channels), 1)

                self.assertEqual(structure.groups[0].channels[0].data_type, ods.DataTypeEnum.DT_COMPLEX)
                self.assertEqual(structure.groups[0].channels[1].data_type, ods.DataTypeEnum.DT_DCOMPLEX)

                self.assertEqual(structure.groups[1].channels[0].data_type, ods.DataTypeEnum.DT_SHORT)
                self.assertEqual(structure.groups[1].channels[1].data_type, ods.DataTypeEnum.DT_BYTE)
                self.assertEqual(structure.groups[1].channels[2].data_type, ods.DataTypeEnum.DT_SHORT)
                self.assertEqual(structure.groups[1].channels[3].data_type, ods.DataTypeEnum.DT_LONG)
                self.assertEqual(structure.groups[1].channels[4].data_type, ods.DataTypeEnum.DT_LONG)
                self.assertEqual(structure.groups[1].channels[5].data_type, ods.DataTypeEnum.DT_LONGLONG)
                self.assertEqual(structure.groups[1].channels[6].data_type, ods.DataTypeEnum.DT_LONGLONG)
                self.assertEqual(structure.groups[1].channels[7].data_type, ods.DataTypeEnum.DT_DOUBLE)

                self.assertEqual(structure.groups[2].channels[0].data_type, ods.DataTypeEnum.DT_DATE)

                self.assertEqual(structure.groups[3].channels[0].data_type, ods.DataTypeEnum.DT_FLOAT)
                self.assertEqual(structure.groups[3].channels[1].data_type, ods.DataTypeEnum.DT_DOUBLE)

                self.assertEqual(structure.groups[4].channels[0].data_type, ods.DataTypeEnum.DT_STRING)

                values = service.GetValues(oed.ValuesRequest(handle=handle, group_id=0, start=0, limit=2, channel_ids=[0,1]), None)
                self.assertEqual(values.channels[0].values.data_type, ods.DataTypeEnum.DT_COMPLEX)
                self.assertSequenceEqual(values.channels[0].values.float_array.values, [1.0, 2.0, 3.0, 4.0])
                self.assertEqual(values.channels[1].values.data_type, ods.DataTypeEnum.DT_DCOMPLEX)
                self.assertSequenceEqual(values.channels[1].values.double_array.values, [5.0, 6.0, 7.0, 8.0])

                values = service.GetValues(oed.ValuesRequest(handle=handle, group_id=1, start=0, limit=2, channel_ids=[0,1,2,3,4,5,6,7]), None)
                self.assertEqual(values.channels[0].values.data_type, ods.DataTypeEnum.DT_SHORT)
                self.assertSequenceEqual(values.channels[0].values.long_array.values, [-2, 4])
                self.assertEqual(values.channels[1].values.data_type, ods.DataTypeEnum.DT_BYTE)
                self.assertSequenceEqual(values.channels[1].values.byte_array.values, [2, 4])
                self.assertEqual(values.channels[2].values.data_type, ods.DataTypeEnum.DT_SHORT)
                self.assertSequenceEqual(values.channels[2].values.long_array.values, [-2, 4])
                self.assertEqual(values.channels[3].values.data_type, ods.DataTypeEnum.DT_LONG)
                self.assertSequenceEqual(values.channels[3].values.long_array.values, [2, 4])
                self.assertEqual(values.channels[4].values.data_type, ods.DataTypeEnum.DT_LONG)
                self.assertSequenceEqual(values.channels[4].values.long_array.values, [-2, 4])
                self.assertEqual(values.channels[5].values.data_type, ods.DataTypeEnum.DT_LONGLONG)
                self.assertSequenceEqual(values.channels[5].values.longlong_array.values, [2, 4])
                self.assertEqual(values.channels[6].values.data_type, ods.DataTypeEnum.DT_LONGLONG)
                self.assertSequenceEqual(values.channels[6].values.longlong_array.values, [-2, 4])
                self.assertEqual(values.channels[7].values.data_type, ods.DataTypeEnum.DT_DOUBLE)
                self.assertSequenceEqual(values.channels[7].values.double_array.values, [2.0, 4.0])

                values = service.GetValues(oed.ValuesRequest(handle=handle, group_id=2, start=0, limit=2, channel_ids=[0]), None)
                self.assertEqual(values.channels[0].values.data_type, ods.DataTypeEnum.DT_DATE)
                self.assertSequenceEqual(values.channels[0].values.string_array.values, ['20170709123500000000', '20170709123600000000'])

                values = service.GetValues(oed.ValuesRequest(handle=handle, group_id=3, start=0, limit=2, channel_ids=[0,1]), None)
                self.assertEqual(values.channels[0].values.data_type, ods.DataTypeEnum.DT_FLOAT)
                self.assertSequenceEqual(values.channels[0].values.float_array.values, [1.100000023841858, 1.2000000476837158])
                self.assertEqual(values.channels[1].values.data_type, ods.DataTypeEnum.DT_DOUBLE)
                self.assertSequenceEqual(values.channels[1].values.double_array.values, [2.1, 2.2])

                values = service.GetValues(oed.ValuesRequest(handle=handle, group_id=4, start=0, limit=2, channel_ids=[0]), None)
                self.assertEqual(values.channels[0].values.data_type, ods.DataTypeEnum.DT_STRING)
                self.assertSequenceEqual(values.channels[0].values.string_array.values, ['abc', 'def'])

            finally:
                service.Close(handle, None)


if __name__ == '__main__':
    unittest.main()