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


class TestDifferentLength(unittest.TestCase):
    log = logging.getLogger(__name__)

    def _get_example_file_path(self, file_name):
        example_file_path = pathlib.Path.joinpath(pathlib.Path(__file__).parent.resolve(), '..', 'data', file_name)
        return pathlib.Path(example_file_path).absolute().resolve().as_uri()

    def test_different_length(self):
        with tempfile.TemporaryDirectory() as temporary_directory_name:
            file_path = os.path.join(temporary_directory_name, "all_datatypes.tdms")

            with TdmsWriter(file_path) as tdms_writer:
                tdms_writer.write_segment([ChannelObject("group_1", "channel_1", np.array([1.1, 1.2], np.float64))])
                tdms_writer.write_segment([ChannelObject("group_1", "channel_2", np.array([2.1, 2.2], np.float64))])
                tdms_writer.write_segment([ChannelObject("group_1", "channel_3", np.array([3.1, 3.2, 3.3, 3.4], np.float64))])
                tdms_writer.write_segment([ChannelObject("group_1", "channel_4", np.array([4.1, 4.2, 4.3, 4.4], np.float64))])

                tdms_writer.write_segment([ChannelObject("group_2", "channel_1", np.array([1.1, 1.2, 1.3, 1.4], np.float64))])
                tdms_writer.write_segment([ChannelObject("group_2", "channel_2", np.array([2.1, 2.2, 2.3, 2.4], np.float64))])
                tdms_writer.write_segment([ChannelObject("group_2", "channel_3", np.array([3.1, 3.2], np.float64))])
                tdms_writer.write_segment([ChannelObject("group_2", "channel_4", np.array([4.1, 4.2], np.float64))])

                tdms_writer.write_segment([ChannelObject("group_3", "channel_1", np.array([1.1, 1.2, 1.3, 1.4], np.float64))])
                tdms_writer.write_segment([ChannelObject("group_3", "channel_2", np.array([2.1, 2.2], np.float64))])
                tdms_writer.write_segment([ChannelObject("group_3", "channel_3", np.array([3.1, 3.2, 3.3, 3.4], np.float64))])
                tdms_writer.write_segment([ChannelObject("group_3", "channel_4", np.array([4.1, 4.2], np.float64))])

                tdms_writer.write_segment([ChannelObject("group_4", "channel_1", np.array([1.1, 1.2, 1.3, 1.4], np.float64))])
                tdms_writer.write_segment([ChannelObject("group_4", "channel_2", np.array([2.1, 2.2, 2.3, 2.4], np.float64))])
                tdms_writer.write_segment([ChannelObject("group_4", "channel_3", np.array([3.1, 3.2, 3.3, 3.4], np.float64))])
                tdms_writer.write_segment([ChannelObject("group_4", "channel_4", np.array([4.1, 4.2, 4.3, 4.4], np.float64))])


            service = ExternalDataReader()
            handle = service.Open(oed.Identifier(
                url = Path(file_path).resolve().as_uri(),
                parameters = ""), None)
            try:
                structure = service.GetStructure(oed.StructureRequest(handle=handle), None)
                self.log.info(MessageToJson(structure))

                self.assertEqual(structure.name, 'all_datatypes.tdms')
                self.assertEqual(len(structure.groups), 7)
                
                self.assertEqual(structure.groups[0].number_of_rows, 2)
                self.assertEqual(len(structure.groups[0].channels), 2)
                self.assertEqual(structure.groups[1].number_of_rows, 4)
                self.assertEqual(len(structure.groups[1].channels), 2)

                meta_group = structure.groups[0]
                values = service.GetValues(oed.ValuesRequest(handle=handle, group_id=meta_group.id, start=0, limit=4, channel_ids=[
                    meta_group.channels[0].id, meta_group.channels[1].id]), None)
                self.assertSequenceEqual(values.channels[0].values.double_array.values, [1.1, 1.2])
                self.assertSequenceEqual(values.channels[1].values.double_array.values, [2.1, 2.2])
                meta_group = structure.groups[1]
                values = service.GetValues(oed.ValuesRequest(handle=handle, group_id=meta_group.id, start=0, limit=4, channel_ids=[
                    meta_group.channels[0].id, meta_group.channels[1].id]), None)
                self.assertSequenceEqual(values.channels[0].values.double_array.values, [3.1, 3.2, 3.3, 3.4])
                self.assertSequenceEqual(values.channels[1].values.double_array.values, [4.1, 4.2, 4.3, 4.4])

                meta_group = structure.groups[2]
                values = service.GetValues(oed.ValuesRequest(handle=handle, group_id=meta_group.id, start=0, limit=4, channel_ids=[
                    meta_group.channels[0].id, meta_group.channels[1].id]), None)
                self.assertSequenceEqual(values.channels[0].values.double_array.values, [1.1, 1.2, 1.3, 1.4])
                self.assertSequenceEqual(values.channels[1].values.double_array.values, [2.1, 2.2, 2.3, 2.4])
                meta_group = structure.groups[3]
                values = service.GetValues(oed.ValuesRequest(handle=handle, group_id=meta_group.id, start=0, limit=4, channel_ids=[
                    meta_group.channels[0].id, meta_group.channels[1].id]), None)
                self.assertSequenceEqual(values.channels[0].values.double_array.values, [3.1, 3.2])
                self.assertSequenceEqual(values.channels[1].values.double_array.values, [4.1, 4.2])

                meta_group = structure.groups[4]
                values = service.GetValues(oed.ValuesRequest(handle=handle, group_id=meta_group.id, start=0, limit=4, channel_ids=[
                    meta_group.channels[0].id, meta_group.channels[1].id]), None)
                self.assertSequenceEqual(values.channels[0].values.double_array.values, [1.1, 1.2, 1.3, 1.4])
                self.assertSequenceEqual(values.channels[1].values.double_array.values, [3.1, 3.2, 3.3, 3.4])
                meta_group = structure.groups[5]
                values = service.GetValues(oed.ValuesRequest(handle=handle, group_id=meta_group.id, start=0, limit=4, channel_ids=[
                    meta_group.channels[0].id, meta_group.channels[1].id]), None)
                self.assertSequenceEqual(values.channels[0].values.double_array.values, [2.1, 2.2])
                self.assertSequenceEqual(values.channels[1].values.double_array.values, [4.1, 4.2])

                meta_group = structure.groups[6]
                values = service.GetValues(oed.ValuesRequest(handle=handle, group_id=meta_group.id, start=0, limit=4, channel_ids=[
                    meta_group.channels[0].id, meta_group.channels[1].id, meta_group.channels[2].id, meta_group.channels[3].id]), None)
                self.assertSequenceEqual(values.channels[0].values.double_array.values, [1.1, 1.2, 1.3, 1.4])
                self.assertSequenceEqual(values.channels[1].values.double_array.values, [2.1, 2.2, 2.3, 2.4])
                self.assertSequenceEqual(values.channels[2].values.double_array.values, [3.1, 3.2, 3.3, 3.4])
                self.assertSequenceEqual(values.channels[3].values.double_array.values, [4.1, 4.2, 4.3, 4.4])

            finally:
                service.Close(handle, None)


if __name__ == '__main__':
    unittest.main()