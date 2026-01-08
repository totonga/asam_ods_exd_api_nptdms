import logging
import pathlib
import unittest

from google.protobuf.json_format import MessageToJson

from ods_exd_api_box import ExternalDataReader, FileHandlerRegistry
from external_data_file import ExternalDataFile, ods, exd_api


class TestExdApiEtc(unittest.TestCase):
    log = logging.getLogger(__name__)

    def setUp(self):
        """Register ExternalDataFile handler before each test."""
        FileHandlerRegistry.register('tdms', ExternalDataFile)

    def _get_example_file_path(self, file_name):
        example_file_path = pathlib.Path.joinpath(pathlib.Path(
            __file__).parent.resolve(), '..', 'data', file_name)
        return pathlib.Path(example_file_path).absolute().resolve().as_uri()

    def test_file_example(self):
        service = ExternalDataReader()
        handle = service.Open(exd_api.Identifier(
            url=self._get_example_file_path('example.tdms'),
            parameters=""), None)
        try:
            structure = service.GetStructure(
                exd_api.StructureRequest(handle=handle), None)
            self.log.info(MessageToJson(structure))

            self.assertEqual(structure.name, 'example.tdms')
            self.assertEqual(len(structure.groups), 2)
            self.assertEqual(structure.groups[0].number_of_rows, 15)
            self.assertEqual(len(structure.groups[0].channels), 3)
            self.assertEqual(
                structure.groups[0].channels[0].data_type, ods.DataTypeEnum.DT_FLOAT)
            self.assertEqual(
                structure.groups[0].channels[1].data_type, ods.DataTypeEnum.DT_FLOAT)
            self.assertEqual(structure.groups[1].number_of_rows, 15)
            self.assertEqual(len(structure.groups[1].channels), 4)
            self.assertEqual(
                structure.groups[1].channels[0].data_type, ods.DataTypeEnum.DT_DOUBLE)
            self.assertEqual(
                structure.groups[1].channels[1].data_type, ods.DataTypeEnum.DT_DOUBLE)

            values = service.GetValues(exd_api.ValuesRequest(handle=handle,
                                                             group_id=0,
                                                             channel_ids=[
                                                                 0, 1],
                                                             start=0,
                                                             limit=4), None)
            self.assertEqual(len(values.channels), 2)
            self.log.info(MessageToJson(values))
            self.assertEqual(
                values.channels[0].values.data_type, ods.DataTypeEnum.DT_FLOAT)
            self.assertSequenceEqual(values.channels[0].values.float_array.values, [
                                     500.0, 700.0, 900.0, 1100.0])
            self.assertEqual(
                values.channels[1].values.data_type, ods.DataTypeEnum.DT_FLOAT)
            self.assertSequenceEqual(values.channels[1].values.float_array.values, [
                                     150.0, 160.0, 170.0, 180.0])

            values = service.GetValues(exd_api.ValuesRequest(handle=handle,
                                                             group_id=1,
                                                             channel_ids=[
                                                                 0, 1],
                                                             start=0,
                                                             limit=4), None)
            self.assertEqual(len(values.channels), 2)
            self.log.info(MessageToJson(values))
            self.assertEqual(
                values.channels[0].values.data_type, ods.DataTypeEnum.DT_DOUBLE)
            self.assertSequenceEqual(
                values.channels[0].values.double_array.values, [1.0, 2.0, 3.0, 4.0])
            self.assertEqual(
                values.channels[1].values.data_type, ods.DataTypeEnum.DT_DOUBLE)
            self.assertSequenceEqual(values.channels[1].values.double_array.values, [
                                     500.0, 700.0, 900.0, 1100.0])

        finally:
            service.Close(handle, None)

    def test_file_big_endian(self):
        service = ExternalDataReader()
        handle = service.Open(exd_api.Identifier(
            url=self._get_example_file_path('big_endian.tdms'),
            parameters=""), None)
        try:
            structure = service.GetStructure(
                exd_api.StructureRequest(handle=handle), None)
            self.log.info(MessageToJson(structure))

            self.assertEqual(len(structure.groups), 1)
            self.assertEqual(structure.groups[0].number_of_rows, 3500)
            self.assertEqual(len(structure.groups[0].channels), 2)
            self.assertEqual(
                structure.groups[0].channels[0].data_type, ods.DataTypeEnum.DT_DOUBLE)
            self.assertEqual(
                structure.groups[0].channels[1].data_type, ods.DataTypeEnum.DT_DOUBLE)

            values = service.GetValues(exd_api.ValuesRequest(handle=handle,
                                                             group_id=0,
                                                             channel_ids=[
                                                                 0, 1],
                                                             start=0,
                                                             limit=4), None)
            self.assertEqual(len(values.channels), 2)
            self.log.info(MessageToJson(values))
            self.assertEqual(
                values.channels[0].values.data_type, ods.DataTypeEnum.DT_DOUBLE)
            self.assertSequenceEqual(
                values.channels[0].values.double_array.values, [0.0, 0.0, 0.0, 0.0])
            self.assertEqual(
                values.channels[1].values.data_type, ods.DataTypeEnum.DT_DOUBLE)
            self.assertSequenceEqual(values.channels[1].values.double_array.values, [
                                     0.0, 0.0634175857813252, 0.1265798623799041, 0.18923254844743084])

        finally:
            service.Close(handle, None)

    def test_file_Digital_Input(self):
        service = ExternalDataReader()
        handle = service.Open(exd_api.Identifier(
            url=self._get_example_file_path('Digital_Input.tdms'),
            parameters=""), None)
        try:
            structure = service.GetStructure(
                exd_api.StructureRequest(handle=handle), None)
            self.log.info(MessageToJson(structure))

            self.assertEqual(len(structure.groups), 3)
            self.assertEqual(structure.groups[0].number_of_rows, 20000)
            self.assertEqual(structure.groups[1].number_of_rows, 400)
            self.assertEqual(structure.groups[2].number_of_rows, 8)
            self.assertEqual(len(structure.groups[0].channels), 1)
            self.assertEqual(len(structure.groups[1].channels), 1)
            self.assertEqual(len(structure.groups[2].channels), 1)
            self.assertEqual(
                structure.groups[0].channels[0].data_type, ods.DataTypeEnum.DT_BYTE)
            self.assertEqual(
                structure.groups[1].channels[0].data_type, ods.DataTypeEnum.DT_BYTE)
            self.assertEqual(
                structure.groups[2].channels[0].data_type, ods.DataTypeEnum.DT_BYTE)

            values = service.GetValues(exd_api.ValuesRequest(handle=handle,
                                                             group_id=0,
                                                             channel_ids=[0],
                                                             start=0,
                                                             limit=4), None)
            self.assertEqual(len(values.channels), 1)
            self.log.info(MessageToJson(values))
            self.assertEqual(
                values.channels[0].values.data_type, ods.DataTypeEnum.DT_BYTE)
            self.assertSequenceEqual(
                values.channels[0].values.byte_array.values, [0.0, 1.0, 0.0, 1.0])

        finally:
            service.Close(handle, None)

    def test_file_raw_timestamps(self):
        service = ExternalDataReader()
        handle = service.Open(exd_api.Identifier(
            url=self._get_example_file_path('raw_timestamps.tdms'),
            parameters=""), None)
        try:
            structure = service.GetStructure(
                exd_api.StructureRequest(handle=handle), None)
            self.log.info(MessageToJson(structure))

            self.assertEqual(len(structure.groups), 1)
            self.assertEqual(structure.groups[0].number_of_rows, 128)
            self.assertEqual(len(structure.groups[0].channels), 1)
            self.assertEqual(
                structure.groups[0].channels[0].data_type, ods.DataTypeEnum.DT_DOUBLE)

            values = service.GetValues(exd_api.ValuesRequest(handle=handle,
                                                             group_id=0,
                                                             channel_ids=[0],
                                                             start=0,
                                                             limit=4), None)
            self.assertEqual(len(values.channels), 1)
            self.log.info(MessageToJson(values))
            self.assertEqual(
                values.channels[0].values.data_type, ods.DataTypeEnum.DT_DOUBLE)
            self.assertSequenceEqual(values.channels[0].values.double_array.values, [
                                     0.0, 0.049067674327418015, 0.0980171403295606, 0.14673047445536175])

        finally:
            service.Close(handle, None)
