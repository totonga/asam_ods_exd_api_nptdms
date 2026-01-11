import logging
import pathlib
import unittest

from google.protobuf.json_format import MessageToJson

from external_data_file import ExternalDataFile
from ods_exd_api_box import ExternalDataReader, FileHandlerRegistry, exd_api, ods


class TestExdApi(unittest.TestCase):
    log = logging.getLogger(__name__)

    def setUp(self):
        """Register ExternalDataFile handler before each test."""
        FileHandlerRegistry.register(file_type_name="test", factory=ExternalDataFile)

    def _get_example_file_path(self, file_name):
        example_file_path = pathlib.Path.joinpath(
            pathlib.Path(__file__).parent.resolve(), "..", "data", file_name
        )
        return pathlib.Path(example_file_path).absolute().resolve().as_uri()

    def test_open(self):
        service = ExternalDataReader()
        handle = service.Open(
            exd_api.Identifier(url=self._get_example_file_path("raw1.tdms"), parameters=""), None
        )
        try:
            pass
        finally:
            service.Close(handle, None)

    def test_structure(self):
        service = ExternalDataReader()
        handle = service.Open(
            exd_api.Identifier(url=self._get_example_file_path("raw1.tdms"), parameters=""), None
        )
        try:
            structure = service.GetStructure(exd_api.StructureRequest(handle=handle), None)
            self.log.info(MessageToJson(structure))

            self.assertEqual(structure.name, "raw1.tdms")
            self.assertEqual(len(structure.groups), 1)
            self.assertEqual(structure.groups[0].number_of_rows, 2000)
            self.assertEqual(len(structure.groups[0].channels), 7)
            self.assertEqual(structure.groups[0].id, 0)
            self.assertEqual(structure.groups[0].channels[0].id, 0)
            self.assertEqual(structure.groups[0].channels[1].id, 1)
            self.assertEqual(structure.groups[0].channels[0].data_type, ods.DataTypeEnum.DT_DOUBLE)
            self.assertEqual(structure.groups[0].channels[1].data_type, ods.DataTypeEnum.DT_DOUBLE)
        finally:
            service.Close(handle, None)

    def test_get_values(self):
        service = ExternalDataReader()
        handle = service.Open(
            exd_api.Identifier(url=self._get_example_file_path("raw1.tdms"), parameters=""), None
        )
        try:
            values = service.GetValues(
                exd_api.ValuesRequest(handle=handle, group_id=0, channel_ids=[0, 1], start=0, limit=4), None
            )
            self.assertEqual(values.id, 0)
            self.assertEqual(len(values.channels), 2)
            self.assertEqual(values.channels[0].id, 0)
            self.assertEqual(values.channels[1].id, 1)
            self.log.info(MessageToJson(values))

            self.assertEqual(values.channels[0].values.data_type, ods.DataTypeEnum.DT_DOUBLE)
            self.assertSequenceEqual(
                values.channels[0].values.double_array.values,
                [-0.18402661214026306, 0.1480147709585864, -0.24506363109225746, -0.29725028229621264],
            )
            self.assertEqual(values.channels[1].values.data_type, ods.DataTypeEnum.DT_DOUBLE)
            self.assertSequenceEqual(
                values.channels[1].values.double_array.values,
                [1.0303048799096652, 0.6497390667439802, 0.7638782921842098, 0.5508590960417493],
            )

        finally:
            service.Close(handle, None)
