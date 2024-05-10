import sys, os
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/..")

import logging
import pathlib
import unittest

from nptdms import TdmsFile

class TestStringMethods(unittest.TestCase):
    log = logging.getLogger(__name__)

    def __get_example_file_path(self, file_name):
        example_file_path = pathlib.Path.joinpath(pathlib.Path(__file__).parent.resolve(), '..', 'data', file_name)
        return pathlib.Path(example_file_path).resolve()

    def test_open(self):
        file_path = self.__get_example_file_path('raw1.tdms')
        self.assertTrue(os.path.isfile(file_path))
        self.log.info(str(file_path).encode('utf-8'))

        with TdmsFile.open(file_path) as tdms_file:
            self.log.info(f"{tdms_file.properties["name"]}")
            for name, value in tdms_file.properties.items():
                self.log.info("f: {0}: {1}".format(name, value))

            groups = tdms_file.groups()
            for group in groups:
                self.log.info(f" {group.name}")
                for name, value in group.properties.items():
                    self.log.info(" g: {0}: {1}".format(name, value))

                channels = group.channels()
                for channel in channels:
                    unit_string = channel.properties["unit_string"]
                    self.log.info(f"  {channel.name} - {channel.dtype}")
                    for name, value in channel.properties.items():
                        self.log.info("  c: {0}: {1}".format(name, value))
                    self.log.info(f"   l: {len(channel)}")
                    self.log.info(f"   d: {channel[0:4]}")

    def test_open_meta(self):
        file_path = self.__get_example_file_path('raw1.tdms')
        self.assertTrue(os.path.isfile(file_path))
        self.log.info(str(file_path).encode('utf-8'))

        with TdmsFile.read_metadata(file_path) as tdms_file:
            self.log.info(f"{tdms_file.properties["name"]}")
            for name, value in tdms_file.properties.items():
                self.log.info("f: {0}: {1}".format(name, value))

            groups = tdms_file.groups()
            for group in groups:
                self.log.info(f" {group.name}")
                for name, value in group.properties.items():
                    self.log.info(" g: {0}: {1}".format(name, value))

                channels = group.channels()
                for channel in channels:
                    unit_string = channel.properties["unit_string"]
                    self.log.info(f"  {channel.name} - {channel.dtype}")
                    for name, value in channel.properties.items():
                        self.log.info("  c: {0}: {1}".format(name, value))
                        self.log.info(f"   l: {len(channel)}")


if __name__ == '__main__':
    unittest.main()