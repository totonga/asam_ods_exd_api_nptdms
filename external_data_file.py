"""Interface for handling file access."""

from __future__ import annotations

import re
from typing import Any, override

import numpy as np
from nptdms import TdmsFile

from ods_exd_api_box import ExdFileInterface, exd_api, ods


class ExternalDataFile(ExdFileInterface):
    """Class for handling for NI tdms files."""

    @classmethod
    @override
    def create(cls, file_path: str, parameters: str) -> ExdFileInterface:
        """Factory method to create a file handler instance."""
        return cls(file_path, parameters)

    @override
    def __init__(self, file_path: str, parameters: str = ""):
        self.file_path = file_path
        self.parameters = parameters
        self.tdms_file = TdmsFile.open(file_path)

    @override
    def close(self):
        """Close the external data file."""
        self.tdms_file.close()

    @override
    def fill_structure(self, structure: exd_api.StructureResult) -> None:
        """Fill the structure of the external data file."""
        file = self.tdms_file

        self.__add_attributes(file.properties, structure.attributes)

        for group_index, group in enumerate(file.groups(), start=0):

            # Special about the TDMS format is that it allows different length channels in a single group.
            # So there needs to be some mapping for the ASAM EXD API which only allows a single
            # `number_of_rows` for a single group.
            channels_by_len_dictionary: dict[int, list[dict[str, Any]]] = {}
            for channel_index, channel in enumerate(group.channels(), start=0):
                number_of_rows = self.__get_channel_length(channel)
                if number_of_rows not in channels_by_len_dictionary:
                    channels_by_len_dictionary[number_of_rows] = []
                channels_by_len_dictionary[number_of_rows].append(
                    {"channel": channel, "channel_id": channel_index}
                )

            group_sub_index = 0
            for number_of_rows, channels_by_len in channels_by_len_dictionary.items():

                new_group = exd_api.StructureResult.Group()
                new_group.name = group.name
                new_group.id = group_index | group_sub_index << 32
                new_group.total_number_of_channels = len(channels_by_len)
                new_group.number_of_rows = number_of_rows

                self.__add_attributes(group.properties, new_group.attributes)

                for channel_by_len in channels_by_len:
                    channel = channel_by_len["channel"]
                    new_channel = exd_api.StructureResult.Channel()
                    new_channel.name = channel.name
                    new_channel.id = channel_by_len["channel_id"]
                    new_channel.data_type = self.__get_datatype(channel.dtype)
                    new_channel.unit_string = (
                        channel.properties["unit_string"] if "unit_string" in channel.properties else ""
                    )
                    self.__add_attributes(
                        channel.properties, new_channel.attributes)

                    new_group.channels.append(new_channel)

                structure.groups.append(new_group)
                group_sub_index += 1

    @override
    def get_values(self, request: exd_api.ValuesRequest) -> exd_api.ValuesResult:
        """Get values from the external data file."""

        file = self.tdms_file
        group_id = request.group_id & 0xFFFFFFFF

        if group_id < 0 or group_id >= len(file.groups()):
            raise ValueError(f"Invalid group id {request.group_id}!")

        group = file.groups()[group_id]

        nr_of_rows = self.__get_channel_length(
            group.channels()[request.channel_ids[0]])
        if request.start >= nr_of_rows:
            raise ValueError(
                f"Channel start index {request.start} out of range! Greater equal than {nr_of_rows}."
            )

        end_index = request.start + request.limit
        if end_index >= nr_of_rows:
            end_index = nr_of_rows

        rv = exd_api.ValuesResult(id=request.group_id)
        for channel_id in request.channel_ids:
            if channel_id >= len(group.channels()):
                raise ValueError(f"Channel id {channel_id} out of range!")

            channel = group.channels()[channel_id]
            ods_data_type = self.__get_datatype(channel.dtype)
            new_channel_values = exd_api.ValuesResult.ChannelValues()
            new_channel_values.id = channel_id
            new_channel_values.values.data_type = ods_data_type
            if ods.DataTypeEnum.DT_BYTE == ods_data_type:
                new_channel_values.values.byte_array.values = channel[request.start: end_index].tobytes(
                )
            elif ods.DataTypeEnum.DT_SHORT == ods_data_type:
                new_channel_values.values.long_array.values[:
                                                            ] = channel[request.start: end_index]
            elif ods.DataTypeEnum.DT_LONG == ods_data_type:
                new_channel_values.values.long_array.values[:
                                                            ] = channel[request.start: end_index]
            elif ods.DataTypeEnum.DT_LONGLONG == ods_data_type:
                new_channel_values.values.longlong_array.values[:
                                                                ] = channel[request.start: end_index]
            elif ods.DataTypeEnum.DT_FLOAT == ods_data_type:
                new_channel_values.values.float_array.values[:
                                                             ] = channel[request.start: end_index]
            elif ods.DataTypeEnum.DT_DOUBLE == ods_data_type:
                new_channel_values.values.double_array.values[:
                                                              ] = channel[request.start: end_index]
            elif ods.DataTypeEnum.DT_DATE == ods_data_type:
                datetime_values = channel[request.start: end_index]
                string_values = []
                for datetime_value in datetime_values:
                    string_values.append(
                        self.__to_asam_ods_time(datetime_value))
                new_channel_values.values.string_array.values[:] = string_values
            elif ods.DataTypeEnum.DT_STRING == ods_data_type:
                new_channel_values.values.string_array.values[:
                                                              ] = channel[request.start: end_index]
            elif ods.DataTypeEnum.DT_COMPLEX == ods_data_type:
                complex_values = channel[request.start: end_index]
                real_values = []
                for complex_value in complex_values:
                    real_values.append(complex_value.real)
                    real_values.append(complex_value.imag)
                new_channel_values.values.float_array.values[:] = real_values
            elif ods.DataTypeEnum.DT_DCOMPLEX == ods_data_type:
                complex_values = channel[request.start: end_index]
                real_values = []
                for complex_value in complex_values:
                    real_values.append(complex_value.real)
                    real_values.append(complex_value.imag)
                new_channel_values.values.double_array.values[:] = real_values
            else:
                raise NotImplementedError(
                    f"Not implemented channel type {ods_data_type}!")

            rv.channels.append(new_channel_values)

        return rv

    def __to_asam_ods_time(self, datetime_value):
        return re.sub("[^0-9]", "", str(datetime_value))

    def __add_attributes(self, properties, attributes):
        for name, value in properties.items():
            if isinstance(value, str):
                attributes.variables[name].string_array.values.append(value)
            elif isinstance(value, float):
                attributes.variables[name].double_array.values.append(value)
            elif isinstance(value, int):
                attributes.variables[name].long_array.values.append(value)
            elif np.issubdtype("datetime64", value.dtype):
                attributes.variables[name].string_array.values.append(
                    self.__to_asam_ods_time(value))
            else:
                raise ValueError(
                    f'Attribute "{name}": "{value}" not assignable')

    def __get_channel_length(self, channel):
        return len(channel)

    def __get_datatype(self, data_type):
        if np.issubdtype(data_type, np.complex64):
            return ods.DataTypeEnum.DT_COMPLEX
        elif np.issubdtype(data_type, np.complex128):
            return ods.DataTypeEnum.DT_DCOMPLEX
        elif np.issubdtype(data_type, np.int8):
            return ods.DataTypeEnum.DT_SHORT
        elif np.issubdtype(data_type, np.uint8):
            return ods.DataTypeEnum.DT_BYTE
        elif np.issubdtype(data_type, np.int16):
            return ods.DataTypeEnum.DT_SHORT
        elif np.issubdtype(data_type, np.uint16):
            return ods.DataTypeEnum.DT_LONG
        elif np.issubdtype(data_type, np.int32):
            return ods.DataTypeEnum.DT_LONG
        elif np.issubdtype(data_type, np.uint32):
            return ods.DataTypeEnum.DT_LONGLONG
        elif np.issubdtype(data_type, np.int64):
            return ods.DataTypeEnum.DT_LONGLONG
        elif np.issubdtype(data_type, np.uint64):
            return ods.DataTypeEnum.DT_DOUBLE
        elif np.issubdtype(data_type, np.datetime64):
            return ods.DataTypeEnum.DT_DATE
        elif np.issubdtype(data_type, np.float32):
            return ods.DataTypeEnum.DT_FLOAT
        elif np.issubdtype(data_type, np.float64):
            return ods.DataTypeEnum.DT_DOUBLE
        elif np.issubdtype(data_type, object):
            return ods.DataTypeEnum.DT_STRING
        raise NotImplementedError(f"Unknown type {data_type}!")


if __name__ == "__main__":

    from ods_exd_api_box import serve_plugin

    serve_plugin(
        file_type_name="TDMS", file_type_factory=ExternalDataFile.create, file_type_file_patterns=["*.tdms"]
    )
