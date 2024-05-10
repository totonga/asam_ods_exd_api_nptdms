"""EXD API implementation for termite raw/dat files"""
import os
import numpy as np
from pathlib import Path
import re
import threading
from urllib.parse import urlparse

import grpc
import ods_pb2 as ods
import ods_external_data_pb2 as exd_api
import ods_external_data_pb2_grpc

from nptdms import TdmsFile

class ExternalDataReader(ods_external_data_pb2_grpc.ExternalDataReader):

    def Open(self, request, context):
        file_path = Path(self.__get_path(request.url))
        if not file_path.is_file():
            raise Exception(f'file "{request.url}" not accessible')

        connection_id = self.__open_file(request)

        rv = exd_api.Handle(uuid=connection_id)
        return rv

    def Close(self, request, context):
        self.__close_file(request)
        return exd_api.Empty()

    def GetStructure(self, request, context):

        if request.suppress_channels or request.suppress_attributes or 0 != len(request.channel_names):
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            context.set_details('Method not implemented!')
            raise NotImplementedError('Method not implemented!')

        identifier = self.connection_map[request.handle.uuid]
        file = self.__get_file(request.handle)

        rv = exd_api.StructureResult(identifier=identifier)
        rv.name = Path(identifier.url).name

        self.__add_attributes(file.properties, rv.attributes)

        for group_index, group in enumerate(file.groups(), start=0) :

            # Special about the TDMS format is that it allows different length channels in a single group.
            # So there needs to be some mapping for the ASAM EXD API which only allows a single 
            # `number_of_rows` for a single group.
            channels_by_len_dictionary = {}
            for channel_index, channel in enumerate(group.channels(), start=0):
                number_of_rows = self.__get_channel_length(channel)
                if number_of_rows not in channels_by_len_dictionary:
                    channels_by_len_dictionary[number_of_rows] = []
                channels_by_len_dictionary[number_of_rows].append({"channel": channel, "channel_id": channel_index})

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
                    new_channel.unit_string = channel.properties["unit_string"] if "unit_string" in channel.properties else ""
                    self.__add_attributes(channel.properties, new_channel.attributes)

                    new_group.channels.append(new_channel)

                rv.groups.append(new_group)
                group_sub_index += 1

        return rv

    def GetValues(self, request, context):

        file = self.__get_file(request.handle)
        group_id = request.group_id & 0xffffffff

        if group_id < 0 or group_id >= len(file.groups()):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f'Invalid group id {request.group_id}!')
            raise NotImplementedError(f'Invalid group id {request.group_id}!')

        group = file.groups()[group_id]

        nr_of_rows = self.__get_channel_length(group.channels()[request.channel_ids[0]])
        if request.start >= nr_of_rows:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f'Channel start index {request.start} out of range!')
            raise NotImplementedError(f'Channel start index {request.start} out of range!')

        end_index = request.start + request.limit
        if end_index >= nr_of_rows:
            end_index = nr_of_rows

        rv = exd_api.ValuesResult(id=request.group_id)
        for channel_id in request.channel_ids:
            if channel_id >= len(group.channels()):
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(f'Invalid channel id {channel_id}!')
                raise NotImplementedError(f'Invalid channel id {channel_id}!')

            channel = group.channels()[channel_id]
            ods_data_type = self.__get_datatype(channel.dtype)
            new_channel_values = exd_api.ValuesResult.ChannelValues()
            new_channel_values.id = channel_id
            new_channel_values.values.data_type = ods_data_type
            if   ods.DataTypeEnum.DT_BYTE == ods_data_type:
                new_channel_values.values.byte_array.values = channel[request.start:end_index].tobytes()
            elif ods.DataTypeEnum.DT_SHORT == ods_data_type:
                new_channel_values.values.long_array.values[:] = channel[request.start:end_index]
            elif ods.DataTypeEnum.DT_LONG == ods_data_type:
                new_channel_values.values.long_array.values[:] = channel[request.start:end_index]
            elif ods.DataTypeEnum.DT_LONGLONG == ods_data_type:
                new_channel_values.values.longlong_array.values[:] = channel[request.start:end_index]
            elif ods.DataTypeEnum.DT_FLOAT == ods_data_type:
                new_channel_values.values.float_array.values[:] = channel[request.start:end_index]
            elif ods.DataTypeEnum.DT_DOUBLE == ods_data_type:
                new_channel_values.values.double_array.values[:] = channel[request.start:end_index]
            elif ods.DataTypeEnum.DT_DATE == ods_data_type:
                datetime_values = channel[request.start:end_index]
                string_values = []
                for datetime_value in datetime_values:
                    string_values.append(self.__to_asam_ods_time(datetime_value))
                new_channel_values.values.string_array.values[:] = string_values
            elif ods.DataTypeEnum.DT_STRING == ods_data_type:
                new_channel_values.values.string_array.values[:] = channel[request.start:end_index]
            elif ods.DataTypeEnum.DT_COMPLEX == ods_data_type:
                complex_values = channel[request.start:end_index]
                real_values = []
                for complex_value in complex_values:
                    real_values.append(complex_value.real)
                    real_values.append(complex_value.imag)
                new_channel_values.values.float_array.values[:] = real_values
            elif ods.DataTypeEnum.DT_DCOMPLEX == ods_data_type:
                complex_values = channel[request.start:end_index]
                real_values = []
                for complex_value in complex_values:
                    real_values.append(complex_value.real)
                    real_values.append(complex_value.imag)
                new_channel_values.values.double_array.values[:] = real_values
            else:
                raise NotImplementedError(f'Not implemented channel type {ods_data_type}!')

            rv.channels.append(new_channel_values)

        return rv

    def GetValuesEx(self, request, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')
    
    
    def __to_asam_ods_time(self, datetime_value):
        return re.sub("[^0-9]", "", str(datetime_value))

    def __add_attributes(self, properties, attributes):
        for name, value in properties.items():
            if isinstance(value , str):
                attributes.variables[name].string_array.values.append(value)
            elif isinstance(value , float):
                attributes.variables[name].double_array.values.append(value)
            elif isinstance(value , int):
                attributes.variables[name].long_array.values.append(value)
            elif np.issubdtype('datetime64', value.dtype):
                attributes.variables[name].string_array.values.append(
                    self.__to_asam_ods_time(value))
            else:
                raise Exception(f'Attribute "{name}": "{value}" not assignable')

    def __get_channel_length(self, channel):
        return len(channel)

    def __get_datatype(self, data_type):
        if   np.issubdtype(data_type, np.complex64  ):
            return ods.DataTypeEnum.DT_COMPLEX
        elif np.issubdtype(data_type, np.complex128 ):
            return ods.DataTypeEnum.DT_DCOMPLEX
        elif np.issubdtype(data_type, np.int8       ):
            return ods.DataTypeEnum.DT_SHORT
        elif np.issubdtype(data_type, np.uint8      ):
            return ods.DataTypeEnum.DT_BYTE
        elif np.issubdtype(data_type, np.int16      ):
            return ods.DataTypeEnum.DT_SHORT
        elif np.issubdtype(data_type, np.uint16     ):
            return ods.DataTypeEnum.DT_LONG
        elif np.issubdtype(data_type, np.int32      ):
            return ods.DataTypeEnum.DT_LONG
        elif np.issubdtype(data_type, np.uint32     ):
            return ods.DataTypeEnum.DT_LONGLONG
        elif np.issubdtype(data_type, np.int64      ):
            return ods.DataTypeEnum.DT_LONGLONG
        elif np.issubdtype(data_type, np.uint64     ):
            return ods.DataTypeEnum.DT_DOUBLE
        elif np.issubdtype(data_type, np.datetime64 ):
            return ods.DataTypeEnum.DT_DATE
        elif np.issubdtype(data_type, np.float32    ):
            return ods.DataTypeEnum.DT_FLOAT
        elif np.issubdtype(data_type, np.float64    ):
            return ods.DataTypeEnum.DT_DOUBLE
        elif np.issubdtype(data_type, object):
            return ods.DataTypeEnum.DT_STRING
        raise NotImplementedError(f'Unknown type {data_type}!')


    def __init__(self):
        self.connect_count = 0
        self.connection_map = {}
        self.file_map = {}
        self.lock = threading.Lock()

    def __get_id(self, identifier):
        self.connect_count = self.connect_count + 1
        rv = str(self.connect_count)
        self.connection_map[rv] = identifier
        return rv

    def __get_path(self, file_url):
        p = urlparse(file_url)
        final_path = os.path.abspath(os.path.join(p.netloc, p.path))
        return final_path

    def __open_file(self, identifier):
        with self.lock:
            identifier.parameters
            connection_id = self.__get_id(identifier)
            connection_url = self.__get_path(identifier.url)
            if connection_url not in self.file_map:
                file_handle = TdmsFile.open(connection_url)
                self.file_map[connection_url] = { "file" : file_handle, "ref_count" : 0 }
            self.file_map[connection_url]["ref_count"] = self.file_map[connection_url]["ref_count"] + 1
            return connection_id

    def __get_file(self, handle):
        identifier = self.connection_map[handle.uuid]
        connection_url = self.__get_path(identifier.url)
        return self.file_map[connection_url]["file"]

    def __close_file(self, handle):
        with self.lock:
            identifier = self.connection_map[handle.uuid]
            connection_url = self.__get_path(identifier.url)
            if self.file_map[connection_url]["ref_count"] > 1:
                self.file_map[connection_url]["ref_count"] = self.file_map[connection_url]["ref_count"] - 1
            else:
                self.file_map[connection_url]["file"].close()
                del self.file_map[connection_url]
