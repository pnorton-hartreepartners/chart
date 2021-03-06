# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: protobuf_hdq/hdq.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from protobuf_net import bcl_pb2 as protobuf__net_dot_bcl__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='protobuf_hdq/hdq.proto',
  package='',
  syntax='proto2',
  serialized_options=None,
  serialized_pb=_b('\n\x16protobuf_hdq/hdq.proto\x1a\x16protobuf_net/bcl.proto\"Q\n\x0cProtoBufData\x12\x0f\n\x07Headers\x18\x01 \x03(\t\x12 \n\x04\x44\x61ta\x18\x02 \x03(\x0b\x32\x12.ProtoBufDataPoint\x12\x0e\n\x06\x45rrors\x18\x03 \x03(\t\"v\n\x10ProtoBufDataList\x12\x0c\n\x04Keys\x18\x01 \x03(\t\x12\'\n\x10ProtoBufDataObjs\x18\x02 \x03(\x0b\x32\r.ProtoBufData\x12\x14\n\x0cInfoMessages\x18\x03 \x03(\t\x12\x15\n\rErrorMessages\x18\x04 \x03(\t\"P\n\x11ProtoBufDataPoint\x12\x0e\n\x06String\x18\x01 \x01(\t\x12\x1b\n\x04\x44\x61te\x18\x02 \x01(\x0b\x32\r.bcl.DateTime\x12\x0e\n\x06Number\x18\x03 \x01(\x01')
  ,
  dependencies=[protobuf__net_dot_bcl__pb2.DESCRIPTOR,])




_PROTOBUFDATA = _descriptor.Descriptor(
  name='ProtoBufData',
  full_name='ProtoBufData',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='Headers', full_name='ProtoBufData.Headers', index=0,
      number=1, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='Data', full_name='ProtoBufData.Data', index=1,
      number=2, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='Errors', full_name='ProtoBufData.Errors', index=2,
      number=3, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=50,
  serialized_end=131,
)


_PROTOBUFDATALIST = _descriptor.Descriptor(
  name='ProtoBufDataList',
  full_name='ProtoBufDataList',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='Keys', full_name='ProtoBufDataList.Keys', index=0,
      number=1, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='ProtoBufDataObjs', full_name='ProtoBufDataList.ProtoBufDataObjs', index=1,
      number=2, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='InfoMessages', full_name='ProtoBufDataList.InfoMessages', index=2,
      number=3, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='ErrorMessages', full_name='ProtoBufDataList.ErrorMessages', index=3,
      number=4, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=133,
  serialized_end=251,
)


_PROTOBUFDATAPOINT = _descriptor.Descriptor(
  name='ProtoBufDataPoint',
  full_name='ProtoBufDataPoint',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='String', full_name='ProtoBufDataPoint.String', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='Date', full_name='ProtoBufDataPoint.Date', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='Number', full_name='ProtoBufDataPoint.Number', index=2,
      number=3, type=1, cpp_type=5, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=253,
  serialized_end=333,
)

_PROTOBUFDATA.fields_by_name['Data'].message_type = _PROTOBUFDATAPOINT
_PROTOBUFDATALIST.fields_by_name['ProtoBufDataObjs'].message_type = _PROTOBUFDATA
_PROTOBUFDATAPOINT.fields_by_name['Date'].message_type = protobuf__net_dot_bcl__pb2._DATETIME
DESCRIPTOR.message_types_by_name['ProtoBufData'] = _PROTOBUFDATA
DESCRIPTOR.message_types_by_name['ProtoBufDataList'] = _PROTOBUFDATALIST
DESCRIPTOR.message_types_by_name['ProtoBufDataPoint'] = _PROTOBUFDATAPOINT
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

ProtoBufData = _reflection.GeneratedProtocolMessageType('ProtoBufData', (_message.Message,), {
  'DESCRIPTOR' : _PROTOBUFDATA,
  '__module__' : 'protobuf_hdq.hdq_pb2'
  # @@protoc_insertion_point(class_scope:ProtoBufData)
  })
_sym_db.RegisterMessage(ProtoBufData)

ProtoBufDataList = _reflection.GeneratedProtocolMessageType('ProtoBufDataList', (_message.Message,), {
  'DESCRIPTOR' : _PROTOBUFDATALIST,
  '__module__' : 'protobuf_hdq.hdq_pb2'
  # @@protoc_insertion_point(class_scope:ProtoBufDataList)
  })
_sym_db.RegisterMessage(ProtoBufDataList)

ProtoBufDataPoint = _reflection.GeneratedProtocolMessageType('ProtoBufDataPoint', (_message.Message,), {
  'DESCRIPTOR' : _PROTOBUFDATAPOINT,
  '__module__' : 'protobuf_hdq.hdq_pb2'
  # @@protoc_insertion_point(class_scope:ProtoBufDataPoint)
  })
_sym_db.RegisterMessage(ProtoBufDataPoint)


# @@protoc_insertion_point(module_scope)
