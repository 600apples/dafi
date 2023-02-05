# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: messager.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\x0emessager.proto"\x07\n\x05\x45mpty"Z\n\x07Message\x12\x1e\n\trpc_realm\x18\x01 \x01(\x0b\x32\t.RpcRealmH\x00\x12&\n\rservice_realm\x18\x02 \x01(\x0b\x32\r.ServiceRealmH\x00\x42\x07\n\x05realm"\xd6\x01\n\x08RpcRealm\x12\x1a\n\x04\x66lag\x18\x01 \x01(\x0e\x32\x0c.MessageFlag\x12\x0c\n\x04uuid\x18\x02 \x01(\r\x12\x13\n\x0btransmitter\x18\x03 \x01(\t\x12\x10\n\x08receiver\x18\x04 \x01(\t\x12\x11\n\tfunc_name\x18\x05 \x01(\t\x12\x0c\n\x04\x64\x61ta\x18\x06 \x01(\x0c\x12\x15\n\rreturn_result\x18\x07 \x01(\x08\x12\x17\n\x06period\x18\x08 \x01(\x0b\x32\x07.Period\x12\x0f\n\x07timeout\x18\t \x01(\x04\x12\x17\n\x0f\x63omplete_marker\x18\n \x01(\x08"\x86\x01\n\x0cServiceRealm\x12\x1a\n\x04\x66lag\x18\x01 \x01(\x0e\x32\x0c.MessageFlag\x12\x0c\n\x04uuid\x18\x02 \x01(\r\x12\x13\n\x0btransmitter\x18\x03 \x01(\t\x12\x10\n\x08receiver\x18\x04 \x01(\t\x12\x0c\n\x04\x64\x61ta\x18\x05 \x01(\x0c\x12\x17\n\x0f\x63omplete_marker\x18\x06 \x01(\x08""\n\x11IntervalCondition\x12\r\n\x05value\x18\x01 \x01(\x01" \n\x0f\x41tTimeCondition\x12\r\n\x05value\x18\x01 \x03(\x01"b\n\x06Period\x12&\n\x08interval\x18\x01 \x01(\x0b\x32\x12.IntervalConditionH\x00\x12#\n\x07\x61t_time\x18\x02 \x01(\x0b\x32\x10.AtTimeConditionH\x00\x42\x0b\n\tcondition*\xca\x02\n\x0bMessageFlag\x12\t\n\x05\x45MPTY\x10\x00\x12\r\n\tHANDSHAKE\x10\x01\x12\x0b\n\x07REQUEST\x10\x02\x12\x0b\n\x07SUCCESS\x10\x03\x12\x14\n\x10UPDATE_CALLBACKS\x10\x04\x12\x1c\n\x18UNABLE_TO_FIND_CANDIDATE\x10\x05\x12\x1a\n\x16UNABLE_TO_FIND_PROCESS\x10\x06\x12\x1f\n\x1bREMOTE_STOPPED_UNEXPECTEDLY\x10\x07\x12\x13\n\x0fSCHEDULER_ERROR\x10\x08\x12\x14\n\x10SCHEDULER_ACCEPT\x10\t\x12\r\n\tBROADCAST\x10\n\x12\x10\n\x0cSTOP_REQUEST\x10\x0b\x12\x0f\n\x0bINIT_STREAM\x10\x0c\x12\x10\n\x0cSTREAM_ERROR\x10\r\x12\x13\n\x0fSTREAM_THROTTLE\x10\x0e\x12\x12\n\x0eRECEIVER_ERROR\x10\x0f\x32\x92\x01\n\x0fMessagerService\x12%\n\x0b\x63ommunicate\x12\x08.Message\x1a\x08.Message(\x01\x30\x01\x12*\n\x14stream_to_controller\x12\x08.Message\x1a\x06.Empty(\x01\x12,\n\x16stream_from_controller\x12\x06.Empty\x1a\x08.Message0\x01\x62\x06proto3'
)

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, "messager_pb2", globals())
if _descriptor._USE_C_DESCRIPTORS == False:

    DESCRIPTOR._options = None
    _MESSAGEFLAG._serialized_start = 644
    _MESSAGEFLAG._serialized_end = 974
    _EMPTY._serialized_start = 18
    _EMPTY._serialized_end = 25
    _MESSAGE._serialized_start = 27
    _MESSAGE._serialized_end = 117
    _RPCREALM._serialized_start = 120
    _RPCREALM._serialized_end = 334
    _SERVICEREALM._serialized_start = 337
    _SERVICEREALM._serialized_end = 471
    _INTERVALCONDITION._serialized_start = 473
    _INTERVALCONDITION._serialized_end = 507
    _ATTIMECONDITION._serialized_start = 509
    _ATTIMECONDITION._serialized_end = 541
    _PERIOD._serialized_start = 543
    _PERIOD._serialized_end = 641
    _MESSAGERSERVICE._serialized_start = 977
    _MESSAGERSERVICE._serialized_end = 1123
# @@protoc_insertion_point(module_scope)
