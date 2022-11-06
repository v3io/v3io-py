# Copyright 2019 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import array
import base64
import datetime
import os

import future.utils

try:
    from urllib.parse import quote, urlencode
except BaseException:
    from urllib import urlencode, quote

import ujson

import v3io.common.helpers
import v3io.dataplane.kv_array
import v3io.dataplane.kv_timestamp

#
# Request
#


class Request(object):
    def __init__(self, container, access_key, raise_for_status, encoder, encoder_args, output=None):
        self.container = container
        self.access_key = access_key
        self.raise_for_status = raise_for_status
        self.encoder = encoder
        self.encoder_args = encoder_args
        self.output = output

        # get request params with the encoder
        self.method, self.path, self.query, self.headers, self.body = encoder(container, access_key, encoder_args)

        # used by the transport
        self.transport = lambda: None

    def encode_path(self):
        if self.query is None:
            return quote(self.path)

        return quote(self.path) + "?" + urlencode(self.query, quote_via=quote)


#
# Encoders
#


#
# Container
#


def encode_get_containers(container_name, access_key, kwargs):
    return _encode("GET", "/", access_key, None, None, {}, None)


def encode_get_container_contents(container_name, access_key, kwargs):
    query = {"prefix": kwargs["path"]}

    if kwargs["get_all_attributes"]:
        query["prefix-info"] = 1

    if kwargs["directories_only"]:
        query["prefix-only"] = 1

    if kwargs["limit"] is not None:
        query["max-keys"] = kwargs["limit"]

    if kwargs["marker"]:
        query["marker"] = kwargs["marker"]

    return _encode("GET", "/" + container_name, access_key, None, query, {}, None)


#
# Object
#


def encode_head_object(container_name, access_key, kwargs):
    return _encode("HEAD", container_name, access_key, kwargs["path"], None, None, None)


def encode_get_object(container_name, access_key, kwargs):
    headers = None

    offset = kwargs.get("offset")

    # if the append flag is passed, add a range header
    if offset:
        range_value = "bytes=" + str(offset)

        num_bytes = kwargs.get("num_bytes")
        if num_bytes:
            range_value += "-" + str(offset + num_bytes - 1)

        headers = {"Range": range_value}

    return _encode("GET", container_name, access_key, kwargs["path"], None, headers, None)


def encode_put_object(container_name, access_key, kwargs):
    headers = None

    # if the append flag is passed, add a range header
    if kwargs["append"]:
        headers = {"Range": "-1"}

    return _encode("PUT", container_name, access_key, kwargs["path"], None, headers, kwargs["body"])


def encode_delete_object(container_name, access_key, kwargs):
    return _encode("DELETE", container_name, access_key, kwargs["path"], None, None, None)


#
# KV
#


def encode_put_item(container_name, access_key, kwargs):
    # add 'Item' to body
    body = {"Item": _dict_to_typed_attributes(kwargs["attributes"])}

    if kwargs["condition"] is not None:
        body["ConditionExpression"] = kwargs["condition"]

    return _encode(
        "PUT",
        container_name,
        access_key,
        kwargs.get("path") or os.path.join(kwargs["table_path"], kwargs["key"]),
        None,
        {"X-v3io-function": "PutItem"},
        body,
    )


def encode_update_item(container_name, access_key, kwargs):
    body = {"UpdateMode": kwargs.get("update_mode") or "CreateOrReplaceAttributes"}

    if kwargs["condition"] is not None:
        body["ConditionExpression"] = kwargs["condition"]

    if not kwargs["expression"] and not kwargs["attributes"]:
        raise RuntimeError("One of expression or attributes must be populated for update item")

    if kwargs["expression"]:
        http_method = "POST"
        function_name = "UpdateItem"
        body["UpdateExpression"] = kwargs["expression"]

    if kwargs["alternate_expression"]:
        http_method = "POST"
        function_name = "UpdateItem"
        body["AlternateUpdateExpression"] = kwargs["alternate_expression"]

    elif kwargs["attributes"]:
        http_method = "PUT"
        function_name = "PutItem"
        body["Item"] = _dict_to_typed_attributes(kwargs["attributes"])

    return _encode(
        http_method,
        container_name,
        access_key,
        kwargs.get("path") or os.path.join(kwargs["table_path"], kwargs["key"]),
        None,
        {"X-v3io-function": function_name},
        body,
    )


def encode_get_item(container_name, access_key, kwargs):
    body = {"AttributesToGet": ",".join(kwargs["attribute_names"])}

    return _encode(
        "PUT",
        container_name,
        access_key,
        kwargs.get("path") or os.path.join(kwargs["table_path"], kwargs["key"]),
        None,
        {"X-v3io-function": "GetItem"},
        body,
    )


def encode_get_items(container_name, access_key, kwargs):
    body = {
        "AttributesToGet": ",".join(kwargs["attribute_names"]),
    }

    if kwargs.get("table_name"):
        body["TableName"] = kwargs["table_name"]

    if kwargs["filter_expression"]:
        body["FilterExpression"] = kwargs["filter_expression"]

    if kwargs["marker"]:
        body["Marker"] = kwargs["marker"]

    if kwargs["sharding_key"]:
        body["ShardingKey"] = kwargs["sharding_key"]

    if kwargs["limit"] is not None:
        body["Limit"] = kwargs["limit"]

    if kwargs["segment"] is not None:
        body["Segment"] = kwargs["segment"]

    if kwargs["total_segments"] is not None:
        body["TotalSegment"] = kwargs["total_segments"]

    if kwargs["sort_key_range_start"]:
        body["SortKeyRangeStart"] = kwargs["sort_key_range_start"]

    if kwargs["sort_key_range_end"]:
        body["SortKeyRangeEnd"] = kwargs["sort_key_range_end"]

    return _encode(
        "PUT",
        container_name,
        access_key,
        kwargs.get("path") or kwargs["table_path"],
        None,
        {"X-v3io-function": "GetItems"},
        body,
    )


#
# Stream
#


def encode_create_stream(container_name, access_key, kwargs):
    body = {"ShardCount": kwargs["shard_count"], "RetentionPeriodHours": kwargs.get("retention_period_hours") or 24}

    return _encode(
        "POST",
        container_name,
        access_key,
        kwargs.get("path") or kwargs["stream_path"],
        None,
        {"X-v3io-function": "CreateStream"},
        body,
    )


def encode_update_stream(container_name, access_key, kwargs):
    body = {
        "ShardCount": kwargs["shard_count"],
    }

    return _encode(
        "POST",
        container_name,
        access_key,
        kwargs.get("path") or kwargs["stream_path"],
        None,
        {"X-v3io-function": "UpdateStream"},
        body,
    )


def encode_describe_stream(container_name, access_key, kwargs):
    return _encode(
        "PUT",
        container_name,
        access_key,
        kwargs.get("path") or kwargs["stream_path"],
        None,
        {"X-v3io-function": "DescribeStream"},
        None,
    )


def encode_seek_shard(container_name, access_key, kwargs):
    body = {
        "Type": kwargs["seek_type"],
    }

    if kwargs["seek_type"] == "SEQUENCE":
        body["StartingSequenceNumber"] = kwargs["starting_sequence_number"]
    elif kwargs["seek_type"] == "TIME":
        body["TimestampSec"] = kwargs["timestamp_sec"]
        body["TimestampNSec"] = kwargs["timestamp_nsec"]
    elif kwargs["seek_type"] not in ["EARLIEST", "LATEST"]:
        raise ValueError(
            "Unsupported seek_type ({0}) for seek_shard. Must be one of SEQUENCE, TIME, EARLIEST, LATEST".format(
                kwargs["seek_type"]
            )
        )

    return _encode(
        "PUT",
        container_name,
        access_key,
        kwargs.get("path") or kwargs["stream_path"],
        None,
        {"X-v3io-function": "SeekShard"},
        body,
    )


def encode_put_records(container_name, access_key, kwargs):
    records = []

    for record in kwargs["records"]:
        record_body = {
            "Data": _to_base64(record["data"]),
        }

        try:
            record_body["ClientInfo"] = _to_base64(record["client_info"])
        except KeyError:
            pass

        try:
            record_body["ShardId"] = record["shard_id"]
        except KeyError:
            pass

        try:
            record_body["PartitionKey"] = record["partition_key"]
        except KeyError:
            pass

        records.append(record_body)

    body = {"Records": records}

    return _encode(
        "POST",
        container_name,
        access_key,
        kwargs.get("path") or kwargs["stream_path"],
        None,
        {"X-v3io-function": "PutRecords"},
        body,
    )


def encode_get_records(container_name, access_key, kwargs):
    body = {
        "Location": kwargs["location"],
    }

    limit = kwargs.get("limit")
    if limit:
        body["Limit"] = limit

    return _encode(
        "PUT",
        container_name,
        access_key,
        kwargs.get("path") or kwargs["stream_path"],
        None,
        {"X-v3io-function": "GetRecords"},
        body,
    )


#
# Helpers
#


def _encode(method, container_name, access_key, path, query, headers, body):
    if path is not None:
        path = v3io.common.helpers.url_join(container_name, path)
    else:
        path = container_name

    headers, body = _resolve_body_and_headers(access_key, headers, body)

    return method, path, query, headers, body


def _typed_attributes_to_dict(self):
    pass


def _ensure_trailing_slash(path):
    if not path.endswith("/"):
        return path + "/"

    return path


def _to_base64(input):
    if isinstance(input, str):
        input = input.encode("utf-8")

    return base64.b64encode(input)


def _dict_to_typed_attributes(d):
    typed_attributes = {}

    for (key, value) in future.utils.viewitems(d):
        attribute_type = type(value)
        type_value = None

        if isinstance(value, future.utils.text_type):
            type_key = "S"
            type_value = value
        elif isinstance(value, future.utils.string_types):
            type_key = "S"
            type_value = str(value)
        elif attribute_type in [int, float]:
            type_key = "N"
            type_value = str(value)
        elif attribute_type in [bytes, bytearray]:
            type_key = "B"
            type_value = base64.b64encode(value)
        elif isinstance(value, bool):
            type_key = "BOOL"
            type_value = value
        elif isinstance(value, list):
            type_key = "B"
            type_value = v3io.dataplane.kv_array.encode_list(value)
        elif isinstance(value, array.array):
            type_key = "B"
            type_value = v3io.dataplane.kv_array.encode_array(value, value.typecode)
        elif isinstance(value, datetime.datetime):
            type_key = "TS"
            type_value = v3io.dataplane.kv_timestamp.encode(value)
        else:
            raise AttributeError("Attribute {0} has unsupported type {1}".format(key, attribute_type))

        typed_attributes[key] = {type_key: type_value}

    return typed_attributes


def _resolve_body_and_headers(access_key, headers, body):
    if access_key:
        headers = headers or {}
        headers["X-v3io-session-key"] = access_key

    if not isinstance(body, dict):
        return headers, body

    body = ujson.dumps(body, reject_bytes=False)
    headers["Content-Type"] = "application/json"

    return headers, body
