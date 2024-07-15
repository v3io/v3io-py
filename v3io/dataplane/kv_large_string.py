# Copyright 2024 Iguazio
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

import zlib

LARGE_STRING_MIN_SIZE = 60000

prefix = b"_v3io_large_string"


def is_large_bstring(attribute_value):
    return attribute_value[: len(prefix)] == prefix


def large_bstring_to_string(attribute_value):
    compressed_value = attribute_value[len(prefix) :]
    return zlib.decompress(compressed_value).decode("utf-8")


def string_to_large_bstring(attribute_value):
    bvalue = zlib.compress(attribute_value.encode("utf-8"))
    return prefix + bvalue
