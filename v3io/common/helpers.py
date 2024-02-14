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


def url_join(*parts):
    result = ""
    slash_suffix = False
    for part_index, part in enumerate(parts):
        if part == "":
            continue
        # add slash prefix before part if:
        # 1. slash suffix did not exit in prev part
        # 2. slash prefix does not exit in this part
        # 3. part is not the first
        if not slash_suffix and part[0] != "/" and part_index != 0:
            result += "/" + part
        else:
            # if slash suffix existed in prev trim slash prefix from this part
            result += part if not slash_suffix else part.lstrip("/")
        slash_suffix = True if part[-1] == "/" else False
    return result
