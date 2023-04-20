# Copyright 2023 Iguazio
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
import unittest

from v3io.common.helpers import url_join


class Test(unittest.TestCase):
    def test_url_join(self):
        self.assertEqual(url_join("a", "b"), "a/b")  # add just exactly one "/" between parts
        self.assertEqual(url_join("/", "a", "b"), "/a/b")
        self.assertEqual(url_join("/", "a", "/b"), "/a/b")
        self.assertEqual(url_join("/", "/a", "b"), "/a/b")
        self.assertEqual(url_join("/", "/a", "/b"), "/a/b")
        self.assertEqual(url_join("/", "/a/", "b"), "/a/b")
        self.assertEqual(url_join("/", "/a/", "/b"), "/a/b")
        self.assertEqual(url_join("a", "b"), "a/b")  # keep suffix "/" exist/not-exist invariant
        self.assertEqual(url_join("a", "b/"), "a/b/")
        self.assertEqual(url_join("a", "b//"), "a/b//")
        self.assertEqual(url_join("a", "b//", "/"), "a/b//")  # suffix "/" count (if > 0) may change (but we don"t care)
