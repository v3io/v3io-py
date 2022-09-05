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
#
.PHONY: all
all:
	$(error please pick a target)

.PHONY: upload
upload:
	pipenv run python pypi_upload.py

.PHONY: debug-upload
debug-upload:
	pipenv run python pypi_upload.py -t

.PHONY: clean_pyc
clean_pyc:
	find . -name '*.pyc' -exec rm {} \;

.PHONY: lint
lint:
	PIPENV_IGNORE_VIRTUALENVS=1 \
	    pipenv run flake8 v3io

.PHONY: test
test: clean_pyc
	PIPENV_IGNORE_VIRTUALENVS=1 \
	    pipenv run python -m unittest \
		tests/test_*

.PHONY: update-deps
update-deps:
	PIPENV_IGNORE_VIRTUALENVS=1 \
	    pipenv update

.PHONY: sync-deps
sync-deps:
	PIPENV_IGNORE_VIRTUALENVS=1 \
	    pipenv sync --dev
