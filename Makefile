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

.PHONY: flask8
flake8:
	PIPENV_IGNORE_VIRTUALENVS=1 \
	    pipenv run flake8 \
	    	v3io

.PHONY: test
test: clean_pyc flake8
	PIPENV_IGNORE_VIRTUALENVS=1 \
	    pipenv run python -m pytest -v \
		--disable-warnings \
		--benchmark-disable \
		tests

.PHONY: update-deps
update-deps:
	PIPENV_IGNORE_VIRTUALENVS=1 \
	    pipenv update

.PHONY: sync-deps
sync-deps:
	PIPENV_IGNORE_VIRTUALENVS=1 \
	    pipenv sync --dev
