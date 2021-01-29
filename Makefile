.PHONY: clean clean-test clean-pyc clean-build docs help gcloud-start-instance gcloud-start-agent gcloud-run-flow
.DEFAULT_GOAL := help

define BROWSER_PYSCRIPT
import os, webbrowser, sys

from urllib.request import pathname2url

webbrowser.open("file://" + pathname2url(os.path.abspath(sys.argv[1])))
endef
export BROWSER_PYSCRIPT

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

BROWSER := python -c "$$BROWSER_PYSCRIPT"
INSTANCE_NAME := nl-open-data-vm-2

gcloud-start-instance: # TODO: Change ssh line (and general setup?) to not use `amigalmail` as user
	gcloud compute instances start $(INSTANCE_NAME)
	sleep 30

gcloud-start-agent:
	gcloud compute ssh amitgalmail@nl-open-data-vm-2
	cd nl-open-data/
	poetry shell
	sleep 10
	prefect agent local start

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test: ## remove test and coverage artifacts
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache

lint: ## check style with flake8
	flake8 nl_open_data tests

test: ## run tests quickly with the default Python
	python setup.py test

test-all: ## run tests on every Python version with tox
	tox

coverage: ## check code coverage quickly with the default Python
	coverage run --source nl_open_data setup.py test
	coverage report -m
	coverage html
	$(BROWSER) htmlcov/index.html

pdoc: ## generate documentation with pdoc3
	pdoc --html --force -o docs/ nl_open_data

docs: ## generate Sphinx HTML documentation, including API docs
	rm -f docs/nl_open_data.rst
	rm -f docs/modules.rst
	sphinx-apidoc -o docs/ nl_open_data
	$(MAKE) -C docs clean
	$(MAKE) -C docs html
	$(BROWSER) docs/_build/html/index.html

pdoc:
	pdoc --html --output-dir ./docs --force nl_open_data

servedocs: docs ## compile the docs watching for changes
	watchmedo shell-command -p '*.rst' -c '$(MAKE) -C docs html' -R -D .

release: dist ## package and upload a release
	twine upload dist/*

dist: clean ## builds source and wheel package
	python setup.py sdist
	python setup.py bdist_wheel
	ls -l dist

install: clean ## install the package to the active Python's site-packages
	python setup.py install

poetry: ## generate setup.py, environment.yml and requirements.txt from poetry
	dephell deps convert
	dephell deps convert --env requirements
	dephell deps convert --env conda

# Redundant for CBS. Maybe for the rest?
bq-datasets:
	bq --location=EU mk -d --description "Data from CBS Statline" cbs
	bq --location=EU mk -d --description "Data from Kadaster" kadaster
	bq --location=EU mk -d --description "Data from Vektis" vektis
