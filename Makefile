setup:
	pip install -r src/requirements.txt
	pip install -r src/requirements-dev.txt
	pip install -e src/cdh_core
	pip install -e src/cdh_core_dev_tools
	pip install -e src/lambdas/cdh_core_api

clean_setup:
	pyenv install -s $(shell cat .python-base-version)
	pyenv virtualenv-delete -f $(shell cat .python-version) || true
	pyenv virtualenv $(shell cat .python-base-version) $(shell cat .python-version)
	make setup
	pyenv version  # Sanity check: Inside your Core API folder, this should give "cdh-core"

lock_dependencies:
	python src/cdh_core_dev_tools/cdh_core_dev_tools/dependencies/lock_dependencies.py
