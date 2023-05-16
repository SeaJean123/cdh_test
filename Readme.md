## Introduction

The Core API allows to manage the AWS resources that keep the Cloud Data Hub running.
It is a traditional REST API using Python on AWS Lambda.

## Structure

### infrastructure
This folder contains all terraform code and necessary scripts.

### src
This folder contains all python code.

#### src/cdh_core
This is an installable package which contains shared code for all other components.
It can be installed via:
```
pip install git+ssh://git@github.com/bmw-cdh/cdh-core.git#egg=cdh-core&subdirectory=src/cdh_core
```

#### src/cdh_core_dev_tools
This is an installable package which contains code and scripts to develop, like unittests tools and dependency management.
This package is not needed as a runtime dependency in any of the AWS Lambda functions.
```
pip install git+ssh://git@github.com/bmw-cdh/cdh-core.git#egg=cdh-core-dev-tools&subdirectory=src/cdh_core_dev_tools
```

#### src/lambdas
This folder contains all AWS lambdas. These are not installable python packages except for `cdh_core_api` and `cdh_billing`.
The code should be deployed via terraform.

#### src/functional_tests
This folder contains code for our functional tests.

#### src/cdh_applications
This is an installable package which contains applications, such as scripts for the cleanup of prefix environments and for the execution of our functional tests.

## Local Setup
### Brew
If not already installed, get the package manager here: https://brew.sh/
``` shell script
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
```

### Python
Install [pyenv](https://github.com/pyenv/pyenv) and [pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv).
``` shell
brew install pyenv
brew install pyenv-virtualenv
```
Make sure to properly configure your terminal so that `which python` points to some `pyenv` shim file.
Probably you need to add the following two lines to your `~/.bashrc`/`~/.zshrc` file:
``` shell
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"
```

Activate your terminal config:
```source ~/.zshrc``` or ```source ~/.bashrc```

In the project root folder execute:
``` shell
make clean_setup
```

**Attention:** If you install the packages first per editable installation and install the requirements.txt after, the previously installed packages are removed and installed as none editable.

### Docker
Terraform is per default using docker to build python dependency layer. Therefore if you want to deploy manually using
local terraform, docker needs to be installed.

### Terraform
Install terraform on your machine (we recommend using [tfenv](https://github.com/tfutils/tfenv)).
It will automatically use the correct terraform version (using the _.terraform-version_ files in the repository).
``` shell script
brew install tfenv
tfenv install
terraform --version
  ```

## Code Quality Checks

We use [black](https://black.readthedocs.io/en/stable/) as auto-formatter.
Make sure to always format your code e.g. with `black -l 120` for Python and `terraform fmt --recursive` for TerraForm.
Linting is done via `flake8`.
Our type checker can be run with `pre-commit run mypy -a` (see next step).

To setup a shared [pre-commit](https://pre-commit.com/) hook (configured via _.pre-commit-config.yaml_), just run
```
pre-commit install
pre-commit install --hook-type commit-msg
pre-commit install --hook-type prepare-commit-msg
```
in this repository.

### disallowed-words-check

The disallowed-words-check hook helps developers to make sure that no sensitive information is accidentally leaked.
It does so by checking that no words from a list of disallowed words are committed. Since the list of
such words is specific to the development team, it is not publicly available and the hook is only run locally.
For further information about the hook and how to configure your own list, visit the repository
[disallowed-words-check](https://github.com/bmw-cdh/disallowed-words-check).

## Dependency management
We use pip-compile to pin our python dependencies.
The overall goal is to have `src/requirements.in` which describes the combined runtime requirements and `src/requirements-dev.in` for development purposes.
The corresponding `src/requirements.txt` and `src/requirements-dev.txt` contain the pinned versions of all dependencies.
`src/cdh_core` and `src/cdh_core_dev_tools` do not use requirements.in files and do not pin their requirements if possible, because they are libraries.

When a new dependency is needed, or the existing ones have to be updated, the following script has to be executed (this takes a few minutes):
```
src/cdh_core_dev_tools/cdh_core_dev_tools/dependencies/lock_dependencies.py
```

The different cdh_core packages do not reference each other in the requirement.txt files if they depend on each other.
Therefore, the cdh_core* dependencies have to be resolved manually.
Those are the internal dependencies ('a -> b' means a requires b):
- lambdas/cdh_core_api -> cdh_core

## Adding a new Lambda

- Place the code in `src/lambdas/<new fancy package name>`.
- Make sure there is a `__init__.py` in every folder.
- Add the package name to the `setup.cfg` to every place which is labeled with `# add new lambdas here`.
- Add the folder to the pytest matrix in: `.github/workflows/pull_requests.yml`.
- If the lambda has any dependencies it requires a  `requirements.in` file, which contains `-c ../../requirements.txt` and maybe `file:src/cdh_core`.
- Add the lambdas `requirements.in` file to the `src/requirements.in` with `-r` and update the dependencies.

## Deployment
Every push to the main branch automatically starts our test deployment via the GitHub 'Deployment' workflow.
As configured in `infrastructure/cdh-oss.bmw.cloud/cdh-core-config-test-deployment.yaml`, this deploys our DEV and PROD
environment.
For both environments, we deploy into the `api`, `test` and `resources` accounts. For PROD, we additionally deploy into the `security` account.

### Prefix Deployment
In addition to the "standard" (prefixless) dev infrastructure, the dev accounts (api, resources, test) contain several "prefixed" versions of our infrastructure.
All resources created by these instances use a given prefix, e.g. 'cdhx007', to separate them from other instances.

To test new features on a real deployment before merging them to master, create your own prefix branch by following the steps in the next section.
The API URL for such a deployment is `https://<your-prefix>.api-dev.cdh-oss.bmw.cloud`

#### Prefix Creation
You can set up a custom deployment for development purposes by using a "prefix deployment".
- Choose a prefix name. Allowed are any names except for 'cdh' or names starting with business object and hub values.
  Since all deployed AWS resources will have a prefixed name, avoid using long names.
  A suggested best practice is to use prefixes starting `cdhx` followed by three characters, such as `cdhx007`.
- Create a new branch `prefix/<your-prefix>`, e.g. `prefix/cdhx007`.
- Push to the new branch. This will trigger the GitHub 'Deployment' workflow that deploys your prefix infrastructure and executes our functional test suite.

### Cleanup of Prefix Resources
During each run of your prefix pipeline, it will remove all resources with your prefix from the DEV environment.
This is done so that the functional tests at the end of the pipeline always start on a clean environment.

### Prefix Deletion
To cleanup and remove an existing prefix branch, follow these steps:
- Manually trigger the GitHub action 'Destroy prefix resources'. For this, use the workflow definition from the prefix branch to be removed.
- Remove the git branch.

## Tests

Depending on the type of test that is executed, the environment variable **CDH_CORE_CONFIG_FILE_PATH** has to point to a
different configuration file:
- When executing unit tests, it should point to the top-level file `cdh-core-config.yaml`. This contains a mock configuration.
- When executing functional tests, it should point to the config file in our test-deployment folder:
  `infrastructure/cdh-oss.bmw.cloud/cdh-core-config-test-deployment.yaml`

### Unit tests

To run all unit tests, execute
```
pytest -n auto -v
```
in the main folder. It is also possible to just use `pytest` directly,
so you can for example run all tests in a specific directory by switching into the directory and executing `pytest`.
Use `pytest -k <name>` to run only tests including `<name>` in the test or class name.

### Functional tests

We have four different sets of functional tests that are all located in separate folders under `src/functional_tests`:
1. `mutating_basic` tests (run on environments where cleanup is enabled as data is modified):
   - `test_prerequisite.py` is used to initialize core_api so that other tests are working (eg. register accounts).
   - `test_basic.py` is used to fully check individual REST endpoints.
   - `test_business_cases.py` is used to check most important business cases of CDH on a user perspective.

2. `mutating_integration` tests (run on integrated environments for which the flag `is_test_environment` is set to true):
   - `test_basic.py` is used for integration checks with external APIs such as the Auth and Users APIs. Data is modified, but cleaned up using context managers. In case of failure manual cleanup might be necessary.

3. `non_mutating_basic` tests (run on all environments):
   - `test_basic.py` is used to check static/info endpoints (e.g. config, api-info endpoints) and OPTIONS calls to bulk endpoints.

4. `non_mutating_known_data` tests (run on environments that contain test data):
   - There is one test file per endpoint (`accounts`, `business_objects`, `datasets`, `resources`, `stats`). They cover basic checks of functionality without creating and modifying any data.

During debugging it is often useful to execute functional tests locally.
For this you need to assume the `cdh-deployer` role of one of the environment's configured test accounts, set the required environment variables (see below) and run:
```
python -m cdh_applications.functional_tests
```

The required environment variables are
- `BASE_URL`
- `RESOURCE_NAME_PREFIX`
- `ENVIRONMENT`
- `CLEANUP_PREFIX_DEPLOYMENT`: Setting this to true enables `mutating_basic` tests
- `IS_INTEGRATED_DEPLOYMENT`: Setting this to true, for an `ENVIRONMENT` with `is_test_environment=true`, enables `mutating_integration` tests
- `CONTAINS_TEST_DATA`: Setting this to true enables `non_mutating_known_data` tests

The tests can also be executed separately via pytest (e.g. `pytest src/functional_tests/${chosen test file} -k ${chosen test}`).
For this, you only need to set the environment variables `BASE_URL`, `RESOURCE_NAME_PREFIX` and `ENVIRONMENT`.

### Infrastructure validation tests

We have additional tests located under `infrastructure/bin` that validate our test-deployment infrastructure.
They can be executed using `pytest <file>`.
- `validate_cdh_deployment_test.py` checks that the infrastructure files and openapi spec are up-to-date. To execute it,
  **CDH_CORE_CONFIG_FILE_PATH** has to point to `infrastructure/cdh-oss.bmw.cloud/cdh-core-config-test-deployment.yaml`.
- `validate_create_cdh_compatibility_test.py` checks that the infrastructure is compatible with the [create-cdh scripts](https://github.com/bmw-cdh/cdh).
  To execute it, **CDH_CORE_CONFIG_FILE_PATH** has to point to the mock config `infrastructure/bin/create-cdh-compatibility-test-config.yaml`.

### Generating openapi spec
To generate the `infrastructure/cdh-oss.bmw.cloud/openapi.yml` spec according to our endpoints, run `cdh-core-oss/src/lambdas/cdh_core_api/cdh_core_api/create_openapi_spec.py`.
To generate a new file on the right location you can use the input parameters `--store` and `--path`.
- For example: `python create_openapi_spec.py --store --path infrastructure/cdh-oss.bmw.cloud/openapi.yml`
The `openapi.yml` file is not automatically generated by the pipeline. It has to be generated manually and committed to the repository.

Note: Only endpoints that are imported on `src/cdh_core/cdh_core/entities/__init__.py` and decorated will be included in the openapi spec.
```

### Manually trying out an endpoint

To manually try out an endpoint, our repository includes a python runner (`src/lambdas/cdh_core_api/cdh_core_api/examples/run.py`)
and some sample request bodies (json files under `src/lambdas/cdh_core_api/cdh_core_api/examples/`). To use them,
1. Fetch credentials for an account with the right to execute the API (e.g. the API account)
   and set the corresponding environment variable for AWS, `AWS_PROFILE`
2. Run the script with an example, also setting the required flags for the region, stage and base URL. e.g.
    ```
    python run.py --region "eu-west-1" --stage prod --base-url https://api.cdh-oss.bmw.cloud get_accounts.py
    ```

## Contributing

### Commit Signing
All commits must be signed off (`git commit -s`).
By signing off a commit, you state that you have the right to submit this work under our license and agree to the [Developer Certificate of Origin](https://developercertificate.org/).

### Merge order
Since we only allow fast-forward merges into our main branch, every branch needs to be rebased on main before being able to merge.
To reduce the number of rebases per pull request, our way of working is to create a merge queue.
We do so by assigning priorities to a PR once it has been approved and its merge checks have run successfully.
For example, if currently the highest number assigned to an open PR is "2", change the title of your PR to "<your title> [3]".
