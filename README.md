# servicex-backend-tests

These are tests for the backends of IRIS-HEP's ServiceX. The intended functionality is to ensure that the xAOD and uproot transformers both correctly provide the same data.

# Installation and Execution

## Docker

Pull the tests using docker commands:

```
docker pull docker pull davidwliu/servicex-backend-tests:latest
```

Afterwards, run the image using this command, which takes three arguments:

```
docker run davidwliu/servicex-backend-tests:latest TEST_TYPE TOKEN OPTIONAL_ENDPOINT
```

The first argument `TEST_TYPE` declares what test type you would like to run. The three options are currently xAOD, uproot, and stress.

The second argument `TOKEN` is your ServiceX login token.

The third argument `OPTIONAL_ENDPOINT` is optional, specifying a custom endpoint to use with ServiceX. If no argument is submitted here, the tests will run using the standard endpoints.

## Alternate

All the tests require local working versions of ServiceX with both the xAOD and the uproot backends. Starting from a clean installation of ServiceX, first set up a virtual environment with Python 3.

Linux/MacOS:
```
% virtualenv ~/.virtualenvs/servicex-backend-testing
% source ~/.virtualenvs/servicex-backend-testing/bin/activate
```

Windows:
```
# virtualenv ~/.virtualenvs/servicex-backend-testing
# ~/.virtualenvs/servicex-backend-testing/scripts/activate
```

Once the virtual environment is active, install the dependencies for the testing suite with the command `% pip install -r requirements.txt`.
Use `kubectl port-forward` on the pods `servicex-app` and `minio` outside of the virtual environment. Here, there must be either a .servicex file in the home directory pointing to the relevant endpoints, or `servicex-app` should be forwarded on port 5000 and `minio` should be on port 9000.

We are now ready to run any of the tests by browsing to the `/tests` directory and running

```
pytest test-script-name.py
```

When finished, we can deactivate our virtual environment with `deactivate`.
