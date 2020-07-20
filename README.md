# servicex-backend-tests

These are tests for the backends of IRIS-HEP's ServiceX. The intended functionality is to ensure that the xAOD and uproot transformers both correctly provide the same data.

# Installation and Execution

All the tests require local working versions of ServiceX with both the xAOD and the uproot backends. Starting from a clean installation of ServiceX, first set up a virtual environment with Python 3.

Linux/MacOS:
```
% virtualenv ~/.virtualenvs/servicex-backend-testing
% source ~/.virtualenvs/servicex-backend-testing/bin/activate
```

Windows (you will need to run PowerShell as an administrator):
```
# virtualenv ~/.virtualenvs/servicex-backend-testing
# Set-ExecutionPolicy Unrestricted -scope process
# Y
# ~/.virtualenvs/servicex-backend-testing/scripts/activate
```

Once the virtual environment is active, install the dependencies for the testing suite with the command `% pip install -r requirements.txt`.
Use `kubectl port-forward` on the pods `servicex-app` and `minio` outside of the virtual environment. We are now ready to run any of the tests by browsing to the `/tests` directory and running

```
pytest test-script-name.py
```

When finished, we can deactivate our virtual environment with `deactivate`.