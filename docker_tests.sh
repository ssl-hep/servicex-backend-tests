#!/bin/bash
# This script will be run on Docker run, allowing users to immediately jump into usage of the test scripts.
# David Liu, 22 September 2020, Seattle, WA.

# Here, we will execute the tests by building a .servicex file with the correct backend and credentials, running the tests, and then deleting the .servicex file.

# First, if the user did not specify a custom endpoint, we use the defaults:
if [[-z $4]];
then
    if [[ $1 = 'uproot' ]] || [[ $1 = 'Uproot' ]];
    then
        printf "api_endpoint:\n  endpoint: http://test1-uproot-servicex.uc.ssl-hep.org/\n  token: $2\n  type: uproot" > ./.servicex
        pytest tests/test_uproot_functions.py
        rm ./.servicex
    elif [[ $1 = 'xaod' ]] || [[ $1 = 'xAOD' ]];
    then
        printf "api_endpoint:\n  endpoint: https://xaod.servicex.ssl-hep.org/\n  token: $2\n  type: xaod" > ./.servicex
        pytest tests/test_xAOD_functions.py
        rm ./.servicex
    elif [[ $1 = 'stress_xAOD' ]] || [[ $1 = 'Stress_xAOD' ]];
    then
        printf "api_endpoint:\n  endpoint: https://xaod.servicex.ssl-hep.org/\n  token: $2\n  type: xaod" > ./.servicex
        pytest --stress tests/test_large_xAOD_datasets.py
        rm ./.servicex
    else
        echo 'Invalid backend specified. Please enter a type of test you would like to run: xAOD, uproot, or stress, or specify a backend using its http address.'
    fi

# if the user specified a custom endpoint, run the custom endpoint
else
    if [[ $1 = 'uproot' ]] || [[ $1 = 'Uproot' ]];
    then
        printf "api_endpoint:\n  endpoint: $3\n  token: $2\n  type: uproot" > ./.servicex
        pytest tests/test_uproot_functions.py
        rm ./.servicex
    elif [[ $1 = 'xaod' ]] || [[ $1 = 'xAOD' ]];
    then
        printf "api_endpoint:\n  endpoint: $3\n  token: $2\n  type: xaod" > ./.servicex
        pytest tests/test_xAOD_functions.py
        rm ./.servicex
    elif [[ $1 = 'stress_xAOD' ]] || [[ $1 = 'Stress_xAOD' ]];
    then
        printf "api_endpoint:\n  endpoint: $3/\n  token: $2\n  type: xaod" > ./.servicex
        pytest --stress tests/test_large_xAOD_datasets.py
        rm ./.servicex
    else
        echo 'Invalid backend specified. Please enter a type of test you would like to run: xAOD, uproot, or stress_xAOD.'
    fi
fi