#!/bin/bash
# This script will be run on Docker run, allowing users to immediately jump into usage of the test scripts.
# David Liu, 22 September 2020, Seattle, WA.

# Here, we will execute the tests by building a .servicex file with the correct backend and credentials, running the tests, and then deleting the .servicex file.

if [[ $1 = 'uproot' ]] || [[ $1 = 'Uproot' ]];
then
    printf "api_endpoint:\n  endpoint: http://test1-uproot-servicex.uc.ssl-hep.org/\n  email: $2\n  password: $3" > ./.servicex
    pytest tests/test_uproot_functions.py
    rm ./.servicex
elif [[ $1 = 'xaod' ]] || [[ $1 = 'xAOD' ]];
then
    printf "api_endpoint:\n  endpoint: http://xaod-servicex.uc.ssl-hep.org/\n  email: $2\n  password: $3" > ./.servicex
    pytest tests/test_xAOD_functions.py
    rm ./.servicex
elif [[ $1 = 'stress' ]] || [[ $1 = 'Stress' ]];
then
    printf "api_endpoint:\n  endpoint: http://xaod-servicex.uc.ssl-hep.org/\n  email: $2\n  password: $3" > ./.servicex
    pytest --stress tests/test_large_xAOD_datasets.py
    rm ./.servicex
elif [[ ${1:0:4} = 'http' ]];
then
    printf "api_endpoint:\n  endpoint: $1\n  email: $2\n  password: $3" > ./.servicex
    pytest
    rm ./.servicex
else;
    echo 'Invalid backend specified. Please enter a type of test you would like to run: xAOD, uproot, or stress, or specify a backend using its http address.'
fi
