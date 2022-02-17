# A simple shell script that will generate a servicex.yaml file and automatically run it, primarily meant for GitHub actions.
# David Liu, 21 September 2020, Seattle, WA.

printf "api_endpoints:\n  - endpoint: $2\n    token: $1\n    name: xaod" > ./servicex.yaml
pytest tests/test_xAOD_functions.py
rm ./servicex.yaml