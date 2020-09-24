# A simple shell script that will generate a .servicex file and automatically run it, primarily meant for GitHub actions.
# David Liu, 21 September 2020, Seattle, WA.

printf "api_endpoint:\n  endpoint: http://xaod-servicex.uc.ssl-hep.org/\n  email: $1\n  password: $2" > ./.servicex
pytest tests/test_xAOD_functions.py
rm ./.servicex