# A simple shell script that will generate a .servicex file and automatically run it, primarily meant for GitHub actions.
# David Liu, 21 September 2020, Seattle, WA.

printf "api_endpoints:\n  - endpoint: https://uproot.servicex.ssl-hep.org/\n    token: $1\n    type: uproot" > ./.servicex
pytest -s tests/test_uproot_functions.py
rm ./.servicex