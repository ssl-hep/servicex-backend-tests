# A simple shell script that will generate a servicex.yaml file and automatically run it, primarily meant for GitHub actions.
# David Liu, 21 September 2020, Seattle, WA.

printf "api_endpoints:\n  - endpoint: $2\n    token: $1\n    name: uproot\n    type: uproot" > ./servicex.yaml
pytest -s tests/uproot --endpoint_uproot=uproot
rm ./servicex.yaml
