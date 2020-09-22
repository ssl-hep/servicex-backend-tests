printf "api_endpoint:\n  endpoint: http://xaod-servicex.uc.ssl-hep.org/\n  email: $1\n  password: $2" > ./.servicex
pytest tests/test_xAOD_functions.py
rm ./.servicex