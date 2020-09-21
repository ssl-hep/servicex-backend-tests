printf "api_endpoint:\n  endpoint: http://test1-uproot-servicex.uc.ssl-hep.org/\n  email: $1\n  password: $2" > ./.servicex
pytest tests/test_uproot_functions.py
rm ./.servicex