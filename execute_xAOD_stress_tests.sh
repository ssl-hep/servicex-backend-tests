printf "api_endpoint:\n  endpoint: http://xaod.uc.ssl-hep.org/\n  email: $1\n  password: $2" > ./.servicex
pytest --stress tests/test_large_xAOD_datasets.py
rm ./.servicex