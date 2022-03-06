import sys
import random
from func_adl_servicex import ServiceXSourceUpROOT

random.seed()

token = ""
endpoint = "https://servicex-release-testing-1.servicex.ssl-hep.org/"
dataset_name = "root://eospublic.cern.ch//eos/opendata/atlas/OutreachDatasets/2020-01-22/4lep/MC/mc_345060.ggH125_ZZ4lep.4lep.root"


if len(sys.argv) > 1:
    token = sys.argv[1]
else:
    print("Usage: python single_uproot_test.py <token> [endpoint] [dataset].")
    sys.exit(1)
if len(sys.argv) > 2:
    endpoint = sys.argv[2]
if len(sys.argv) > 3:
    dataset_name = sys.argv[3]

f = open("servicex.yaml", "w")
sx = f"api_endpoints: \n - endpoint: {endpoint}\n   token: {token}\n   name: uproot"
print(sx)
f.write(sx)
f.close()

src = ServiceXSourceUpROOT([dataset_name], 'mini', backend_name="uproot")
r = (
    src.Select(lambda e: {'lep_pt': e['lep_pt']}).AsAwkwardArray().value()
)
print(r)
