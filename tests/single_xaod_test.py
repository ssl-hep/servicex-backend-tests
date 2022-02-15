import sys
import random
from func_adl_servicex import ServiceXSourceXAOD

random.seed()

token = ""
endpoint = "https://servicex-release-testing-2.servicex.ssl-hep.org/"
dataset_name = "mc15_13TeV:mc15_13TeV.361106.PowhegPythia8EvtGen_AZNLOCTEQ6L1_Zee.merge.DAOD_STDM3.e3601_s2576_s2132_r6630_r6264_p2363_tid05630052_00"


def create_yaml():
    if len(sys.argv) > 1:
        token = sys.argv[1]
        f = open("../servicex.yaml", "w")
        sx = f"api_endpoints: \n - endpoint: {endpoint}\n    token: {token}\n    name: xaod"
        print(sx)
        f.write(sx)
        f.close()
    else:
        print("needs at least token input parameter")


src = ServiceXSourceXAOD(dataset_name, backend="xaod")
df = src \
    .SelectMany('lambda e: e.Jets("AntiKt4EMTopoJets")') \
    .Select('lambda j: j.pt()/'+str(random.random())) \
    .AsPandasDF('JetPt') \
    .value()
print(df)
