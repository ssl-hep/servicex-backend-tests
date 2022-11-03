# This script checks if a query using ServiceX is actually returning data correctly.
# Requires that ServiceX be running with appropriate port-forward commands on ports 5000 and 9000 for ServiceX and Minio.
# Written by David Liu at the University of Washington, Seattle.
# 29 June 2020

from servicex import ServiceXDataset, ignore_cache
from func_adl_servicex import ServiceXSourceXAOD
from numpy import genfromtxt


def test_func_adl_simple_jet_pts(endpoint_xaod, xaod_did):
    with ignore_cache():
        sx = ServiceXDataset(xaod_did,
                             backend_name=endpoint_xaod,
                             status_callback_factory=None)

        query = (ServiceXSourceXAOD(sx)
                 .SelectMany('lambda e: (e.Jets("AntiKt4EMTopoJets"))')
                 .Where('lambda j: (j.pt()/1000)>30')
                 .Select('lambda j: (j.pt())')
                 .AsPandasDF("JetPt")
                 .value())

    retrieved_data = query.JetPt
    retrieved_data = retrieved_data.to_numpy()
    correct_data = genfromtxt('tests/data.csv', delimiter=',')
    assert retrieved_data.all() == correct_data.all()


def test_get_attribute_float_data(endpoint_xaod, xaod_did):
    with ignore_cache():
        sx = ServiceXDataset(xaod_did,
                             backend_name=endpoint_xaod,
                             status_callback_factory=None)

        query = (ServiceXSourceXAOD(sx)
                 .SelectMany('lambda e: e.Jets("AntiKt4EMTopoJets")')
                 .Where('lambda j: j.pt()/1000 > 20 and abs(j.eta()/1000) < 4.5')
                 .Select('lambda j: j.getAttributeFloat("LArQuality")')
                 .AsPandasDF("LArQuality")
                 .value())

    assert len(query.LArQuality) == 3551964


# test if lambda capture is working properly in func_adl
def test_lambda_capture(endpoint_xaod, xaod_did):
    def retrieve_jet_lambda(dataset):
        with ignore_cache():
            sx = ServiceXDataset(dataset,
                                 backend_name=endpoint_xaod,
                                 status_callback_factory=None)

            jets = (ServiceXSourceXAOD(sx)
                    .Select(
                'lambda e: (e.Jets("AntiKt4EMTopoJets").Where(lambda jet: jet.pt()/1000 > 60.0), e.Electrons("Electrons"))')
                    .Select(
                'lambda ls: ls[1].Select(lambda ele: ls[0].Select(lambda jet: jet.pt() + ele.pt()).Count())')
                    .AsAwkwardArray(('JetPt'))
                    .value())

        return jets

    lambda_test = retrieve_jet_lambda(xaod_did)
    assert len(lambda_test['JetPt']) == 1993800
