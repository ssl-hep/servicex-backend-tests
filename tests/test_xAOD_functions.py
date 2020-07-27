# This script checks if a query using ServiceX is actually returning data correctly.
# Requires that ServiceX be running with appropriate port-forward commands on ports 5000 and 9000 for ServiceX and Minio.
# Written by David Liu at the University of Washington, Seattle.
# 29 June 2020

import servicex
from servicex import ServiceXDataset
from servicex.minio_adaptor import MinioAdaptor
from servicex.servicex_adaptor import ServiceXAdaptor
from func_adl_xAOD import ServiceXDatasetSource
import uproot_methods
from numpy import genfromtxt
import math

# test if we can retrieve the Pts from this particular data set, and that we get back the correct number of lines.
def test_retrieve_simple_jet_pts():
    query = "(call ResultTTree (call Select (call SelectMany (call EventDataset (list 'localds:bogus')) (lambda (list e) (call (attr e 'Jets') 'AntiKt4EMTopoJets'))) (lambda (list j) (/ (call (attr j 'pt')) 1000.0))) (list 'JetPt') 'analysis' 'junk.root')"
    dataset = ServiceXDataset("mc15_13TeV:mc15_13TeV.361106.PowhegPythia8EvtGen_AZNLOCTEQ6L1_Zee.merge.DAOD_STDM3.e3601_s2576_s2132_r6630_r6264_p2363_tid05630052_00")
    r = dataset.get_data_pandas_df(query)
    assert len(r.index) == 11355980

# tests if a func_adl query works as above; however, in this case we test to make sure we retrieve the correct amount of data in the correct order.
def test_func_adl_simple_jet_pts():
    dataset = ServiceXDataset("mc15_13TeV:mc15_13TeV.361106.PowhegPythia8EvtGen_AZNLOCTEQ6L1_Zee.merge.DAOD_STDM3.e3601_s2576_s2132_r6630_r6264_p2363_tid05630052_00")
    query = ServiceXDatasetSource(dataset) \
        .SelectMany('lambda e: (e.Jets("AntiKt4EMTopoJets"))') \
        .Where('lambda j: (j.pt()/1000)>30') \
        .Select('lambda j: (j.pt())') \
        .AsPandasDF("JetPt") \
        .value()

    retrieved_data = query.JetPt
    retrieved_data = retrieved_data.to_numpy()
    correct_data = genfromtxt('data.csv', delimiter=',')
    assert retrieved_data.all() == correct_data.all()

# test if we can retrieve the electron four-vectors from this particular data set, and that we get back the correct number of lines.
def test_retrieve_lepton_data():
    dataset = ServiceXDataset("mc15_13TeV:mc15_13TeV.361106.PowhegPythia8EvtGen_AZNLOCTEQ6L1_Zee.merge.DAOD_STDM3.e3601_s2576_s2132_r6630_r6264_p2363_tid05630052_00")
    query = ServiceXDatasetSource(dataset)	\
        .Select('lambda e: (e.Electrons("Electrons"), e.Muons("Muons"))') \
        .Select('lambda ls: (ls[0].Select(lambda e: e.pt()), \
                             ls[0].Select(lambda e: e.eta()), \
							 ls[0].Select(lambda e: e.phi()), \
							 ls[0].Select(lambda e: e.e()), \
							 ls[1].Select(lambda m: m.pt()), \
							 ls[1].Select(lambda m: m.eta()), \
							 ls[1].Select(lambda m: m.phi()), \
							 ls[1].Select(lambda m: m.e()))') \
        .AsAwkwardArray(('ElePt', 'EleEta', 'ElePhi', 'EleE', 'MuPt', 'MuEta', 'MuPhi', 'MuE')) \
        .value()

    four_vector = uproot_methods.TLorentzVectorArray.from_ptetaphi(query[b'ElePt'], query[b'EleEta'], query[b'ElePhi'], query[b'EleE'],)
    four_vector = four_vector[four_vector.counts >= 2]
    dielectrons = four_vector[:, 0] + four_vector[:, 1]

    assert len(dielectrons.mass) == 1502958

def test_lambda_capture():
    dataset = ServiceXDataset("mc15_13TeV:mc15_13TeV.361106.PowhegPythia8EvtGen_AZNLOCTEQ6L1_Zee.merge.DAOD_STDM3.e3601_s2576_s2132_r6630_r6264_p2363_tid05630052_00")
    jets = ServiceXDatasetSource(dataset) \
        .Select('lambda e: e.Jets("AntiKt4EMTopoJets")') \
        .Select('lambda j: (j.pt(), \
                            j.eta(), \
                            j.phi())') \
        .AsPandasDF("JetPt", "JetEta", "JetPhi") \
        .value()

    electrons = ServiceXDatasetSource(dataset) \
        .Select('lambda e: e.Electrons("Electrons")') \
        .Select('lambda e: (e.pt(), \
                            e.eta(), \
                            e.phi())') \
        .AsPandasDF("ElePt", "EleEta", "ElePhi") \
        .value()

    jetr = abs(math.sqrt(jets.JetEta**2 + jets.JetPhi**2))
    eler = abs(math.sqrt(electrons.EleEta**2 + electrons.ElePhi**2))

    for electron in eler:
        event_counter = 0
        electrons_within_tolerance = []
        for jet in jetr:
            if abs(jet - electron) <= 1.0:
                electrons_within_tolerance.append(event_counter)
        event_counter += 1
    
    final_list = []

    for i in range(len(electrons_within_tolerance)):
        final_list.append(electrons.ElePt[electrons_within_tolerance[i]])

    assert final_list == final_list