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
import numpy as np
import math
import asyncio
import pytest
import logging

@pytest.fixture(autouse=True)
def turn_on_logging():
    logging.basicConfig(level=logging.DEBUG)
    yield None
    logging.basicConfig(level=logging.WARNING)

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
    correct_data = genfromtxt('tests/data.csv', delimiter=',')
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

# test if we can retrieve electrons that are within a certain pseudorapidity allowance of a jet
@pytest.mark.asyncio
async def test_lambda_capture():
    dataset = ServiceXDataset("mc15_13TeV:mc15_13TeV.361106.PowhegPythia8EvtGen_AZNLOCTEQ6L1_Zee.merge.DAOD_STDM3.e3601_s2576_s2132_r6630_r6264_p2363_tid05630052_00")

    async def retrieve_jet_data(dataset):
        jets = ServiceXDatasetSource(dataset) \
            .Select('lambda e: e.Jets("AntiKt4EMTopoJets").Where(lambda jet: jet.pt()/1000>60)') \
            .Select('lambda j: (j.Select(lambda jet: jet.pt()), \
                                j.Select(lambda jet: jet.eta()), \
                                j.Select(lambda jet: jet.phi()))') \
            .AsAwkwardArray(("JetPt", "JetEta", "JetPhi")) \
            .value_async()

        return await jets

    async def retrieve_ele_data(dataset):
        electrons = ServiceXDatasetSource(dataset) \
            .Select('lambda e: e.Electrons("Electrons")') \
            .Select('lambda e: (e.Select(lambda ele: ele.pt()), \
                                e.Select(lambda ele: ele.eta()), \
                                e.Select(lambda ele: ele.phi()))') \
            .AsAwkwardArray(("ElePt", "EleEta", "ElePhi")) \
            .value_async()

        return await electrons

    data_jet, data_ele = await asyncio.gather(retrieve_jet_data(dataset), retrieve_ele_data(dataset))

    jetr = np.sqrt(data_jet[b'JetEta']**2 + data_jet[b'JetPhi']**2)
    eler = np.sqrt(data_ele[b'EleEta']**2 + data_ele[b'EleEta']**2)

    electrons_within_tolerance = []
    for electron in eler:
        event_counter = 0
        for jet in jetr:
            if abs(jet - electron) <= 1.0:
                electrons_within_tolerance.append(event_counter)
        event_counter += 1
    
    final_list = []

    for i in range(len(electrons_within_tolerance)):
        final_list.append(data_ele[b'ElePt'][electrons_within_tolerance[i]])
        
    assert final_list == final_list