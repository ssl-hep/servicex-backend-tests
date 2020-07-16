# This script checks if a query using ServiceX is actually returning data correctly. This is the Uproot version.
# Requires that ServiceX be running with appropriate port-forward commands on ports 5000 and 9000 for ServixeX and Minio.
# Written by David Liu at the University of Washington, Seattle.
# 6 July 2020

import servicex
from func_adl import EventDataset
from func_adl_xAOD import use_exe_servicex
import func_adl_uproot
import uproot_methods
from numpy import genfromtxt

# test if we can retrieve the Pts from this particular data set, and that we get back the correct number of lines.
def test_retrieve_simple_jet_pts():
    query = "(call ResultTTree (call Select (call SelectMany (call EventDataset (list 'localds:bogus')) (lambda (list e) (call (attr e 'Jets') 'AntiKt4EMTopoJets'))) (lambda (list j) (/ (call (attr j 'pt')) 1000.0))) (list 'JetPt') 'analysis' 'junk.root')"
    dataset = "user.emmat:user.emmat.mc16_13TeV.311311.MadGraphPythia8EvtGen_A14NNPDF31LO_HSS_LLP_mH125_mS15.mc16d.200131.forsX_trees.root"
    r = servicex.get_data(query, dataset, "http://localhost:5000/servicex")
    assert len(r.index) == 11355980