# Copyright (c) 2022, IRIS-HEP
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# * Neither the name of the copyright holder nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
import pytest
from servicex import ignore_cache, ServiceXException
from func_adl_servicex import ServiceXSourceXAOD


def test_unknown_property(endpoint_xaod):
    with pytest.raises(ServiceXException) as bad_property_exception:
        with ignore_cache():
            (ServiceXSourceXAOD(
                "mc15_13TeV:mc15_13TeV.361106.PowhegPythia8EvtGen_AZNLOCTEQ6L1_Zee.merge.DAOD_STDM3.e3601_s2576_s2132_r6630_r6264_p2363_tid05630052_00",
                backend=endpoint_xaod)
             .SelectMany('lambda e: (e.Jets("I_Am_Not_A_Jet_Field"))')
             .Where('lambda j: (j.pt()/1000)>30')
             .Select('lambda j: (j.pt())')
             .AsPandasDF("JetPt")
             .value())

    print("Bad property in query results in exception as expected")

    # todo: This is a useless error message
    assert bad_property_exception.value.args[1].startswith('Failed to transform all files in')


def test_codegen_error(endpoint_xaod):
    with pytest.raises(ServiceXException) as code_gen_error:
        with ignore_cache():
            (ServiceXSourceXAOD(
                "mc15_13TeV:mc15_13TeV.361106.PowhegPythia8EvtGen_AZNLOCTEQ6L1_Zee.merge.DAOD_STDM3.e3601_s2576_s2132_r6630_r6264_p2363_tid05630052_00",
                backend=endpoint_xaod)
             .SelectMany('lambda e: (e.NotAnElectronMember("AntiKt4EMTopoJets"))')
             .Where('lambda j: (j.pt()/1000)>30')
             .Select('lambda j: (j.pt())')
             .AsPandasDF("JetPt")
             .value())

    print("Code Gen error as expected")

    # todo can this exception be easier to read and parse?
    assert code_gen_error.value.args[1].startswith('ServiceX rejected the transformation request: (400){"message": "Failed to submit transform request: Failed to generate translation code: Internal Error: attempted to get C++ representation for AST node ')

