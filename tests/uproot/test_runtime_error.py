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
from func_adl_servicex import ServiceXSourceUpROOT
from servicex import ignore_cache, ServiceXException, ServiceXDataset


def test_unknown_property(endpoint_uproot, uproot_single_file):
    with pytest.raises(ServiceXException) as bad_property_exception:
        with ignore_cache():
            sx = ServiceXDataset([uproot_single_file],
                                 backend_name=endpoint_uproot,
                                 status_callback_factory=None)

            src = ServiceXSourceUpROOT(sx, 'mini')
            src.Select(lambda e: {'lep_pt': e['lep_ptttttt']}).AsAwkwardArray().value()

    print("Bad property in query results in exception as expected")

    # todo: This is a useless error message
    assert bad_property_exception.value.args[1].startswith('Failed to transform all files in')


def test_code_gen_error(endpoint_uproot, uproot_single_file):
    with pytest.raises(ServiceXException) as bad_property_exception:
        with ignore_cache():
            sx = ServiceXDataset([uproot_single_file],
                                 backend_name=endpoint_uproot,
                                 status_callback_factory=None)

            src = ServiceXSourceUpROOT(sx, 'mini')
            src.Select(lambda event: x).AsAwkwardArray().value()

    print("Bad property in query results in exception as expected")

    # todo: Make this test
    assert bad_property_exception.value.args[1] == \
    'ServiceX rejected the transformation request: (400){"message": "Failed to submit transform request: Failed to generate translation code: Unknown id: x"}\n'

