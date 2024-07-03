# Copyright (c) 2024, IRIS-HEP
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

from servicex_did_finder_lib.accumulator import Accumulator
from servicex_did_finder_lib.did_summary import DIDSummary
from servicex_did_finder_lib.servicex_adaptor import ServiceXAdapter


@pytest.fixture
def servicex(mocker) -> ServiceXAdapter:
    return mocker.MagicMock(ServiceXAdapter)


@pytest.fixture
def did_summary_obj() -> DIDSummary:
    return DIDSummary('did')


def test_add_single_file(servicex, did_summary_obj, single_file_info):
    acc = Accumulator(sx=servicex, sum=did_summary_obj)
    acc.add(single_file_info)
    assert acc.cache_len == 1


def test_add_list_of_files(servicex, did_summary_obj, single_file_info):
    acc = Accumulator(sx=servicex, sum=did_summary_obj)
    acc.add([single_file_info, single_file_info, single_file_info])
    assert acc.cache_len == 3


def test_send_on(servicex, did_summary_obj, single_file_info):
    acc = Accumulator(sx=servicex, sum=did_summary_obj)
    acc.add([single_file_info, single_file_info, single_file_info])
    acc.send_on(3)
    servicex.put_file_add_bulk.assert_called_with(
        [single_file_info, single_file_info, single_file_info]
    )
    assert acc.cache_len == 0
    assert acc.summary.file_count == 3


def test_invalid_constructor_arg(servicex, did_summary_obj):
    with pytest.raises(ValueError):
        acc = Accumulator(sx=servicex, sum=did_summary_obj)
        acc.add("not a dict!")
