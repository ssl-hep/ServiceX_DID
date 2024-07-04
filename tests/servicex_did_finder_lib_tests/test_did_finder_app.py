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
import sys
from unittest.mock import patch
import pytest
from celery import Celery

from servicex_did_finder_lib.accumulator import Accumulator
from servicex_did_finder_lib.did_finder_app import DIDFinderTask, DIDFinderApp


@pytest.fixture()
def servicex(mocker):
    """Return a ServiceXAdaptor for testing"""
    with patch(
        "servicex_did_finder_lib.did_finder_app.ServiceXAdapter", autospec=True
    ) as sx_ctor:
        sx_adaptor = mocker.MagicMock()
        sx_ctor.return_value = sx_adaptor

        yield sx_ctor


def test_did_finder_task(mocker, servicex, single_file_info):
    did_finder_task = DIDFinderTask()
    # did_finder_task.app = mocker.Mock()
    did_finder_task.app.did_finder_args = {}
    mock_generator = mocker.Mock(return_value=iter([single_file_info]))

    mock_accumulator = mocker.MagicMock(Accumulator)
    with patch(
        "servicex_did_finder_lib.did_finder_app.Accumulator", autospec=True
    ) as acc:
        acc.return_value = mock_accumulator
        did_finder_task.do_lookup('did', 1, 'https://my-servicex', mock_generator)
        servicex.assert_called_with(dataset_id=1, endpoint="https://my-servicex")
        acc.assert_called_once()

        mock_accumulator.add.assert_called_with(single_file_info)
        mock_accumulator.send_on.assert_called_with(-1)

        servicex.return_value.put_fileset_complete.assert_called_with(
            {
                "files": 0,  # Aught to have a side effect in mock accumulator that updates this
                "files-skipped": 0,
                "total-events": 0,
                "total-bytes": 0,
                "elapsed-time": 0,
            }
        )


def test_did_finder_task_exception(mocker, servicex, single_file_info):
    did_finder_task = DIDFinderTask()
    # did_finder_task.app = mocker.Mock()
    did_finder_task.app.did_finder_args = {}
    mock_generator = mocker.Mock(side_effect=Exception("Boom"))

    mock_accumulator = mocker.MagicMock(Accumulator)
    with patch(
        "servicex_did_finder_lib.did_finder_app.Accumulator", autospec=True
    ) as acc:
        acc.return_value = mock_accumulator
        did_finder_task.do_lookup('did', 1, 'https://my-servicex', mock_generator)
        servicex.assert_called_with(dataset_id=1, endpoint="https://my-servicex")
        acc.assert_called_once()

        mock_accumulator.add.assert_not_called()
        mock_accumulator.send_on.assert_not_called()

        servicex.return_value.put_fileset_complete.assert_called_with(
            {
                "files": 0,  # Aught to have a side effect in mock accumulator that updates this
                "files-skipped": 0,
                "total-events": 0,
                "total-bytes": 0,
                "elapsed-time": 0,
            }
        )


def test_did_finder_app(mocker, monkeypatch):
    # Temporarily replace sys.argv with mock_args
    monkeypatch.setattr(sys, 'argv', [
        "did_finder.py",
        "--rabbit-uri", "my-rabbit"
    ])

    mock_celery_app = mocker.MagicMock(Celery)

    with patch(
        "servicex_did_finder_lib.did_finder_app.Celery", autospec=True
    ) as celery:
        celery.return_value = mock_celery_app
        app = DIDFinderApp(did_finder_name="pytest", parsed_args=None)
        app.start()
        celery.assert_called_with("did_finder_pytest", broker_url="my-rabbit")
        mock_celery_app.worker_main.assert_called_with(argv=['worker', '--loglevel=INFO'])
