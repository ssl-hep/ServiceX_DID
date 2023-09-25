import argparse
import json
from typing import Any, AsyncGenerator, Dict, Optional
from unittest.mock import ANY, MagicMock, patch

import pika
import pytest
from servicex_did_finder_lib.communication import (
    add_did_finder_cnd_arguments,
    init_rabbit_mq,
    start_did_finder,
    run_file_fetch_loop,
)


class RabbitAdaptor:
    def __init__(self, channel):
        self._channel = channel
        pass

    def send_did_request(self, did_name: str):
        # Get out the callback so we can send it.
        self._channel.basic_consume.assert_called_once()
        callback = self._channel.basic_consume.call_args[1]["on_message_callback"]
        channel = MagicMock()
        method = MagicMock()
        properties = MagicMock()
        body = json.dumps(
            {
                "request_id": '123-456',
                "did": did_name,
                "dataset_id": "000-111-222-444",
                "endpoint": "http://localhost:2334/",
            }
        )

        callback(channel, method, properties, body)


@pytest.fixture()
def SXAdaptor(mocker):
    "Return a ServiceXAdaptor for testing"
    with patch(
        "servicex_did_finder_lib.communication.ServiceXAdapter", autospec=True
    ) as sx_ctor:
        sx_adaptor = mocker.MagicMock()
        sx_ctor.return_value = sx_adaptor

        yield sx_adaptor


@pytest.fixture()
def rabbitmq(mocker):
    with patch(
        "servicex_did_finder_lib.communication.pika.BlockingConnection", autospec=True
    ) as block_connection_ctor:

        block_connection = mocker.MagicMock()
        block_connection_ctor.return_value = block_connection

        channel = mocker.MagicMock()
        block_connection.channel.return_value = channel

        control = RabbitAdaptor(channel)
        yield control


@pytest.fixture()
def rabbitmq_fail_once(mocker):
    with patch(
        "servicex_did_finder_lib.communication.pika.BlockingConnection", autospec=True
    ) as block_connection_ctor:

        block_connection = mocker.MagicMock()
        block_connection_ctor.side_effect = [
            pika.exceptions.AMQPConnectionError(),  # type: ignore
            block_connection,
        ]

        channel = mocker.MagicMock()
        block_connection.channel.return_value = channel

        control = RabbitAdaptor(channel)
        yield control


@pytest.fixture()
def init_rabbit_callback(mocker):
    with patch(
        "servicex_did_finder_lib.communication.init_rabbit_mq", autospec=True
    ) as call_back:
        yield call_back


@pytest.fixture()
def simple_argument_parser(mocker):
    with patch(
        "servicex_did_finder_lib.communication.argparse.ArgumentParser", autospec=True
    ) as ctor_ArgumentParser:
        parser = mocker.MagicMock(spec=argparse.ArgumentParser)
        ctor_ArgumentParser.return_value = parser

        parsed_args = mocker.MagicMock()
        parsed_args.rabbit_uri = "test_queue_address"

        parser.parse_args = mocker.MagicMock(return_value=parsed_args)

        yield parser


def test_one_file_call(rabbitmq, SXAdaptor):
    "Test a working, simple, one file call"

    seen_name = None

    async def my_callback(did_name: str, info: Dict[str, Any]):
        nonlocal seen_name
        seen_name = did_name
        yield {
            "paths": ["fork/it/over"],
            "adler32": "no clue",
            "file_size": 22323,
            "file_events": 0,
        }

    init_rabbit_mq(
        my_callback,
        "http://myrabbit.com",
        "test_queue_name",
        retries=12,
        retry_interval=10,
    )

    rabbitmq.send_did_request("hi-there")

    # Make sure callback was called
    assert seen_name == "hi-there"

    # Make sure the file was sent along, along with the completion
    SXAdaptor.put_file_add.assert_called_once_with(
        {
            "paths": ["fork/it/over"],
            "adler32": "no clue",
            "file_size": 22323,
            "file_events": 0,
        }
    )
    SXAdaptor.put_fileset_complete.assert_called_once()


def test_one_file_call_with_param(rabbitmq, SXAdaptor):
    "Test a working, simple, one file call with parameter"

    seen_name = None

    async def my_callback(did_name: str, info: Dict[str, Any]):
        nonlocal seen_name
        seen_name = did_name
        yield {
            "paths": ["fork/it/over"],
            "adler32": "no clue",
            "file_size": 22323,
            "file_events": 0,
        }

    init_rabbit_mq(
        my_callback,
        "http://myrabbit.com",
        "test_queue_name",
        retries=12,
        retry_interval=10,
    )

    rabbitmq.send_did_request("hithere?files=10")

    # Make sure callback was called
    assert seen_name == "hithere"

    # Make sure the file was sent along, along with the completion
    SXAdaptor.put_file_add_bulk.assert_called_once()
    SXAdaptor.put_fileset_complete.assert_called_once()


def test_bulk_file_call_with_param(rabbitmq, SXAdaptor):
    "Test a working, simple, two file call with parameter"

    seen_name = None

    async def my_callback(did_name: str, info: Dict[str, Any]):
        nonlocal seen_name
        seen_name = did_name
        yield [
            {
                "paths": ["fork/it/over"],
                "adler32": "no clue",
                "file_size": 22323,
                "file_events": 0,
            },
            {
                "paths": ["fork/it/over"],
                "adler32": "no clue",
                "file_size": 22323,
                "file_events": 0,
            },
        ]

    init_rabbit_mq(
        my_callback,
        "http://myrabbit.com",
        "test_queue_name",
        retries=12,
        retry_interval=10,
    )

    rabbitmq.send_did_request("hithere?files=10")

    # Make sure callback was called
    assert seen_name == "hithere"

    # Make sure the file was sent along, along with the completion
    SXAdaptor.put_file_add_bulk.assert_called_once()
    SXAdaptor.put_fileset_complete.assert_called_once()


def test_bulk_file_call_with_file_limit(rabbitmq, SXAdaptor):
    "Test a working, simple, two file call with parameter"

    seen_name = None

    f1 = {
        "paths": ["fork/it/over/1"],
        "adler32": "no clue",
        "file_size": 22323,
        "file_events": 0,
    }
    f2 = {
        "paths": ["fork/it/over/2"],
        "adler32": "no clue",
        "file_size": 22323,
        "file_events": 0,
    }

    async def my_callback(did_name: str, info: Dict[str, Any]):
        nonlocal seen_name
        seen_name = did_name
        yield [f1, f2]

    init_rabbit_mq(
        my_callback,
        "http://myrabbit.com",
        "test_queue_name",
        retries=12,
        retry_interval=10,
    )

    rabbitmq.send_did_request("hithere?files=1")

    # Make sure callback was called
    assert seen_name == "hithere"

    # Make sure only one file was sent along.
    SXAdaptor.put_file_add_bulk.assert_called_with([f1])
    SXAdaptor.put_fileset_complete.assert_called_once()


def test_with_scope(rabbitmq, SXAdaptor):
    seen_name: Optional[str] = None

    async def my_callback(did_name: str, info: Dict[str, Any]):
        nonlocal seen_name
        seen_name = did_name
        yield {
            "paths": ["fork/it/over"],
            "adler32": "no clue",
            "file_size": 22323,
            "file_events": 0,
        }

    init_rabbit_mq(
        my_callback,
        "http://myrabbit.com",
        "test_queue_name",
        retries=12,
        retry_interval=10,
    )

    did = (
        "cms:DYJetsToLL_M-50_TuneCP5_13TeV-amcatnloFXFX-pythia8"
        "/RunIIAutumn18NanoAODv7-Nano02Apr2020_102X_upgrade2018_realistic_v21_ext2-v1/NANOAODSIM"
    )
    rabbitmq.send_did_request(did)

    assert seen_name == did


def test_failed_file(rabbitmq, SXAdaptor):
    "Test a callback that fails before any files are sent"

    async def my_callback(
        did_name: str, info: Dict[str, Any]
    ) -> AsyncGenerator[Dict[str, Any], None]:
        if False:
            yield {"ops": "no"}
        raise Exception("that did not work")

    init_rabbit_mq(
        my_callback,
        "http://myrabbit.com",
        "test_queue_name",
        retries=12,
        retry_interval=10,
    )
    rabbitmq.send_did_request("hi-there")

    # Make sure the file was sent along, along with the completion
    SXAdaptor.put_file_add.assert_not_called()
    SXAdaptor.put_fileset_complete.assert_not_called()
    SXAdaptor.post_status_update.assert_any_call(ANY, severity="fatal")


def test_failed_file_after_good(rabbitmq, SXAdaptor):
    "Test a callback that fails before any files are sent"

    async def my_callback(
        did_name: str, info: Dict[str, Any]
    ) -> AsyncGenerator[Dict[str, Any], None]:
        yield {
            "paths": ["fork/it/over"],
            "adler32": "no clue",
            "file_size": 22323,
            "file_events": 0,
        }
        raise Exception("that did not work")

    init_rabbit_mq(
        my_callback,
        "http://myrabbit.com",
        "test_queue_name",
        retries=12,
        retry_interval=10,
    )
    rabbitmq.send_did_request("hi-there")

    # Make sure the file was sent along, along with the completion
    SXAdaptor.put_file_add.assert_called_once()
    SXAdaptor.put_fileset_complete.assert_not_called()
    SXAdaptor.post_status_update.assert_any_call(ANY, severity="fatal")


def test_failed_file_after_good_with_avail(rabbitmq, SXAdaptor):
    "Test a callback that fails before any files are sent"

    async def my_callback(
        did_name: str, info: Dict[str, Any]
    ) -> AsyncGenerator[Dict[str, Any], None]:
        yield {
            "paths": ["fork/it/over"],
            "adler32": "no clue",
            "file_size": 22323,
            "file_events": 0,
        }
        raise Exception("that did not work")

    init_rabbit_mq(
        my_callback,
        "http://myrabbit.com",
        "test_queue_name",
        retries=12,
        retry_interval=10,
    )
    rabbitmq.send_did_request("hi-there?get=available")

    # Make sure the file was sent along, along with the completion
    SXAdaptor.put_file_add.assert_called_once()
    SXAdaptor.put_fileset_complete.assert_called_once()
    SXAdaptor.post_status_update.assert_any_call("Completed load of files in 0 seconds")


def test_failed_file_after_good_with_avail_limited_number(rabbitmq, SXAdaptor):
    "Files are sent back, only available allowed, but want a certian number"

    async def my_callback(
        did_name: str, info: Dict[str, Any]
    ) -> AsyncGenerator[Dict[str, Any], None]:
        yield {
            "paths": ["fork/it/over1"],
            "adler32": "no clue",
            "file_size": 22323,
            "file_events": 0,
        }
        yield {
            "paths": ["fork/it/over2"],
            "adler32": "no clue",
            "file_size": 22323,
            "file_events": 0,
        }
        raise Exception("that did not work")

    init_rabbit_mq(
        my_callback,
        "http://myrabbit.com",
        "test_queue_name",
        retries=12,
        retry_interval=10,
    )
    rabbitmq.send_did_request("hi-there?get=available&files=1")

    # Make sure the file was sent along, along with the completion
    SXAdaptor.put_file_add_bulk.assert_called_once()
    SXAdaptor.put_fileset_complete.assert_called_once()
    SXAdaptor.post_status_update.assert_any_call("Completed load of files in 0 seconds")


def test_no_files_returned(rabbitmq, SXAdaptor):
    "Test a callback that fails before any files are sent"

    async def my_callback(
        did_name: str, info: Dict[str, Any]
    ) -> AsyncGenerator[Dict[str, Any], None]:
        if False:
            yield {"ops": "no"}

    init_rabbit_mq(
        my_callback,
        "http://myrabbit.com",
        "test_queue_name",
        retries=12,
        retry_interval=10,
    )
    rabbitmq.send_did_request("hi-there")

    # Make sure the file was sent along, along with the completion
    SXAdaptor.put_file_add.assert_not_called()
    SXAdaptor.put_fileset_complete.assert_called_once()
    assert SXAdaptor.put_fileset_complete.call_args[0][0]["files"] == 0
    SXAdaptor.post_status_update.assert_any_call(ANY, severity="fatal")


def test_rabbitmq_connection_failure(rabbitmq_fail_once, SXAdaptor):
    "Make sure that when we have a connection failure we try again"

    called = False

    async def my_callback(
        did_name: str, info: Dict[str, Any]
    ) -> AsyncGenerator[Dict[str, Any], None]:
        nonlocal called
        called = True
        yield {
            "paths": ["fork/it/over"],
            "adler32": "no clue",
            "file_size": 22323,
            "file_events": 0,
        }

    init_rabbit_mq(
        my_callback,
        "http://myrabbit.com",
        "test_queue_name",
        retries=1,
        retry_interval=0.1,
    )
    rabbitmq_fail_once.send_did_request("hi-there")

    assert called


def test_auto_args_callback(init_rabbit_callback, simple_argument_parser):
    "If there is a missing argument on the command line it should cause a total failure"

    async def my_callback(
        did_name: str, info: Dict[str, Any]
    ) -> AsyncGenerator[Dict[str, Any], None]:
        if False:
            yield {"ops": "no"}

    start_did_finder("special", my_callback)

    assert init_rabbit_callback.call_args[0][1] == "test_queue_address"
    assert init_rabbit_callback.call_args[0][2] == "special_did_requests"


def test_arg_required(init_rabbit_callback):
    "Make sure the argument passed on the command line makes it in"
    # This option is only available in 3.9, so for less than 3.9 we can't run this test.
    parser = argparse.ArgumentParser()
    add_did_finder_cnd_arguments(parser)
    parser.add_argument("--dude", dest="sort_it", action="store")

    args = parser.parse_args(["--rabbit-uri", "not-really-there"])

    async def my_callback(
        did_name: str, info: Dict[str, Any]
    ) -> AsyncGenerator[Dict[str, Any], None]:
        if False:
            yield {"ops": "no"}

    start_did_finder("test_finder", my_callback, args)

    assert init_rabbit_callback.call_args[0][1] == "not-really-there"


@pytest.mark.asyncio
async def test_run_file_fetch_loop(SXAdaptor, mocker):
    async def my_user_callback(did, info):
        return_values = [
            {
                "paths": ["/tmp/foo"],
                "adler32": "13e4f",
                "file_size": 1024,
                "file_events": 128,
            },
            {
                "paths": ["/tmp/bar"],
                "adler32": "f33d",
                "file_size": 2046,
                "file_events": 64,
            },
        ]
        for v in return_values:
            yield v

    await run_file_fetch_loop("123-456", SXAdaptor, {}, my_user_callback)
    SXAdaptor.post_transform_start.assert_called_once()

    assert SXAdaptor.put_file_add.call_count == 2
    assert SXAdaptor.put_file_add.call_args_list[0][0][0]["paths"][0] == "/tmp/foo"
    assert SXAdaptor.put_file_add.call_args_list[1][0][0]["paths"][0] == "/tmp/bar"

    SXAdaptor.put_fileset_complete.assert_called_once
    assert SXAdaptor.put_fileset_complete.call_args[0][0]["files"] == 2
    assert SXAdaptor.put_fileset_complete.call_args[0][0]["files-skipped"] == 0
    assert SXAdaptor.put_fileset_complete.call_args[0][0]["total-events"] == 192
    assert SXAdaptor.put_fileset_complete.call_args[0][0]["total-bytes"] == 3070

    assert SXAdaptor.post_status_update.called_once()


@pytest.mark.asyncio
async def test_run_file_bulk_fetch_loop(SXAdaptor, mocker):
    async def my_user_callback(did, info):
        return_values = [
            {
                "paths": ["/tmp/foo"],
                "adler32": "13e4f",
                "file_size": 1024,
                "file_events": 128,
            },
            {
                "paths": ["/tmp/bar"],
                "adler32": "f33d",
                "file_size": 2046,
                "file_events": 64,
            },
        ]
        yield return_values

    await run_file_fetch_loop("123-456", SXAdaptor, {}, my_user_callback)
    SXAdaptor.post_transform_start.assert_called_once()

    assert SXAdaptor.put_file_add_bulk.call_count == 1
    assert (
        SXAdaptor.put_file_add_bulk.call_args_list[0][0][0][0]["paths"][0] == "/tmp/foo"
    )
    assert (
        SXAdaptor.put_file_add_bulk.call_args_list[0][0][0][1]["paths"][0] == "/tmp/bar"
    )

    SXAdaptor.put_fileset_complete.assert_called_once
    assert SXAdaptor.put_fileset_complete.call_args[0][0]["files"] == 2
    assert SXAdaptor.put_fileset_complete.call_args[0][0]["files-skipped"] == 0
    assert SXAdaptor.put_fileset_complete.call_args[0][0]["total-events"] == 192
    assert SXAdaptor.put_fileset_complete.call_args[0][0]["total-bytes"] == 3070

    assert SXAdaptor.post_status_update.called_once()


@pytest.mark.asyncio
async def test_run_file_fetch_one(SXAdaptor, mocker):
    async def my_user_callback(did, info):
        return_values = [
            {
                "paths": ["/tmp/foo"],
                "adler32": "13e4f",
                "file_size": 1024,
                "file_events": 128,
            },
            {
                "paths": ["/tmp/bar"],
                "adler32": "f33d",
                "file_size": 2046,
                "file_events": 64,
            },
        ]
        for v in return_values:
            yield v

    await run_file_fetch_loop("123-456?files=1", SXAdaptor, {}, my_user_callback)
    SXAdaptor.post_transform_start.assert_called_once()

    assert SXAdaptor.put_file_add_bulk.call_count == 1
    SXAdaptor.put_file_add_bulk.assert_called_with(
        [
            {
                "paths": ["/tmp/bar"],
                "adler32": "f33d",
                "file_size": 2046,
                "file_events": 64,
            }
        ]
    )

    SXAdaptor.put_fileset_complete.assert_called_once
    assert SXAdaptor.put_fileset_complete.call_args[0][0]["files"] == 1
    assert SXAdaptor.post_status_update.called_once()


@pytest.mark.asyncio
async def test_run_file_fetch_one_reverse(SXAdaptor, mocker):
    "The files should be sorted so they return the same"

    async def my_user_callback(did, info):
        return_values = [
            {
                "paths": ["/tmp/bar"],
                "adler32": "f33d",
                "file_size": 2046,
                "file_events": 64,
            },
            {
                "paths": ["/tmp/foo"],
                "adler32": "13e4f",
                "file_size": 1024,
                "file_events": 128,
            },
        ]
        for v in return_values:
            yield v

    await run_file_fetch_loop("123-456?files=1", SXAdaptor, {}, my_user_callback)
    SXAdaptor.post_transform_start.assert_called_once()

    assert SXAdaptor.put_file_add_bulk.call_count == 1
    SXAdaptor.put_file_add_bulk.assert_called_with(
        [
            {
                "paths": ["/tmp/bar"],
                "adler32": "f33d",
                "file_size": 2046,
                "file_events": 64,
            }
        ]
    )

    SXAdaptor.put_fileset_complete.assert_called_once
    assert SXAdaptor.put_fileset_complete.call_args[0][0]["files"] == 1
    assert SXAdaptor.post_status_update.called_once()


@pytest.mark.asyncio
async def test_run_file_fetch_one_multi(SXAdaptor, mocker):
    async def my_user_callback(did, info):
        return_values = [
            {
                "paths": ["/tmp/foo", "others:/tmp/foo"],
                "adler32": "13e4f",
                "file_size": 1024,
                "file_events": 128,
            },
            {
                "paths": ["/tmp/bar", "others:/tmp/bar"],
                "adler32": "f33d",
                "file_size": 2046,
                "file_events": 64,
            },
        ]
        for v in return_values:
            yield v

    await run_file_fetch_loop("123-456?files=1", SXAdaptor, {}, my_user_callback)
    SXAdaptor.post_transform_start.assert_called_once()

    assert SXAdaptor.put_file_add_bulk.call_count == 1
    SXAdaptor.put_file_add_bulk.assert_called_with(
        [
            {
                "paths": ["/tmp/bar", "others:/tmp/bar"],
                "adler32": "f33d",
                "file_size": 2046,
                "file_events": 64,
            }
        ]
    )

    SXAdaptor.put_fileset_complete.assert_called_once
    assert SXAdaptor.put_fileset_complete.call_args[0][0]["files"] == 1
    assert SXAdaptor.post_status_update.called_once()


@pytest.mark.asyncio
async def test_run_file_fetch_loop_bad_did(SXAdaptor, mocker):
    async def my_user_callback(did, info):
        return_values = []
        for v in return_values:
            yield v

    await run_file_fetch_loop("123-456", SXAdaptor, {}, my_user_callback)

    assert SXAdaptor.put_file_add.assert_not_called
    SXAdaptor.put_fileset_complete.assert_called_once()

    assert (
        SXAdaptor.post_status_update.call_args_list[0][0][0]
        == "DID Finder found zero files for dataset 123-456"
    )

    assert SXAdaptor.post_status_update.call_args_list[0][1]["severity"] == "fatal"
