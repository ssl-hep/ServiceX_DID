import argparse
from typing import Any, AsyncGenerator, Dict
import pika
import pytest
from unittest.mock import ANY, call, patch, MagicMock
import json

from servicex_did.communication import default_command_line_args, init_rabbit_mq, start_did_finder


class RabbitAdaptor:
    def __init__(self, channel):
        self._channel = channel
        pass

    def send_did_request(self, did_name: str):
        # Get out the callback so we can send it.
        self._channel.basic_consume.assert_called_once()
        callback = self._channel.basic_consume.call_args[1]['on_message_callback']
        channel = MagicMock()
        method = MagicMock()
        properties = MagicMock()
        body = json.dumps({'did': did_name, 'request_id': '000-111-222-444',
                           'service-endpoint': 'http://localhost:2334'})

        callback(channel, method, properties, body)


@pytest.fixture()
def SXAdaptor(mocker):
    'Return a ServiceXAdaptor for testing'
    with patch('servicex_did.communication.ServiceXAdapter', autospec=True) as sx_ctor:
        sx_adaptor = mocker.MagicMock()
        sx_ctor.return_value = sx_adaptor

        yield sx_adaptor


@pytest.fixture()
def rabbitmq(mocker):
    with patch('servicex_did.communication.pika.BlockingConnection', autospec=True) \
            as block_connection_ctor:

        block_connection = mocker.MagicMock()
        block_connection_ctor.return_value = block_connection

        channel = mocker.MagicMock()
        block_connection.channel.return_value = channel

        control = RabbitAdaptor(channel)
        yield control


@pytest.fixture()
def rabbitmq_fail_once(mocker):
    with patch('servicex_did.communication.pika.BlockingConnection', autospec=True) \
            as block_connection_ctor:

        block_connection = mocker.MagicMock()
        block_connection_ctor.side_effect = [pika.exceptions.AMQPConnectionError(),
                                             block_connection]

        channel = mocker.MagicMock()
        block_connection.channel.return_value = channel

        control = RabbitAdaptor(channel)
        yield control


@pytest.fixture()
def init_rabbit_callback(mocker):
    with patch('servicex_did.communication.init_rabbit_mq', autospec=True) as call_back:
        yield call_back


@pytest.fixture()
def simple_argument_parser(mocker):
    with patch('servicex_did.communication.argparse.ArgumentParser', autospec=True) \
            as ctor_ArgumentParser:
        parser = mocker.MagicMock(spec=argparse.ArgumentParser)
        ctor_ArgumentParser.return_value = parser

        parsed_args = mocker.MagicMock()
        parsed_args.rabbit_uri = 'test_queue_address'

        parser.parse_args = mocker.MagicMock(return_value=parsed_args)

        yield parser


def test_one_file_call(rabbitmq, SXAdaptor):
    'Test a working, simple, one file call'

    seen_name = None

    async def my_callback(did_name: str):
        nonlocal seen_name
        seen_name = did_name
        yield {
            'file_path': "fork/it/over",
            'adler32': 'no clue',
            'file_size': 22323,
            'file_events': 0,
        }

    init_rabbit_mq(my_callback, 'http://myrabbit.com', 'test_queue_name', retries=12,
                   retry_interval=10)

    rabbitmq.send_did_request('hi-there')

    # Make sure callback was called
    assert seen_name == 'hi-there'

    # Make sure the file was sent along, along with the completion
    SXAdaptor.put_file_add.assert_called_once()
    SXAdaptor.put_fileset_complete.assert_called_once()


def test_failed_file(rabbitmq, SXAdaptor):
    'Test a callback that fails before any files are sent'

    async def my_callback(did_name: str) -> AsyncGenerator[Dict[str, Any], None]:
        if False:
            yield {
                'ops': 'no'
            }
        raise Exception('that did not work')

    init_rabbit_mq(my_callback, 'http://myrabbit.com', 'test_queue_name', retries=12,
                   retry_interval=10)
    rabbitmq.send_did_request('hi-there')

    # Make sure the file was sent along, along with the completion
    SXAdaptor.put_file_add.assert_not_called()
    SXAdaptor.put_fileset_complete.assert_not_called()
    SXAdaptor.post_status_update.assert_any_call(ANY, severity='fatal')


def test_no_files_returned(rabbitmq, SXAdaptor):
    'Test a callback that fails before any files are sent'

    async def my_callback(did_name: str) -> AsyncGenerator[Dict[str, Any], None]:
        if False:
            yield {
                'ops': 'no'
            }

    init_rabbit_mq(my_callback, 'http://myrabbit.com', 'test_queue_name', retries=12,
                   retry_interval=10)
    rabbitmq.send_did_request('hi-there')

    # Make sure the file was sent along, along with the completion
    SXAdaptor.put_file_add.assert_not_called()
    SXAdaptor.put_fileset_complete.assert_not_called()
    SXAdaptor.post_status_update.assert_any_call(ANY, severity='fatal')


def test_rabbitmq_connection_failure(rabbitmq_fail_once, SXAdaptor):
    'Make sure that when we have a connection failure we try again'

    called = False

    async def my_callback(did_name: str) -> AsyncGenerator[Dict[str, Any], None]:
        nonlocal called
        called = True
        yield {
            'file_path': "fork/it/over",
            'adler32': 'no clue',
            'file_size': 22323,
            'file_events': 0,
        }

    init_rabbit_mq(my_callback, 'http://myrabbit.com', 'test_queue_name', retries=1,
                   retry_interval=0.1)
    rabbitmq_fail_once.send_did_request('hi-there')

    assert called


def test_auto_args_callback(init_rabbit_callback, simple_argument_parser):
    'If there is a missing argument on the command line it should cause a total failure'

    async def my_callback(did_name: str) -> AsyncGenerator[Dict[str, Any], None]:
        if False:
            yield {
                'ops': 'no'
            }

    start_did_finder("special", my_callback)

    assert init_rabbit_callback.call_args[0][1] == 'test_queue_address'
    assert init_rabbit_callback.call_args[0][2] == 'special_did_requests'


# @pytest.mark.skipif(sys.version_info < (3, 9), reason='exit_on_error only added in python 3.9')
# For SOME REASON python 3.9 is not listening to exit_on_error for this type of error
# def test_arg_required():
#     'Make sure the rabbit mq argument is required'
#     # This option is only available in 3.9, so for less than 3.9 we can't run this test.
#     parser = argparse.ArgumentParser(exit_on_error=False)
#     default_command_line_args(parser)
#     parser.add_argument('--dude', dest="sort_it", action='store')

#     with pytest.raises(argparse.ArgumentError):
#         parser.parse_args(['--dude', 'fork'])


def test_arg_required(init_rabbit_callback):
    'Make sure the argument passed on the command line makes it in'
    # This option is only available in 3.9, so for less than 3.9 we can't run this test.
    parser = argparse.ArgumentParser()
    default_command_line_args(parser)
    parser.add_argument('--dude', dest="sort_it", action='store')

    args = parser.parse_args(['--rabbit-uri', 'not-really-there'])

    async def my_callback(did_name: str) -> AsyncGenerator[Dict[str, Any], None]:
        if False:
            yield {
                'ops': 'no'
            }
    start_did_finder('test_finder', my_callback, args)

    assert init_rabbit_callback.call_args[0][1] == 'not-really-there'
