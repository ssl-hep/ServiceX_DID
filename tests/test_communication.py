import pytest
from unittest.mock import patch, MagicMock
import json

from servicex_did.communication import init_rabbit_mq


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
