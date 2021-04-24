import argparse
import json
import logging
import time
from typing import Any, AsyncGenerator, Callable, Dict, Optional
import sys
import traceback

import pika
from make_it_sync import make_sync
from .servicex_adaptor import ServiceXAdapter

# The type for the callback method to handle DID's, supplied by the user.
UserDIDHandler = Callable[[str], AsyncGenerator[Dict[str, Any], None]]

# Given name, build the RabbitMQ queue name by appending this.
# This is backed into how ServiceX works - do not change unless it
# is also changed in the ServiceX_App
QUEUE_NAME_POSTFIX = '_did_requests'


async def run_file_fetch_loop(did: str, servicex: ServiceXAdapter, user_callback: UserDIDHandler):
    file_count = 0
    async for file_info in user_callback(did):
        file_count += 1
        servicex.put_file_add(file_info)
        if file_count == 1:
            servicex.post_preflight_check(file_info)

    if file_count == 0:
        servicex.post_status_update(f'DID Finder found zero files for dataset {did}',
                                    severity='fatal')


def rabbit_mq_callback(user_callback: UserDIDHandler, channel, method, properties, body):
    '''rabbit_mq_callback Respond to RabbitMQ Message

    When a request to resolve a DID comes into the DID finder, we
    respond with this callback. This callback will remain active
    until the request has been completed satisfied (so for some
    DID finders, this could be a fairly long time.)

    Args:
        channel ([type]): RabbitMQ channel
        method ([type]): Delivery method
        properties ([type]): Properties of the message
        body ([type]): The body (json for us) of the message
    '''
    try:
        # Unpack the message. Really bad if we fail up here!
        did_request = json.loads(body)
        logging.info(f'Received DID request {did_request}')
        did = did_request['did']
        request_id = did_request['request_id']
        servicex = ServiceXAdapter(did_request['service-endpoint'])

        # Process the request and resolve the DID
        try:
            make_sync(run_file_fetch_loop)(did, servicex, user_callback)

        except Exception as e:
            traceback.print_exc()
            _, exec_value, _ = sys.exc_info()
            servicex.post_status_update(f'DID Request Failed for id {request_id}: '
                                        f'{str(e)} - {exec_value}')
            raise

    except Exception as e:
        logging.error(f'DID request failed {str(e)}')
        traceback.print_exc()

    finally:
        channel.basic_ack(delivery_tag=method.delivery_tag)


def init_rabbit_mq(user_callback: UserDIDHandler,
                   rabbitmq_url: str, queue_name: str, retries: int, retry_interval: int):
    rabbitmq = None
    retry_count = 0

    while not rabbitmq:
        try:
            rabbitmq = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
            _channel = rabbitmq.channel()
            _channel.queue_declare(queue=queue_name)

            logging.info("Connected to RabbitMQ. Ready to start consuming requests")

            _channel.basic_consume(queue=queue_name,
                                   auto_ack=False,
                                   on_message_callback=lambda c, m, p, b:
                                   rabbit_mq_callback(user_callback, c, m, p, b))
            _channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as eek:  # type: ignore
            rabbitmq = None
            retry_count += 1
            if retry_count < retries:
                logging.error(f'Failed to connect to RabbitMQ (try #{retry_count}). '
                              f'Waiting {retry_interval} seconds before trying again {str(eek)}')
                time.sleep(retry_interval)
            else:
                print(f'Failed to connect to RabbitMQ. Giving Up after {retry_count} '
                      f'tries: {str(eek)}')
                raise


def start_did_finder(did_name: str,
                     callback: UserDIDHandler,
                     parser: Optional[argparse.ArgumentParser] = None):
    # Setup command line parsing
    parser = parser if parser is not None else argparse.ArgumentParser()
    parser.add_argument('--rabbit-uri', dest="rabbit_uri", action='store',
                        default='host.docker.internal')

    # Parse the arguments, and get the callback going
    args = parser.parse_args()

    # Start up rabbit mq and also callbacks
    init_rabbit_mq(callback,
                   args.rabbit_uri,
                   f'{did_name}{QUEUE_NAME_POSTFIX}',
                   retries=12,
                   retry_interval=10)
