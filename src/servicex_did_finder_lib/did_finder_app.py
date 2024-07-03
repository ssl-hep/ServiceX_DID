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
import argparse
import logging
from datetime import datetime
from typing import Any, Generator, Callable, Dict, Optional

from celery import Celery, Task

from servicex_did_finder_lib.accumulator import Accumulator
from servicex_did_finder_lib.did_logging import initialize_root_logger
from servicex_did_finder_lib.did_summary import DIDSummary
from servicex_did_finder_lib.servicex_adaptor import ServiceXAdapter
from servicex_did_finder_lib.util_uri import parse_did_uri

# The type for the callback method to handle DID's, supplied by the user.
# Arguments are:
#   - The DID to process
#   - A dictionary of information about the DID request
#   - A dictionary of arguments passed to the DID finder
UserDIDHandler = Callable[
    [str, Dict[str, Any], Dict[str, Any]],
    Generator[Dict[str, Any], None, None]
]


__logging = logging.getLogger(__name__)
__logging.addHandler(logging.NullHandler())


class DIDFinderTask(Task):
    """
    A Celery task that will process a single DID request. This task will
    call the user supplied DID finder to get the list of files associated
    with the DID, and then send that list to ServiceX for processing.
    """
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(logging.NullHandler())

    def do_lookup(self, did: str, dataset_id: int, endpoint: str, user_did_finder: UserDIDHandler):
        """
        Perform the DID lookup for the given DID. This will call the user supplied
        DID finder to get the list of files associated with the DID, and then send
        that list to ServiceX for processing.
        After all of the files have been sent, send a message to ServiceX indicating
        that the fileset is complete
        Args:
            did: The DID to process
            dataset_id: The dataset ID for the request
            endpoint: The ServiceX endpoint to send the request to
            user_did_finder: The user supplied DID finder to call to get the list of files
        """

        self.logger.info(
            f"Received DID request {did}",
            extra={"dataset_id": dataset_id}
        )

        servicex = ServiceXAdapter(dataset_id=dataset_id, endpoint=endpoint)

        info = {
            "dataset-id": dataset_id,
        }

        start_time = datetime.now()

        summary = DIDSummary(did)
        did_info = parse_did_uri(did)
        acc = Accumulator(servicex, summary)

        try:
            for file_info in user_did_finder(did_info.did, info, self.app.did_finder_args):
                acc.add(file_info)

        except Exception:
            if did_info.get_mode == "all":
                raise

        acc.send_on(did_info.file_count)

        elapsed_time = int((datetime.now() - start_time).total_seconds())
        servicex.put_fileset_complete(
            {
                "files": summary.file_count,
                "files-skipped": summary.files_skipped,
                "total-events": summary.total_events,
                "total-bytes": summary.total_bytes,
                "elapsed-time": elapsed_time,
            }
        )


class DIDFinderApp:
    """
    The main application for a DID finder. This will setup the Celery application
    and start the worker to process the DID requests.
    """
    def __init__(self, did_finder_name: str,
                 parsed_args: Optional[argparse.Namespace] = None):
        """
        Initialize the DID finder application
        Args:
            did_finder_name: The name of the DID finder
            parsed_args: The parsed command line arguments. Leave as None to use the default parser
        """

        self.name = did_finder_name
        self.parsed_args = vars(parsed_args) if parsed_args else None

        # Setup command line parsing
        if self.parsed_args is None:
            parser = argparse.ArgumentParser()
            self.add_did_finder_cnd_arguments(parser)
            self.parsed_args = vars(parser.parse_args())

        initialize_root_logger(self.name)

        self.app = Celery(f"did_finder_{self.name}", broker_url=self.parsed_args['rabbit_uri'])

        # Cache the args in the App so they are accessible to the tasks
        self.app.did_finder_args = self.parsed_args

    def did_lookup_task(self, name):
        """
        Decorator to create a new task to handle a DID lookup request wihout
        needing to know about Celery tasks.
        Usage:
            @app.did_lookup_task(name="did_finder_cern_opendata.lookup_dataset")
            def lookup_dataset(self, did: str, dataset_id: int, endpoint: str) -> None:
                self.do_lookup(did=did, dataset_id=dataset_id,
                               endpoint=endpoint, user_did_finder=find_files)

        Args:
            name: The name of the task
        """
        def decorator(func):
            @self.app.task(base=DIDFinderTask, bind=True, name=name)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper
        return decorator

    def start(self):
        self.app.worker_main(argv=['worker', '--loglevel=INFO'])

    @classmethod
    def add_did_finder_cnd_arguments(cls, parser: argparse.ArgumentParser):
        """add_did_finder_cnd_arguments Add required arguments to a parser

        If you need to parse command line arguments for some special configuration, create your
        own argument parser, and call this function to make sure the arguments needed
        for running the back-end communication are filled in properly.

        Then pass the results of the parsing to the DID Finder App's constructor method.

        Args:
            parser (argparse.ArgumentParser): The argument parser. Arguments needed for the
                                              did finder/servicex communication will be added.
        """
        parser.add_argument(
            "--rabbit-uri", dest="rabbit_uri", action="store", required=True
        )
        parser.add_argument(
            "--prefix",
            dest="prefix",
            action="store",
            required=False,
            default="",
            help="Prefix to add to use a caching proxy for URIs",
        )
