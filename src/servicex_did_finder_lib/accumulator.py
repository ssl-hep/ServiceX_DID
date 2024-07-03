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
from typing import List, Dict, Any, Union

from servicex_did_finder_lib.did_summary import DIDSummary
from servicex_did_finder_lib.servicex_adaptor import ServiceXAdapter


class Accumulator:
    """Track or cache files depending on the mode we are operating in"""

    def __init__(self, sx: ServiceXAdapter, sum: DIDSummary):
        self.servicex = sx
        self.summary = sum
        self.file_cache: List[Dict[str, Any]] = []

    def add(self, file_info: Union[Dict[str, Any], List[Dict[str, Any]]]):
        """
        Track and inject the file back into the system
        :param file_info: The file information to track can be a single record or a list
        """
        if isinstance(file_info, dict):
            self.file_cache.append(file_info)
        elif isinstance(file_info, list):
            self.file_cache.extend(file_info)
        else:
            raise ValueError("Invalid input: expected a dictionary or a list of dictionaries")

    @property
    def cache_len(self) -> int:
        return len(self.file_cache)

    def send_on(self, count):
        """
        Send the accumulated files
        :param count: The number of files to send. Set to -1 to send all
        """

        # Sort the list to insure reproducibility
        files = sorted(self.file_cache, key=lambda x: x["paths"])
        self.send_bulk(files[:count])
        self.file_cache.clear()

    def send_bulk(self, file_list: List[Dict[str, Any]]):
        """
        does a bulk put of files
        :param file_list: The list of files to send
        """
        for ifl in file_list:
            self.summary.add_file(ifl)
        self.servicex.put_file_add_bulk(file_list)
