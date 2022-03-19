from dataclasses import dataclass
from typing import Dict, List
import urllib


@dataclass
class ParsedDIDInfo:
    # The did to pass into the library
    did: str

    # Mode to get the files (default 'all')
    get_mode: str

    # Number of files to fetch (default '-1')
    file_count: int


def parse_did_uri(uri: str) -> ParsedDIDInfo:
    '''Parse the uri that is given to us from ServiceX, pulling out
    the components we care about, and keeping the DID that needs to
    be passed down.

    Args:
        uri (str): DID from ServiceX

    Returns:
        ParsedDIDInfo: The URI parsed into parts
    '''
    info = urllib.parse.urlparse(uri)  # type: ignore

    params = urllib.parse.parse_qs(info.query)  # type: ignore
    get_string = 'all' if 'get' not in params else params['get'][-1]
    file_count = -1 if 'files' not in params else int(params['files'][0])

    if get_string not in ['all', 'available']:
        raise ValueError('Bad value for "get" string in DID - must be "all" or "available", not '
                         f'"{get_string}"')

    for k in ['get', 'files']:
        if k in params:
            del params[k]

    def unwind_params(ps: Dict[str, List[str]]):
        for k, values in ps.items():
            for v in values:
                yield k, v

    new_query = "&".join(f'{k}={v}' for k, v in unwind_params(params))
    if len(new_query) > 0:
        new_query = "?" + new_query

    return ParsedDIDInfo(info.path + new_query, get_string, file_count)
