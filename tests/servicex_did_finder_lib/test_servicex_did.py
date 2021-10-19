import json

import responses
from servicex_did_finder_lib import __version__
from servicex_did_finder_lib.servicex_adaptor import ServiceXAdapter


def test_version():
    assert __version__ == '1.0.0a1'


@responses.activate
def test_put_file_add():
    responses.add(responses.PUT, 'http://servicex.org/files', status=206)
    sx = ServiceXAdapter("http://servicex.org")
    sx.put_file_add({
        'file_path': 'root://foo.bar.ROOT',
        'adler32': '32',
        'file_size': 1024,
        'file_events': 3141
    })

    assert len(responses.calls) == 1
    submitted = json.loads(responses.calls[0].request.body)
    assert submitted['file_path'] == 'root://foo.bar.ROOT'
    assert submitted['adler32'] == '32'
    assert submitted['file_events'] == 3141
    assert submitted['file_size'] == 1024


@responses.activate
def test_put_file_add_with_prefix():
    responses.add(responses.PUT, 'http://servicex.org/files', status=206)
    sx = ServiceXAdapter("http://servicex.org", "xcache123:")
    sx.put_file_add({
        'file_path': 'root://foo.bar.ROOT',
        'adler32': '32',
        'file_size': 1024,
        'file_events': 3141
    })

    assert len(responses.calls) == 1
    submitted = json.loads(responses.calls[0].request.body)
    assert submitted['file_path'] == 'xcache123:root://foo.bar.ROOT'
    assert submitted['adler32'] == '32'
    assert submitted['file_events'] == 3141
    assert submitted['file_size'] == 1024
