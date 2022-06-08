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
        'paths': ['root://foo.bar.ROOT'],
        'adler32': '32',
        'file_size': 1024,
        'file_events': 3141
    })

    assert len(responses.calls) == 1
    submitted = json.loads(responses.calls[0].request.body)
    assert submitted['paths'][0] == 'root://foo.bar.ROOT'
    assert submitted['adler32'] == '32'
    assert submitted['file_events'] == 3141
    assert submitted['file_size'] == 1024


@responses.activate
def test_put_file_add_bulk():
    responses.add(responses.PUT, 'http://servicex.org/files', status=206)
    sx = ServiceXAdapter("http://servicex.org")
    sx.put_file_add_bulk([{
        'paths': ['root://foo.bar.ROOT'],
        'adler32': '32',
        'file_size': 1024,
        'file_events': 3141
    }, {
        'paths': ['root://foo.bar1.ROOT'],
        'adler32': '33',
        'file_size': 1025,
        'file_events': 3142
    }])

    assert len(responses.calls) == 1
    submitted = json.loads(responses.calls[0].request.body)
    assert submitted[0]['paths'][0] == 'root://foo.bar.ROOT'
    assert submitted[0]['adler32'] == '32'
    assert submitted[0]['file_events'] == 3141
    assert submitted[0]['file_size'] == 1024

    assert submitted[0]['paths'][0] == 'root://foo.bar1.ROOT'
    assert submitted[0]['adler32'] == '33'
    assert submitted[0]['file_events'] == 3142
    assert submitted[0]['file_size'] == 1025


@responses.activate
def test_put_file_add_bulk_large():
    responses.add(responses.PUT, 'http://servicex.org/files', status=206)
    sx = ServiceXAdapter("http://servicex.org")
    sx.put_file_add_bulk([{
        'paths': ['root://foo.bar.ROOT'],
        'adler32': '32',
        'file_size': 1024,
        'file_events': 3141
    }] * 32)
    assert len(responses.calls) == 2


@responses.activate
def test_put_file_add_with_prefix():
    responses.add(responses.PUT, 'http://servicex.org/files', status=206)
    sx = ServiceXAdapter("http://servicex.org", "xcache123:")
    sx.put_file_add({
        'paths': ['root://foo.bar.ROOT'],
        'adler32': '32',
        'file_size': 1024,
        'file_events': 3141
    })

    assert len(responses.calls) == 1
    submitted = json.loads(responses.calls[0].request.body)
    assert submitted['paths'][0] == 'xcache123:root://foo.bar.ROOT'
    assert submitted['adler32'] == '32'
    assert submitted['file_events'] == 3141
    assert submitted['file_size'] == 1024


@responses.activate
def test_post_transform_start():
    responses.add(responses.POST,
                  'http://servicex.org/servicex/internal/transformation/123-456/start',
                  status=206)

    sx = ServiceXAdapter("http://servicex.org/servicex/internal/transformation/123-456")
    sx.post_transform_start()
    assert len(responses.calls) == 1
