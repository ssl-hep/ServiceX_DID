import json

import requests
import responses
from servicex_did_finder_lib.servicex_adaptor import ServiceXAdapter


@responses.activate
def test_put_file_add_bulk():
    call_count = 0

    def request_callback(request):
        nonlocal call_count
        call_count += 1

        if call_count == 1:
            raise requests.exceptions.ConnectionError("Connection failed")
        else:
            return (206, {}, "")

    responses.add_callback(responses.PUT,
                           'http://servicex.org/12345/files',
                           callback=request_callback)

    sx = ServiceXAdapter("http://servicex.org/", '12345')
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

    assert len(responses.calls) == 1 + 1  # 1 retry
    submitted = json.loads(responses.calls[0].request.body)
    assert submitted[0]['paths'][0] == 'root://foo.bar.ROOT'
    assert submitted[0]['adler32'] == '32'
    assert submitted[0]['file_events'] == 3141
    assert submitted[0]['file_size'] == 1024

    assert submitted[1]['paths'][0] == 'root://foo.bar1.ROOT'
    assert submitted[1]['adler32'] == '33'
    assert submitted[1]['file_events'] == 3142
    assert submitted[1]['file_size'] == 1025


@responses.activate
def test_put_file_add_bulk_large():

    responses.add(responses.PUT, 'http://servicex.org/12345/files', status=206)

    sx = ServiceXAdapter("http://servicex.org/", '12345')
    sx.put_file_add_bulk([{
        'paths': ['root://foo.bar.ROOT'],
        'adler32': '32',
        'file_size': 1024,
        'file_events': 3141
    }] * 320)
    assert len(responses.calls) == 2  # No retries


@responses.activate
def test_put_file_add_bulk_failure():

    def request_callback(request):
        raise requests.exceptions.ConnectionError("Connection failed")

    responses.add_callback(responses.PUT,
                           'http://servicex.org/12345/files',
                           callback=request_callback)

    sx = ServiceXAdapter("http://servicex.org/", '12345')
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

    assert len(responses.calls) == 3  # Max retries


@responses.activate
def test_put_file_complete():
    call_count = 0

    def request_callback(request):
        nonlocal call_count
        call_count += 1

        if call_count == 1:
            raise requests.exceptions.ConnectionError("Connection failed")
        else:
            return (206, {}, "")

    responses.add_callback(responses.PUT,
                           'http://servicex.org/12345/complete',
                           callback=request_callback)

    sx = ServiceXAdapter("http://servicex.org/", '12345')
    sx.put_fileset_complete({
        "files": 100,
        "files-skipped": 0,
        "total-events": 1000,
        "total-bytes": 10000,
        "elapsed-time": 10
    })
    assert len(responses.calls) == 1 + 1  # 1 retry
    submitted = json.loads(responses.calls[0].request.body)
    assert submitted['files'] == 100
    assert submitted['files-skipped'] == 0
    assert submitted['total-events'] == 1000
    assert submitted['total-bytes'] == 10000
    assert submitted['elapsed-time'] == 10


@responses.activate
def test_put_file_complete_failure():

    def request_callback(request):
        raise requests.exceptions.ConnectionError("Connection failed")

    responses.add_callback(responses.PUT,
                           'http://servicex.org/12345/complete',
                           callback=request_callback)

    sx = ServiceXAdapter("http://servicex.org/", '12345')
    sx.put_fileset_complete({
        "files": 100,
        "files-skipped": 0,
        "total-events": 1000,
        "total-bytes": 10000,
        "elapsed-time": 10
    })
    assert len(responses.calls) == 3  # Max retries
