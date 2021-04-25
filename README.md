# ServiceX_DID

 ServiceX DID Library

## Introduction

ServiceX DID finders take a dataset name and turn them into files to be transformed. They interact with ServiceX via the Rabbit MQ message broker. As such, the integration into ServiceX and the API must be the same for all DID finders. This library abstracts away some of that interaction so that the code that interacts with ServiceX can be separated form the code that translates a dataset identifier into a list of files.

## Usage

Create an async callback method that `yield`s file info dictionaries. For example:

```python
    async def my_callback(did_name: str):
        for i in range(0, 10):
            yield {
                'file_path': f"root://atlas-experiment.cern.ch/dataset1/file{i}.root",
                'adler32': 'no clue',
                'file_size': 0,
                'file_events': 0,
            }
```

Yield the results as you find them - ServiceX will actually start processing the files before your DID lookup is finished using this scheme. Fill in `file_size` in bytes and number of events in `file_events` if you know them.

In your main script, start off the DID finder with a call similar to:

```python
start_did_finder('myfinder', my_callback)
```

This script should be run in your docker container at start up. Once running, if a user submits a query with the DID name `myfinder://dataset1`, then your callback will be called with `did_name` set to `dataset1`.

## Left to do

- what is adler32
- Understand how to deal with command line arguments for user settings
- Finish the docs once we have a demo running
- Automatic push to pypi

## Notes to keep for a short while

Add the following back into the ci for publishing

```yaml
  publish:
    needs: test
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@master

    - name: Extract tag name
      shell: bash
      run: echo "##[set-output name=imagetag;]$(echo ${GITHUB_REF##*/})"
      id: extract_tag_name

    - name: Build DID-Finder Image
      uses: elgohr/Publish-Docker-Github-Action@master
      with:
        name: sslhep/servicex-did-finder:${{ steps.extract_tag_name.outputs.imagetag }}
        username: ${{ secrets.DOCKER_USERNAME }}
        password: ${{ secrets.DOCKER_PASSWORD }}
        tag: "${GITHUB_REF##*/}"


      run: |
        poetry run coverage report
        # codecov --token=${{ secrets.CODECOV_TOKEN }}

```
