# ServiceX_DID

 ServiceX DID Library

## Introduction

ServiceX DID finders take a dataset name and turn them into files to be transformed. They are 
implemented as a Celery application with a task called `do_lookup`. Developers of a specific
DID Finder implementation need to write a generator function which yields a dictionary for each
file in the dataset.

The Task interacts with ServiceX through the App's REST endpoint to add files to the dataset and
a separate REST endpoint to signal that the dataset is complete.

The app caches DID lookups. The `dataset_id` is the primary key for the cache table.

Invocations of the `do_lookup` task accepts the following arguments:
* `did`: The dataset identifier to look up
* `dataset_id`: The ID of the dataset in the database
* `endpoint`: The ServiceX endpoint to send the results to
* `user_did_finder`: The user callback that is a generator function that yields file information dictionaries.

## Creating a DID Finder
You start with a new Python project. You will need to add this library as a dependency to the project
by adding the following to your `pyproject.tom` file:

```
servicex-did-finder-lib = "^3.0"
```

Create a script that will run your DID. It needs to contain your generator function that adheres
to the UserDIDHandler signature:
```python
UserDIDHandler = Callable[
    [str, Dict[str, Any], Dict[str, Any]], 
    Generator[Dict[str, Any], None, None]
]
```

The arguments to the method are straight forward:

* `did_name`: the name of the DID that you should look up. It has the schema stripped off (e.g. if the user sent ServiceX `rucio://dataset_name_in_rucio`, then `did_name` will be `dataset_name_in_rucio`)
* `info` contains a dict of various info about the database ID for this dataset.
* `did_finder_args` contains the arguments that were passed to the DID finder at startup. This is a way to pass command line arguments to your file finder

Yield the results as you find them. The fields you need to pass back to the library are as follows:

* `paths`: An ordered list of URIs that a transformer in ServiceX can access to get at the file. Often these are either `root://` or `http://` schema URI's. When accessing the file, URIs will be tried in ordered listed.
* `adler32`: A CRC number for the file. This CRC is calculated in a special way by rucio and is not used. Leave as 0 if you do not know it.
* `file_size`: Number of bytes of the file. Used to calculate statistics. Leave as zero if you do not know it (or it is expensive to look up).
* `file_events`: Number of events in the file. Used to calculate statistics. Leave as zero if you do not know it (or it is expensive to look up).

Here's a simple example of a did handler generator:

```python
def find_files(did_name: str,
               info: Dict[str, Any],
               did_finder_args: Dict[str, Any]
               ) -> Generator[Dict[str, Any], None]:
    __log.info('DID Lookup request received.', extra={
        'requestId': info['request-id'], 'dataset': did_name})

    urls = xrd.glob(cache_prefix + did_name)
    if len(urls) == 0:
        raise RuntimeError(f"No files found matching {did_name} for request "
                           f"{info['request-id']} - are you sure it is correct?")

    for url in urls:
        yield {
            'paths': [url],
            'adler32': 0,  # No clue
            'file_size': 0,  # We could look up the size but that would be slow
            'file_events': 0,  # And this we do not know
        }
```

There is a small amount of additional boilerplate code that is required to create a DID Finder. This
is the code that will create the Celery app and register your function as a task. Here is an 
example (which assumes that `find_files` is your DID handler):
```python
from servicex_did_finder_lib import DIDFinderApp

app = DIDFinderApp('cernopendata')

@app.did_lookup_task(name="did_finder_cern_opendata.lookup_dataset")
def lookup_dataset(self, did: str, dataset_id: int, endpoint: str) -> None:
    self.do_lookup(did=did, dataset_id=dataset_id,
                   endpoint=endpoint, user_did_finder=find_files)

app.start()
```

## Extra Command Line Arguments
Sometimes you need to pass additional information to your DID Finder from the command line. You do
this by creating your own `ArgParser` and then calling the `add_did_finder_cnd_arguments` method
which inserts the arguments that the library needs to pass to the finder. Here is an example:

```python
import argparse
from servicex_did_finder_lib import DIDFinderApp

parser = argparse.ArgumentParser()
parser.add_argument('--foo', dest='foo', action='store',
                    default='',
                    help='Prefix to add to Xrootd URLs')

DIDFinderApp.add_did_finder_cnd_arguments(parser)

```

You then just pass the dictionary of parsed args to your app constructor:
```python
app = DIDFinderApp('cernopendata', parsed_args=parser.parse_args())
```

These parsed args will be passed to your `find_files` function as a dictionary in 
the `did_finder_args` parameter.


### Proper Logging

In the end, all DID finders for ServiceX will run under Kubernetes. ServiceX comes with a built in logging mechanism. If anything is to be logged it should use the log system using the python standard `logging` module, with some extra information. For example, here is how to log a message from your callback function:

```python
    import logger
    __log = logger.getLogger(__name__)
    async def my_callback(did_name: str, info: Dict[str, Any]):
        __log.info(f'Looking up dataset {did_name}.',
                     extra={'somethign': info['something']})

        for i in range(0, 10):
            yield {
                'paths': [f"root://atlas-experiment.cern.ch/dataset1/file{i}.root"]
                'adler32': b183712731,
                'file_size': 0,
                'file_events': 0,
            }
```

The `DIDFinderApp` will configure the python root logger properly.

## URI Format

All the incoming DID's are expected to be URI's without the schema. As such, there are two parameters that are currently parsed by the library. The rest are let through and routed to the callback:

* `files` - Number of files to report back to ServiceX. All files from the dataset are found, and then sorted in order. The first n files are then
    sent back. Default is all files.
* `get` - If the value is `all` (the default) then all files in the dataset must be returned. If the value is `available`, then only files that are accessible need be returned.

As am example, if the following URI is given to ServiceX, "rucio://dataset_name?files=20&get=available", then the first 20 available files of the dataset will be processed by the rest of servicex.
