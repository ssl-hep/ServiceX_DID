# ServiceX_DID

 ServiceX DID Library

## Introduction

ServiceX DID finders take a dataset name and turn them into files to be transformed. They interact with ServiceX via the Rabbit MQ message broker. As such, the integration into ServiceX and the API must be the same for all DID finders. This library abstracts away some of that interaction so that the code that interacts with ServiceX can be separated from the code that translates a dataset identifier into a list of files.

The DID Finder author need write only a line of initialization code and then a small routine that translates a DID into a list of files.

## Usage

Create an async callback method that `yield`s file info dictionaries. For example:

```python
    async def my_callback(did_name: str, info: Dict[str, Any]):
        for i in range(0, 10):
            yield {
                'file_path': f"root://atlas-experiment.cern.ch/dataset1/file{i}.root",
                'adler32': b183712731,
                'file_size': 0,
                'file_events': 0,
            }
```

The arguments to the method are straight forward:

* `did_name`: the name of the DID that you should look up. It has the schema stripped off (e.g. if the user sent ServiceX `rucio://dataset_name_in_rucio`, then `did_name` will be `dataset_name_in_rucio`)
* `info` contains a dict of various info about the request that asked for this DID:
  * `request-id` The request id that has this DID associated. For logging.

Yield the results as you find them - ServiceX will actually start processing the files before your DID lookup is finished if you do this. The fields you need to pass back to the library are as follows:

* `file_path`: A URI that a transformer in ServiceX can access to get at the file. Often these are either `root://` or `http://` schema URI's.
* `adler32`: A CRC number for the file. This CRC is calculated in a special way by rucio and is not used. Leave as 0 if you do not know it.
* `file_size`: Number of bytes of the file. Used to calculate statistics. Leave as zero if you do not know it (or it is expensive to look up).
* `file_events`: Number of events in the file. Used to calculate statistics. Leave as zero if you do not know it (or it is expensive to look up).

Once the callback is working privately, it is time to build a container. ServiceX will start the container and pass, one way or the other, a few arguments to it. Several arguments are required for the DID finder to work (like `rabbitUrl`). The library will automatically parse these during initialization.

To initialize the library and start listening for and processing messages from RabbitMQ, you must initialize the library. In all cases, the call to `start_did_finder` will not return.

If you do not have to process any command line arguments to configure the service, then the following is good enough:

```python
start_did_finder('my_finder', my_callback)
```

The first argument is the name of the schema. The user will use `my_finder://dataset_name` to access this DID lookup (and `my_callback` is called with `dataset_name` as `did_name`). Please make sure everything is lower case: schema in URI's are not case sensitive.

The second argument is the call back.

If you do need to configure your DID finder from command line arguments, then use the python `argparse` library, being sure to hook in the did finder library as follows (this code is used to start the rucio finder):

```python
    # Parse the command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--site', dest='site', action='store',
                        default=None,
                        help='XCache Site)')
    parser.add_argument('--prefix', dest='prefix', action='store',
                        default='',
                        help='Prefix to add to Xrootd URLs')
    parser.add_argument('--threads', dest='threads', action='store',
                        default=10, type=int, help="Number of threads to spawn")
    default_command_line_args(parser)

    args = parser.parse_args()

    site = args.site
    prefix = args.prefix
    threads = args.threads
    logger.info("ServiceX DID Finder starting up: "
                f"Threads: {threads} Site: {site} Prefix: {prefix}")

    # Initialize the finder
    did_client = DIDClient()
    replica_client = ReplicaClient()
    rucio_adapter = RucioAdapter(did_client, replica_client)

    # Run the DID Finder
    try:
        logger.info('Starting rucio DID finder')

        async def callback(did_name, info):
            async for f in find_files(rucio_adapter, site, prefix, threads,
                                      did_name, info):
                yield f

        start_did_finder('rucio',
                         callback,
                         parsed_args=args)

    finally:
        logger.info('Done running rucio DID finder')
```

In particular note:

1. The call to `default_command_line_args` to setup the arguments required by the finder library.
2. Parsing of the arguments using the usual `parse_args` method
3. Passing the parsed arguments to `start_did_finder`.

Another pattern in the above code that one might find useful - a thread-safe way of passing global arguments into the callback. Given Python's Global Interpreter Lock, this is probably not necessary.

### Proper Logging

In the end, all DID finders for ServiceX will run under Kubernetes. ServiceX comes with a built in logging mechanism. If anything is to be logged it should use the log system using the python standard `logging` module, with some extra information. For example, here is how to log a message from your callback function:

```python
    async def my_callback(did_name: str, info: Dict[str, Any]):
        info['log'].info(f'Looking up dataset {did_name}.',
                         extras={'requestId': info['request-id']})
        for i in range(0, 10):
            yield {
                'file_path': f"root://atlas-experiment.cern.ch/dataset1/file{i}.root",
                'adler32': b183712731,
                'file_size': 0,
                'file_events': 0,
            }
```

Note the parameter `request-id`: this marks the log messages with the request id that triggered this DID request. This will enable the system to track all log messages across all containers connected with this particular request id - making debugging a lot easier.
