<!--
Copyright (c) 2012 - 2020 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

## Quick Start

This section describes how to run YCSB on a remote RocksDB instance. The machine for running the YCSB can be different from the one that runs the RocksDB instance. The only requirement is that the two machines are network reachable. The communication implementation between the two separated components relies on the [Java RMI mechanism](https://docs.oracle.com/javase/tutorial/rmi/overview.html).

NOTE: RocksDB is an embedded database and so articles like [How to run in parallel](https://github.com/brianfrankcooper/YCSB/wiki/Running-a-Workload-in-Parallel) are not applicable here.


### 1. Set Up RocksDB RMI Server

See [RMI Server for RocksDB](https://github.com/ljishen/rocksdbjni-rmi) for details.


### 2. Set Up YCSB

Clone the YCSB git repository and compile:

    git clone https://github.com/ljishen/YCSB.git
    cd YCSB
    git checkout remote-rocksdb
    mvn -pl site.ycsb:rocksdb-binding -am clean package


### 3. Run YCSB

Now you are ready to run! First, load the data:

    ./bin/ycsb load rocksdb -s -P workloads/workloada \
        -p rocksdb.dir=/tmp/ycsb-rocksdb-data \
        [-p rocksdb.optionsfile=OPTIONS_FILE \]
        [-p rocksdb.registryhost=HOSTNAME \]
        [-p rocksdb.registryport=PORT]

Then, run the workload:

    ./bin/ycsb run rocksdb -s -P workloads/workloada \
        -p rocksdb.dir=/tmp/ycsb-rocksdb-data \
        [-p rocksdb.registryhost=HOSTNAME \]
        [-p rocksdb.registryport=PORT]

NOTE: `HOSTNAME` and `PORT` should be the same parameters as to launch the RocksDB RMI server.


## RocksDB Configuration Parameters

* ```rocksdb.dir``` - (required) A path to a folder to hold the RocksDB data files.
    * EX. ```/tmp/ycsb-rocksdb-data```
* ```rocksdb.optionsfile``` - (optional) A path to a [RocksDB options file](https://github.com/facebook/rocksdb/wiki/RocksDB-Options-File).
    * EX. ```ycsb-rocksdb-options.ini```
* ```rocksdb.registryport``` - (optional) The port that runs the RocksDB RMI server.
    * If not specified explicitly, the default port number is `1099`.
* ```rocksdb.registryhost``` - (optional) The hostname or IP that runs the RocksDB RMI server.
    * If not specified explicitly, the default hostname is `localhost`.


## Note on RocksDB Options

If `rocksdb.optionsfile` is given, YCSB will apply all [RocksDB options](https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning) exactly as specified in the options file. Otherwise, YCSB will try to set reasonable defaults.
