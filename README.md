# Geo-QA
Geo-Distributed Querying Analysis \
Authors: Andrew Hoyle & Ryan Johnsen

## Build
First, configurations are managed within app/config.ini. In this file, you can specify how many nodes you want through:
```
number_of_workers=3
```
By default, there will always be an aggregator node; workers are the nodes that are consisted in the network attached to the aggregator. 
Next, after specifying (we recommend 3), run the following commands (after the Docker engine has started) in the given order:

```bash
./system-clean.sh
./system-setup.sh
./system-nodes.sh
```
This will clean up any nodes that remain prior, set up the images (aggregator and worker using Dockerfiles), and set up the network bridge.
Finally, it will generate all the node containers (this includes aggregator and workers). 

<b> NOTE: This process will take some time especially on a weaker machine. </b> 

## Running
Once setup has finished, you can then specify the partitioning scheme and which queries you'd like to run. 
Below are some examples:

### Full Example
```bash
python3 test/manager.py -p lineitem -a 0 -m 0
Enter a SQL query file: test/test_queries/12.sql
Enter the tables the query will be run on: lineitem
```

### Uniform Partition

#### Local Multi-Primary
```bash
python3 test/manager.py -p lineitem -a 0 -m 0
```

#### Local Leader
```bash
python3 test/manager.py -p lineitem -a 1 -m 0
```

#### Distributed Multi-Primary
<b> NOTE: For "Enter the tables the query will be run on:", the table added is ignored for distributed </b>

```bash
python3 test/manager.py -p lineitem -a 0 -m 1
```

#### Distributed Leader
<b> NOTE: For "Enter the tables the query will be run on:", the table added is ignored for distributed </b>

```bash
python3 test/manager.py -p lineitem -a 1 -m 1
```

### Smart Partition
<b> NOTE: `-p` is simply ignored when smart partitioning is enabled. </b>

```bash
python3 test/manager.py -p lineitem -a 0 -m 0 test/test_queries/12.sql 
```

#### Local Leader
```bash
python3 test/manager.py -p lineitem -a 1 -m 0 test/test_queries/12.sql 
```

#### Distributed Multi-Primary
<b> NOTE: For "Enter the tables the query will be run on:", the table added is ignored for distributed </b>

```bash
python3 test/manager.py -p lineitem -a 0 -m 1 test/test_queries/12.sql 
```

#### Distributed Multi-Primary
<b> NOTE: For "Enter the tables the query will be run on:", the table added is ignored for distributed </b>

```bash
python3 test/manager.py -p lineitem -a 1 -m 1 test/test_queries/12.sql 
```

## Cleanup
We recommend running `./system-clean.sh` to reset and clean up the system for different setups and query support. Further, `docker system prune -a` and `docker volume prune` to clean up Docker instances and volumes (these can take up space).

