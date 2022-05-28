#!/bin/bash
./run.sh alien/test/cassandra/CatalogueToCassandraThreads auto 0 50000000 /cassandra/ 100 cassandra-auto-0_500M-100t-2 2 2>&1 > stdout_Insert_500M

