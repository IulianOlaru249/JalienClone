#!/bin/bash
./run.sh alien/test/cassandra/CatalogueTestWhereisGenerated 0 5000000 1 10000000 40 auto-whereis-0_50-10M-40t 2 _auto 2>&1 > stdout_w1
sleep 30
./run.sh alien/test/cassandra/CatalogueTestWhereisGenerated 5000000 10000000 1 10000000 40 auto-whereis-50_100-10M-40t 2 _auto 2>&1 > stdout_w2
sleep 30
./run.sh alien/test/cassandra/CatalogueTestWhereisGenerated 10000000 15000000 1 10000000 40 auto-whereis-100_150-10M-40t 2 _auto 2>&1 > stdout_w3

