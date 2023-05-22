#!/bin/bash

rm -rf     /tmp/zookeeper1
mkdir      /tmp/zookeeper1
echo "1" > /tmp/zookeeper1/myid
rm -rf     /tmp/zookeeper2
mkdir      /tmp/zookeeper2
echo "2" > /tmp/zookeeper2/myid
rm -rf     /tmp/zookeeper3
mkdir      /tmp/zookeeper3
echo "3" > /tmp/zookeeper3/myid
rm -rf     /tmp/zookeeper4
mkdir      /tmp/zookeeper4
echo "4" > /tmp/zookeeper4/myid # zookeeper4 is a replacement of 2.


function start_zk {
	zkhome=$1
	cd $zkhome
	./bin/zkServer.sh --config ./conf start
	cd -
}

function stop_zk {
	zkhome=$1
	cd $zkhome
	./bin/zkServer.sh --config ./conf stop
	cd -
}

function inject_fault {
	sleep 20
	stop_zk $HOME/Downloads/zknode2
	start_zk $HOME/Downloads/zknode4
}

mvn package

echo "Starting zookeepers..."
start_zk $HOME/Downloads/zknode1
start_zk $HOME/Downloads/zknode3
start_zk $HOME/Downloads/zknode2
echo "Starting zookeepers... done!"

sleep 5

echo "Launching experiment in background..."
java -cp target/lib/\*:target/\* edu.brown.cs.zkbenchmark.DynamicZooKeeperBenchmark 60000 "127.0.0.1:2181" "127.0.0.1:2182" "127.0.0.1:2183" "127.0.0.1:2184" &> run-experiment.log &
echo "Launching experiment in background... done!"

inject_fault

# Wait for zookeeper benchmark.
wait

echo "Stopping zookeepers..."
stop_zk $HOME/Downloads/zknode1
stop_zk $HOME/Downloads/zknode2 # Not needed if we don't inject a fault.
stop_zk $HOME/Downloads/zknode3
stop_zk $HOME/Downloads/zknode4 # Needed if we inject a fault.
echo "Stopping zookeepers... done!"
