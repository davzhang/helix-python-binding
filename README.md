# Python Binding for Apache Helix

## Apache Helix

Apache Helix is a generic cluster management framework used for the automatic management of partitioned, replicated and distributed resources hosted on a cluster of nodes.
For more information, visit http://helix.incubator.apache.org

## Python Binding for Apache Helix

A PARTICIPANT for Helix can join the cluster as a worker node and respond to the messages sent by Helix CONTROLLER. With the Python binding, one can write worker code in python.

## Installation requirements

### Install [Kazoo](https://github.com/python-zk/kazoo) is used to interact with Zookeeper. 

  $ sudo pip install kazoo
   
### Install future packages for python 2

  download tar.gz from http://pypi.python.org/pypi/futures
  expand it and then
  ```
   setup.py build
   sudo setup.py install
  ```

## Example 

### setup the a cluster as shown in 

    http://helix.incubator.apache.org/Quickstart.html

### start the Example Participant 

    python org/apache/helix/examples/ExampleProcess.py --zkSvr localhost:2199 --cluster mycluster --host localhost --port 12913 --stateModelType MasterSlave

