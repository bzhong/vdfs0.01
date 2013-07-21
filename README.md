Virtual Distributed File System
========

Individual Project


Introduction

This is a project about virtual distributed file system on wide-area data centers, which, like virtual file system on PC, provides much more interoperability and scalability for many different tyeps of storage system such as distributed file system, NoSQL databases, key-value/document store and even traditional database and network file system.

There are mainly three parts of work to design and implement the functionality. I prefer regarding them as three problems to describe. They are delicious dishes as follows.

1. How to use the same access mechanism to create, fetch, update or delete data?

2. How to maintain the connectivity inside the system to allow one sub-storage system could interoperate with another? It involves with metadata management and synchronization.

3. How to make the volume of system scalable?


Installation

1. Make sure you have installed jdk/jre. 

2. Make sure you have deployed your sub-storage systems like HDFS, HBase etc.. Notice the version of jdk/jre used by all sub-storage system if applicable and this project must be the same version.

3. Generate a jar ball within the project. Notice you are responsible to add proper library like hadoop-core.jar to make the compilation successful.

4. Since the project adopts client/server mode, you decide which will be client and which will be server. Of course one can be both client and server. Put the jar ball to appropriate machines and run client/server program.

5. Miracles happen...


Development plan

Currently, the project only supports HDFS and HBase. I expect more other famous storage systems can be supported if I have spare time to code...


Summary

All approaches lie in code. Take a look at these stuff if you are interested in. Give me your valuable suggestions if you find any bad part such as code format, performance bottleneck, design framework etc.. Thanks in advance.


Author

Brady Zhong
