## Controller
In Daffi's parlance, the server is referred to as the `Controller.` 
This Controller operates as an intermediary and is not able to independently invoke remote callbacks. 
It can be launched within any process in which remote callbacks are registered, or it can function as a standalone application. 
The choice between these options depends on the user's specific needs. 
Typically, an application infrastructure will feature only one Controller, although multiple Controllers can be used to create isolated systems if desired.


## Node
A `Node` in Daffi is a client that runs either alongside the Controller or as a separate process. 
When a Node is running, its associated application can register callbacks and invoke the callbacks of other Nodes. 
The Node is responsible for managing all aspects of serialization/deserialization, remote callback execution, and related processes.

### Typical architecture

A typical application architecture consists of one controller and two or more nodes.
The controller can work as a standard application or share the process with a node:

![stand alone controller](images/stand-alone-controller-arch.png) 
<br /><br /><br />
Stand alone `Controller`.

This architecture is suitable where there are more than 2 nodes and complex inter-node communication is expected.
<br /><br />
<hr/>
![controller + node](images/controller-plus-node-arch.png) 
<br /><br />
`Controller` shares process with one of nodes.

This architecture is suitable for simple node-to-node requests or streams.
But also can be considered as solution when one of the processes is the leader with the ability to give commands to all other nodes
<br /><br /><br /><br /><br />


!!! warning
    If you want to initialize controller with the ability to make remote requests,
    you need to consider latter solution (`Controller shares process with one of nodes`)