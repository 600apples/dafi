![test and validate](https://github.com/600apples/dafi/actions/workflows/test_and_validate.yml/badge.svg)
![publish docs](https://github.com/600apples/dafi/actions/workflows/publish_docs.yml/badge.svg)
![coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/600apples/c64b2cee548575858e40834754432018/raw/covbadge.json)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![Linux](https://svgshare.com/i/Zhy.svg)](https://svgshare.com/i/Zhy.svg)
[![macOS](https://svgshare.com/i/ZjP.svg)](https://svgshare.com/i/ZjP.svg)
[![Downloads](https://static.pepy.tech/badge/daffi/month)](https://pepy.tech/project/daffi)


Daffi's goal is to move away from the traditional server-client communication approach where processes are unequal. This concept has been replaced by the controller-node principle. Each process can contain either a controller or a node, or both.
Each daffi node gets the ability to create remote callbacks and execute callbacks created on other nodes. 

This can best be described as a multiprocessing [observer](https://refactoring.guru/design-patterns/observer) pattern where there are remote subscribers to a callback and there are publishers who can trigger those callbacks remotely.
This approach allows you to create both simple architectural solutions such as one-to-one communication between nodes and more complex schemes such as a pipeline chain or one-to-many broadcasting.
It is worth to note that the daffi's syntax is very easy. Unlike many other similar libraries, on the surface, daffi contains only a few top-level classes for initializing the process as a node or controller. 

Briefly speaking about the area of use. Daffi takes the best of several worlds and can be used as a task dispatcher or for streaming.

### Features
 
- Centralized approach to registering callbacks in microservices. You do not need to run dozens of microservers and register callbacks separately for each, which can turn into a difficult to manage architecture.
- Super fast and strong serialization/deserialization system based on [grpc](https://grpc.io/docs/) streams and [dill](https://pypi.org/project/dill/). You can serialize dataclasses, functions (with yield statements as well), lambdas, modules and many other types.
- Daffi works equally well with both synchronous and asynchronous applications. You can call asynchronous remote callback from synchronous application and vice versa. [Trio](https://trio.readthedocs.io/en/stable/) support is also included.
- Simple syntax. Calling remote callback is as simple as execution of local method. 
- Daffi can work via TCP or via UNIX socket.
- Daffi can either manage single on demand tasks or be using for streaming.
