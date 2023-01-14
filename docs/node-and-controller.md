## Controller
In daffi terminology server is called `Controller`.

The `Controller` behaves as a broker and cannot call remote callbacks on its own.
`Controller` can be run in any of the processes where remote callbacks are registered, 
but it can also work as stand-alone application. Both variants are fine depends on your requirements.
Usually application infrastructure includes only one `Controller` but it is possible to have several 
if you want to create several isolated systems.


## Node
`Node` is client that is running along with `Controller` or as stand-alone process.

The application where the `Node` is running gets the opportunity to register callbacks and call the callbacks of other nodes.
All serialization/deserialization of messages, remote callbacks executions etc is `Node` work.
