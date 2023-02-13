!!! note
    in this section of the documentation we will cover only the basic description and basic syntax of daffi objects.
    To learn more about the use of a particular object, see the dedicated sections of this documentation.
    
    If you want to look at practical examples of how to use daffi check [example](./basic-example.md) 
    and [example with bidirectional communication](./example-bidirectional-com.md) sections of this documentation.


At the top level daffi consists of [Global](code-reference/global.md) object,
 two decorators [callback](code-reference/callback.md) and [fetcher](code-reference/fetcher.md) and [execution modifiers](execution-modifiers.md).


<table>
    <caption>Daffi objects definition</caption>
    <tr>
        <th scope="col">Daffi object</th>
        <th scope="col">Description</th>
        <th scope="col">Example</th>
    </tr>
    <tr>
        <th scope="row"><a href="./global-object.md">Global</a></th>
        <td>
        Global object is the main daffi configurator.<br/>
        This is the place where you can specify what to initialize in particular process (Controller,  Node or both),
        what kind of connection you want to have (Unix socket or TCP), specify host/port or UNIX socket path and so on.
        <br/>Global must be initialized at application startup.</td>
        <td>
        
        ```python
        from daffi import Global
        
        g = Global(host="localhost", port=8888)
        ``` 
        </td>
    </tr>
    <tr>
        <th scope="row"><a href="./callback-decorator.md">callback</a></th>
        <td>
        callback decorator is used when you want to expose function or class ir order to make it visible for other processes (daffi nodes).
        <br/>In other words callback registers decorated function/class as remote callback so that you can trigger it from remote.
        </td>
        <td>
        
        ```python
        from daffi import callback
        
        @callback
        def my_callback(arg1: int, arg2: int):
            return arg1 + arg2
        ``` 
        </td>
    </tr>
    <tr>
        <th scope="row"><a href="./fetcher-decorator.md">fetcher</a></th>
        <td>fetcher decorator describes how to execute remote callback. 
        For example if you registered function `abc` in process `A` as callback then in process `B` you need to register fetcher with the same name `abc` to execute `abc` from process `A` like it is local function.
        <br/>fetcher works in pair with execution modifiers classes.</td>
        <td>
        ```python
        from daffi import fetcher, __body_unknown__
        
        @fetcher
        def my_callback(arg1: int, arg2: int):
            __body_unknown__(arg1, arg2)
        ``` 
        </td>
    </tr>
    <tr>
        <th scope="row"><a href="./execution-modifiers.md">execution modifiers</a></th>
        <td>
        execution modifiers are set of classes that describe how to execute remote callback. 
        Possible options you can consider is trigger callback in foreground (wait for result), 
        background (allow process to wait result in background),
        use broadcast (trigger several callbacks with the same name registered in different processes at once), 
        use stream to remote callback/callbacks and more.
        </td>
        <td>
        ```python
        from daffi import fetcher, __body_unknown__, BG
        
        @fetcher(BG(return_result=True, eta=2))
        def my_callback(arg1: int, arg2: int):
            __body_unknown__(arg1, arg2)
        ``` 
        </td>
    </tr>
</table>
