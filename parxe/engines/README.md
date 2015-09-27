# Engines

This folder contains all the engines available to PARXE. An engine is a class
which implements the methods:

- `socket = init()` allocates a socket, binding it to a transport, and returns
  the socket object.

- `abort(task)` forces termination of the given task.

- `execute(task, stdout, stderr)` runs the given task.

- `finished(task)` acknowledges about a task which is finished.

- `bool = acceptting_tasks()` indicates with true/false if the engine is
  accepting tasks for execution.

- `number = get_max_tasks()` this method returns the maximum number of
  concurrent tasks which can be executed by the engine.

Every engine is implemented in its own Lua module inside `parxe.engines` path.
The module should declare a singleton instance for the corresponding engine,
being the reference to this singleton returned by the module. Engines API is
known by `parxe.scheduler`, being the later responsible for resource acounting,
engine execution, task serialization, reply deserialization, etc.
