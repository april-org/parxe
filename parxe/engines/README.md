# Engines

This folder contains all the engines available to PARXE. An engine is a class
which implements the methods:

- `future = execute(func, ...)` which receives a function as long as a variadic
  list of arguments, and returns a future object which allow to keep track of
  the result as soon as it is available. The future object should be manually
  configured with all the data needed by the engine. The engine has the
  responsibility to fill fields `_result_`, `_err_ `, `_stdout_`, `_stderr_` and
  `_state_`.


- `wait()` every engine should keep track of all futures pending and under its
  responsibility, so the `wait()` just goes through the whole list waiting
  all futures to be ready.

- `number = get_max_tasks()` this method returns the maximum number of
  concurrent tasks which can be executed by the engine.

Every engine is implemented in its own Lua module inside `parxe.engines` path.
The module should declare a singleton instance for the corresponding engine,
being the reference to this singleton returned by the module.
