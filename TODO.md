- Error checking in ssh, pbs and fork engines, allowing to check when a worker
  has exited without computing its result. Once a worker fails, it is possible
  to repeat the task for a limited number of times before marking it broken and
  throwing an error.

- Add cancel operation to future or engine objects, in such a way that any job
  can be aborted using this method.

- Yield support for individual tasks, as ones executed by "px.run" function, in
  such a way workers can yield results to the server and the server can give
  parameters to the worker through join function in its corresponding future
  object.

- Use time field in future objects to control timeout or connections problems.

- In qsub engine, cancel all pending/executing jobs in PBS when exiting the
  program.

- Keep a checkpoint list at user folder which can be useful to recover from
  failures. To protect this checkpoint list the file will be keeped at
  ~/.parxe/checkpoints instead of tmp folder.
