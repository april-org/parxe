- Error checking in pbs and fork engines, allowing to check when a worker has
  exited without computing its result. Once a worker fails, it is possible to
  repeat the task for a limited number of times before marking it broken.

- Add cancel operation to future or engine objects, in such a way that any job
  can be aborted using this method.
