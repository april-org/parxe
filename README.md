# PARXE

PARalel eXecution Engine (PARXE) for [APRIL-ANN](https://github.com/pakozm/april-ann)

PARXE is an extension written in Lua for APRIL-ANN and similar tools (it can be
adapted to work with [Torch](http://torch.ch/), we hope to develop this
adaptation layer in the future). PARXE implements functions like `map` and
`reduce` over Lua *iterable objects* and runs the process automatically in a
parallel environment. An iterable object is such an object implementing `__len`
and `__index` metamethods, that is, operator `#` and operator `[i]` with `i` a
positional index are valid. For instance:

```Lua
> px = require "parxe"
> f  = px.map(function(x) return 2*x end, {1,2,3,4,5,6}) -- map returns future
> t  = f:get() -- capture future value (waits until termination)
> print(table.concat(t, " "))
2 4 6 8 10 12
```

Previous code maps a table into a new table doubling every value in original
table. As an exception, PARXE accepts *numbers* as iterable objects, being it an
implicit array of sequential numbers. Therefore, the following code is equivalent to
previous one:

```Lua
> px = require "parxe"
> f  = px.map(function(x) return 2*x end, 6) -- map returns future
> t  = f:get() -- capture future value (waits until termination)
> print(table.concat(t, " "))
2 4 6 8 10 12
```

PARXE functions return an object of class `future`, so this objects
are not Lua values, but they will acquire the value in the *future*. To access
the value you need to call `f:get()` method, waiting the necessary time until
the value is ready and it can be safely returned to Lua.

In the same way, you can perform a reduce operation:

```Lua
> px = require "parxe"
> f  = px.reduce.self_distributive(function(a,b) return a+b end,
                                   {1,2,3,4,5,6})
> x  = f:get()
> print(x)
21
```

Notice the call to `px.reduce.self_distributive`, which indicates that the given
reducer is a commutative, associative and idempotent operation. When these three
properties are true, the operation is said to be distributive over itself. In
this case the reduce operation returns just a value and reduction is performed
by halving the giving object by two in a binary tree of operations.

Other reducers can be more complicated, so they are not distributive over
itself, and reduce operation receives an aggregated value and a new value to
aggregate. This reductions should receive an initial value as last argument and
returns a table of intermediate aggregated values which need to be reduced
again:

```Lua
> px = require "parxe"
> t  = iterator.range(256):map(function(x) return {x} end):table()
> f  = px.reduce(function(a,x) return a+x[1] end, t, 0)
> x  = f:get()
> print(table.concat(x, " "))
528 1552 2576 3600 4624 5648 6672 7696
> y = iterator(x):reduce(math.add, 0)
> print(y)
32896
```

Notice that initial value can be `nil` but should be given **explicitly**. In
case of given `nil` as last argument, the operation takes the first slice of the
object as the initial value of the aggregation. This can be useful for certain
reduce functions where you don't know the properties of the aggregation result
(for instance, when reducing a matrix, the shape of the aggregated matrix can be
unknown):

```Lua
> px = require "parxe"
> t  = iterator.range(256):map(function(x) return {x} end):table()
> f  = px.reduce(function(a,x) return a+x[1] end, t, nil)
> x  = f:get()
> print(table.concat(x, " "))
528 1552 2576 3600 4624 5648 6672 7696
> y = iterator(x):reduce(math.add, 0)
> print(y)
32896
```

Besides map and reduce, you can run any function into the parallel environment.
This can be done by means of `px.run()` function, whose arguments are a
function and a variable list of arguments received by the function.

```Lua
> px = require "parxe"
> f1 = px.run(function() return matrix(1024):linspace():sum() end)
> f2 = px.run(function() return matrix(2048):linspace():sum() end)
> f  = px.future.all{f1,f2}
> f:wait()
> print(f1:get())
524800
> print(f2:get())
2098176
```
You can use `px.future.all()` which receives an array of futures to wait several
futures at the same time. Similarly, you can use `px.scheduler:wait()`.

```Lua
> px = require "parxe"
> f1 = px.run(function() return matrix(1024):linspace():sum() end)
> f2 = px.run(function() return matrix(2048):linspace():sum() end)
> px.scheduler:wait()
> print(f1:get())
524800
> print(f2:get())
2098176
```

Future objects allow math operations, and the output of the operation is another
future. Be careful, you only can operate with two futures, it is not possible to
operate using a future and a non future object. In case you need this behavior,
you can use the wrapper `px.future.value`:

```Lua
> px = require "parxe"
> fv = px.future.value
> f1 = px.run(function() return matrix(1024):linspace():sum() end)
> f2 = px.run(function() return matrix(2048):linspace():sum() end)
> f3 = f1 + f2 + fv(20)
> px.scheduler:wait()
> print(f3:get())
2622996
```

Even more, it is possible to condition the execution of a function to the
evaluation of a list of future object or values, using `px.future.conditioned`
as in:

```Lua
> px = require "parxe"
> fv = px.future.value
> fc = px.future.conditioned
> f1 = px.run(function() return matrix(1024):linspace():sum() end)
> f2 = px.run(function() return matrix(2048):linspace():sum() end)
> f3 = fc(function(f1,f2,a) return (f1+f2+a)/2 end, f1, f2, 20)
> px.scheduler:wait()
> print(f3:get())
1311498
```

Finally, the bootstrapping function `px.boot()` has been added to perform
resampling using large computation clusters. This function is a replacement of
`stats.boot()` function in APRIL-ANN, and it can be used as follows:

```Lua
> px = require "parxe"
> rnd = random(567)
> errors = stats.dist.normal():sample(rnd,1000)
> boot_result = px.boot{
  size=errors:size(), R=1000, seed=1234, verbose=true, k=2,
  statistic = function(sample_indices)
    local s = errors:index(1, sample_indices)
    local var,mean = stats.var(s)
    return mean,var
  end
}
> boot_result = boot_result:index(1, boot_result:select(2,1):order())
> a,b = stats.boot.ci(boot_result, 0.95)
> print(a,b)
-0.073265952430665	0.051443199906498
> m,p0,pn = stats.boot.percentile(boot_result, { 0.5, 0.0, 1.0 })
> print(m,p0,pn)
-0.012237208895385	-0.11794531345367	0.09270916134119
```

The function `px.boot()` receives a table with this fields:

- `size=number or table` the sample population size, or a table with several
  sample population sizes.

- `R=number` the number of repetitions of the procedure.

- `k=number` the number of results returned by `statistic` function. **By
  default** it is 1.

- `statistic=function` a function which receives a `matrixInt32` with a list of
  indices for resampling the data. The function can compute a number of k
  statistics (with k>=1), being returned as multiple results.

- `verbose=false` an **optional** boolean indicating if you want or not a
  verbose output. By default it is `false`.

- `seed=1234` an **optional** number indicating the initial seed for random
  numbers, by default it is `1234`.

- `random` an **optional** random number generator. Fields `seed`
  and `random` are forbidden together, only one can be indicated.

## Scheduler

The scheduler is responsible to queue tasks and deliver them to the selected
engine.

## Engines

PARXE can be extended by different parallel engines. They can be configured
using the function `px.config.set_engine(ENGINE)` given in `ENGINE` a Lua string
with the name of the particular engine you want to use. Currently there are
three engines available:

- "seq" is not a parallel engine, it just executes every command as
  soon as they are requested. It uses
  [Xemsg!](https://github.com/pakozm/xemsg) with nanomsg INPROC
  transport to be compatible with parallel engines API.

- "local" it uses `nohup` to execute as many workers as cores has the local
  machine. It uses [Xemsg!](https://github.com/pakozm/xemsg) with nanomsg IPC
  transport to communication between processes.

- "pbs" it uses `qsub` to execute as many workers as needed in a PBS cluster. It
  uses [Xemsg!](https://github.com/pakozm/xemsg) with nanomsg TCP transport to
  communication between processes.

- "ssh" uses `ssh` with private/public key credentials to run commands in remote
  hosts. It uses [Xemsg!](https://github.com/pakozm/xemsg) with nanomsg TCP
  transport to communication between processes. This engine needs at least
  one machine with one core to work, so you need to execute at least once
  `px.config.engine():add_machine(login,num_cores)`.

## Default configuration

Some configuration parameters can be setup before the execution of any command
by PARXE. This can be done writing into file `$HOME/.parxe/default/config.lua`
something similar to:

```Lua
local config = ...
-- Where your stdout and stderr will be written during PARXE execution. All
-- written files will be cleaned-up before exiting.
config.set_tmp("/home/public/tmp")
-- The engine you want to use by default.
config.set_engine("pbs")
```

Other things which can be configured are:

- `config.set_clean_tmp_at_exit(boolean)` indicates if remove all stdout and
  stderr files produced by task execution.

- `config.set_engine(ENGINE)` receives the name of the engine, a string with
  any of these values: "seq", "local", "pbs", "ssh".

- `config.set_max_number_tasks(N)` sets a maximum number of concurrent tasks
  you want to execute. By default it is set to 64. This number will prevail over
  the maximum number of tasks declared by the engine.

- `config.set_min_task_len(N)` map/reduce commands are split into slices with a
  minimum length number, which can be configured with this function. By default
  it is 32.

- `config.set_tmp(PATH)` changes the temporary directory used by PARXE. For
  cluster engines, like "pbs" or "ssh", it should be a shred folder between all
  your machines. In this folder stdout and stderr outputs will be written.

- `config.set_wait_step(SECONDS)` in order to not block the execution of Lua,
  all operations are performed with a wait timeout. The default value is
  0.1 seconds.

- `config.set_wd(PATH)` changes the working directory where PARXE workers will
  be executed. By default it is given by `pwd` OS  command.

### PBS engine configuration

Additionally, some engines, like the "pbs" engine, use resources from your
computer or cluster which can be configured beforehand. To do that, you can
write into `$HOME/.parxe/default/pbs.lua` a file containing something like:

```Lua
local pbs = ...
pbs:append_shell_line(". /etc/my-env-vars")
pbs:set_resource("q", "short")    -- PBS queue name
pbs:set_resource("name", "TEST")  -- Default name of qsub jobs
pbs:set_resource("omp", 1)        -- Number of OMP threads
pbs:set_resource("appname", "$APRIL_EXEC")
```

In "pbs" engine you can execute as many `append_shell_line()` as you need.
All this lines will be enqueued and executed in order just before execution
of the worker application. The PBS resources available to be configured are:
`mem`, `q`, `name`, `omp`, `appname`, `host`, `properties`. All of them are
numbers or just a string with the resource, except properties which is a table
of strings. Keep in mind that `appname` accepts an environment variable which
will be expanded by the worker machine.

### SSH engine configuration

In "ssh" engine you can write a configure file at
`$HOME/.parxe./default/ssh.lua` where it is possible to execute
`append_shell_line()`, `set_resource()` with keys `omp`, `appname`, `host`, and
`add_machine()` function. Keep in mind that `appname` accepts an environment
variable which will be expanded by the worker machine. For instance, the
configuration script could be:

```Lua
local ssh = ...
ssh:append_shell_line(". /etc/my-env-vars")
ssh:set_resource("omp", 1)
ssh:set_resource("appname", "$APRIL_EXEC")
```

## Dependencies

PARXE needs the installation of [Xemsg!](https://github.com/pakozm/xemsg), a
binding of [nanomsg](http://nanomsg.org/) for Lua. So, first you need to have
installed libnanomsg-dev in your system and then execute:

```
$ git clone https://github.com/pakozm/xemsg.git
$ cd xemsg
$ make LUAPKG=lua5.2
$ sudo make install
```
