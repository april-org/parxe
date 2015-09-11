# PARXE

PARalel eXecution Engine (PARXE) for [APRIL-ANN](https://github.com/pakozm/april-ann)

PARXE is an extension written in Lua for APRIL-ANN and similar tools (it can be
adapted to work with [Torch](http://torch.ch/), we hope to develop this
adaptation layer in the future). PARXE implements functions like `map` and
`reduce` over Lua objects (see below *which* objects are available) and runs the
process automatically in a parallel environment. For instance:

```Lua
> px = require "parxe"
> f  = px.map(function(x) return 2*x end, {1,2,3,4,5,6}) -- map returns future
> t  = f:get() -- capture future value (waits until termination)
> print(table.concat(t, " "))
2 4 6 8 10 12
```

Previous code maps a table into a new table doubling every value in original
table. PARXE functions return an object of class `future`, so this objects
are not Lua values, but they will acquire the value in the *future*. To access
the value you need to call `f:get()` method, waiting the necessary time until
the value is ready and it can be safely returned to Lua.

In the same way, you can perform a reduce operation:

```Lua
> px = require "parxe"
> f  = px.reduce.self_distributive(function(a,b) return a+b end,
                                   {1,2,3,4,5,6}, 0)
> x  = f:get()
> print(x)
21
```

Notice the call to `px.reduce.self_distributive`, which indicates that the given
reducer is a commutative, associative and idempotent operation. When these three
properties are true, the operation is said to be distributive over itself. In
this case the reduce operation returns just a value.

Other reducers can be more complicated:

```Lua
> px = require "parxe"
> t  = iterator.range(256):map(function(x) return {x} end):table()
> f  = px.reduce(function(a,x) return a+x[1] end, t, 0)
> x  = f:get()
> print(table.concat(x, " "))
21
> y  = iterator(x):reduce(math.add)
> print(y)
528 1552 2576 3600 4624 5648 6672 7696
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
futures at the same time. Similarly, you can use `px.config.engine().wait()`.

```Lua
> px = require "parxe"
> f1 = px.run(function() return matrix(1024):linspace():sum() end)
> f2 = px.run(function() return matrix(2048):linspace():sum() end)
> px.config.engine().wait()
> print(f1:get())
524800
> print(f2:get())
2098176
```

## Dependencies

The "pbs" engine needs the installation of
[Xemsg!](https://github.com/pakozm/xemsg) a binding of
[nanomsg](http://nanomsg.org/) for Lua. So, first you need to have installed
libnanomsg-dev in your system and then execute:

```
$ git clone https://github.com/pakozm/xemsg.git
$ cd xemsg
$ make LUAPKG=lua5.2
$ sudo make install
```
