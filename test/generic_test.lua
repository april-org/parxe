--[[
  PARalel eXecution Engine (PARXE) for APRIL-ANN
  Copyright (C) 2015  Francisco Zamora-Martinez

  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program.  If not, see <http://www.gnu.org/licenses/>.
]]
local px = require "parxe"
-- mapping a table of data
local f1 = px.map(function(x) return 2*x end, iterator.range(1024):table())
-- mapping a matrix
local f2 = px.map(function(x) return stats.amean(x,1) end, matrix(1024,20):linspace())
-- mapping a range of 1024 numbers
local f3 = px.map(function(x) return 2*x end, 1024)
local f3 = px.map(function(x) return 2*x end, f3)
-- mapping a range of 1024 in bunches
local f4 = px.map.bunch(function(obj)
                          local n,a,b = #obj,obj[1],obj[#obj]
                          local m = matrix(#obj,20):linspace((a-1)*20+1,b*20)
                          return stats.amean(m,2)
                        end, 1024)
-- mapping a matrix in bunches
local f5 = px.map.bunch(function(x) return stats.amean(x,2) end, matrix(1024,20):linspace())
local f5 = px.map(function(x) return 2*x end, f5)
--
print(table.concat(f1:get(), ","))
print(matrix.join(0,f2:get()))
print(table.concat(f3:get(), ","))
print(matrix.join(1,f4:get()))
print(matrix.join(1,f5:get()))

local f = px.reduce(function(a,x) return a+x end, iterator.range(1024):table(), 0)
local f = px.reduce(math.add, f, 0)
local r = f:get()
print(r)

local f = px.reduce.self_distributive(function(a,x) return a+x end, iterator.range(1024):table(), 0)
local r = f:get()
print(r)

local f1 = px.run(function() return matrix(1024):linspace():sum() end)
local f2 = px.run(function() return matrix(2048):linspace():sum() end)
local f  = px.future.all{f1,f2}
f:wait()
pprint(f:get())
print(f1:get())
print(f2:get())

local f1 = px.run(function() return matrix(1024):linspace():sum() end)
local f2 = px.run(function() return matrix(2048):linspace():sum() end)
local f3 = px.future.conditioned(function(x) return x/2 end,
                                 f1 + f2 + px.future.value(20))
px.config.engine():wait()
print(f1:get())
print(f2:get())
print(f3:get())

local f = px.map(function(x) return 2*x end, {1,2,3,4})
print(f)
f:wait_until_running()
print(f)
f:wait()
print(f)
pprint(f:get())

local g = px.run(function() return matrix(1024):linspace():sum() end)
local h = px.future.conditioned(function(a,x) return a*x end, 2, g)
local i = px.future.conditioned(function(a,x) return a*x end, 3, g)
local f = px.future.conditioned(function(a,b,c) return a+b+c end, h, i, 20)
print(f:get())

local f1 = px.run(function() return matrix(1024):linspace():sum() end)
local f2 = px.run(function() return matrix(2048):linspace():sum() end)
local f3 = px.future.conditioned(function(f1,f2,x) return (f1+f2+x)/2 end,
                                 f1, f2, 20)
print(f3:get())

local rnd = random(567)
local errors = matrix(iterator(range(1,1000)):map(function()return rnd:randNorm(0.0,1.0)end):table())

local boot_result = px.boot{
  size=errors:size(), R=1000, seed=1234, verbose=true, k=2,
  statistic = function(sample)
    local s = errors:index(1, sample)
    local var,mean = stats.var(s)
    return mean,var
  end
}
local boot_result = boot_result:index(1, boot_result:select(2,1):order())
local a,b = stats.boot.ci(boot_result, 0.95)
local m,p0,pn = stats.boot.percentile(boot_result, { 0.5, 0.0, 1.0 })
print(a,b,m,p0,pn)
