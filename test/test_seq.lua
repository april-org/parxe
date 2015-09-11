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
px.config.set_engine("seq")

local f = px.map(function(x) return 2*x end, iterator.range(1024):table())
local r = f:get()
pprint(r)

local m = matrix(1024,20):linspace()

local f = px.map(function(x) return 2*x end, m)
local r = matrix.join(0,f:get())
print(r)

local f = px.map.bunch(function(x) return 2*x end, m)
local r = matrix.join(1,f:get())
print(r)

local f = px.reduce(function(a,x) return a+x end, iterator.range(1024):table(), 0)
local r = iterator(f:get()):reduce(math.add, 0)
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
