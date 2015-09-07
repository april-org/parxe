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
px.config.set_engine("pbs")
-- mapping a table of data
local f1 = px.map(function(x) return 2*x end, iterator.range(1024):table())
-- mapping a matrix
local f2 = px.map(function(x) return stats.amean(x,1) end, matrix(1024,20):linspace())
-- mapping a range of 1024 numbers
local f3 = px.map(function(x) return 2*x end, 1024)
-- mapping a range of 1024 in bunches
local f4 = px.map.bunch(function(obj)
                          local n,a,b = #obj,obj[1],obj[#obj]
                          local m = matrix(#obj,20):linspace((a-1)*20+1,b*20)
                          return stats.amean(m,2)
                        end, 1024)
-- mapping a matrix in bunches
local f5 = px.map.bunch(function(x) return stats.amean(x,2) end, matrix(1024,20):linspace())
--
print(table.concat(f1:get(), ","))
print(matrix.join(0,f2:get()))
print(table.concat(f3:get(), ","))
print(matrix.join(1,f4:get()))
print(matrix.join(1,f5:get()))

local f = px.reduce(function(a,x) return a+x end, iterator.range(1024):table(), 0)
local r = iterator(f:get()):reduce(math.add, 0)
print(r)

local f = px.reduce.self_distributive(function(a,x) return a+x end, iterator.range(1024):table(), 0)
local r = f:get()
print(r)
