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
px.config.set_engine("fork")
-- mapping a table of data
local f1 = px.map(iterator.range(1024):table(), function(x) return 2*x end)
-- mapping a matrix
local f2 = px.map(matrix(1024,20):linspace(), function(x) return stats.amean(x,1) end)
-- mapping a range of 1024 numbers
local f3 = px.map(1024, function(x) return 2*x end)
-- mapping a range of 1024 in bunches
local f4 = px.map.bunch(1024,
                        function(obj)
                          local n,a,b = #obj,obj[1],obj[#obj]
                          local m = matrix(#obj,20):linspace((a-1)*20+1,b*20)
                          return stats.amean(m,2)
end)
-- mapping a matrix in bunches
local f5 = px.map.bunch(matrix(1024,20):linspace(),
                        function(x) return stats.amean(x,2) end)
--
print(table.concat(f1:get(), ","))
print(matrix.join(0,f2:get()))
print(table.concat(f3:get(), ","))
print(matrix.join(1,f4:get()))
print(matrix.join(1,f5:get()))
