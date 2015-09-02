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
local px     = require "parxe"
local engine = require "parxe.engines.fork"
px.config.set_engine(engine)

local m = matrix(8192,20):linspace()

local f1 = px.map(iterator.range(1024):table(), function(x) return 2*x end)
local f2 = px.map(m, function(x) return stats.amean(x) end)
local f3 = px.map.bunch(m, function(x) return stats.amean(x,2) end)

print(table.concat(f1:get(), ","))
print(f2:get())
print(f3:get())
