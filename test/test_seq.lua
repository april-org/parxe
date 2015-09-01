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
local engine = require "parxe.engines.seq"
px.config.set_engine(engine) -- not necessary

local f = px.map({1,2,3}, function(x) return 2*x end)
local r = f:get()
april_list(r)

local m = matrix(10,20):linspace()

local f = px.map(m, function(x) return 2*x end)
local r = f:get()
print(r)

local f = px.map.bunch(m, function(x) return 2*x end)
local r = f:get()
print(r)
