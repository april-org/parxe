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

-- This module implements a plain function execution using the parallel engine.
-- Functions just return one result, use table.pack for packing multiple
-- results. Communication between the function and the caller is left
-- unimplemented, you can use Xemsg! or other similar packages to communicate
-- both ends. Additionally, you can execute as many functions as you need, and
-- them can be communicated by using message communication libraries.

local config = require "parxe.config"
local future = require "parxe.future"

local function run(func, ...)
  local engine = config.engine()
  return engine:execute(func, ...)
end

return run
