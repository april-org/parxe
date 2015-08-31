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
local config = require "parxe.config"
local future = require "parxe.future"

local matrix_join = function(tbl) return matrix.join(1, tbl) end

-- object needs to be iterable using # and [] operators
local px_map = function(object, map_func, ...)
  if class.is_a(object, future) then
    error("Not implemented for a future as input")
  else
    local min_task_len = config.min_task_len()
    local engine   = config.engine()
    local map_args = table.pack(...)
    local futures  = {}
    local N = #object
    local M = engine:get_max_tasks() or N
    local K = math.max(math.ceil(N/M), min_task_len)
    for i=1,M do
      local a,b = math.min(N,(i-1)*K)+1,math.min(N,i*K)
      if b<a then break end
      futures[i] = engine:map(object, a, b, map_func, map_args)
    end
    return future.all(futures, type(object):find("^matrix") and join or nil)
  end
end

return px_map
