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
local common = require "parxe.common"

local table_unpack   = table.unpack
local print          = print

local range_object   = common.range_object
local take_slice     = common.take_slice

local px_slice_map = function(slice_object, map_func, ...)
  local result = {}
  for i=1,#slice_object do result[i] = map_func(slice_object[i], ...) end
  april_assert(#result == 0 or #result == #slice_object,
               "Incorrect number of returned values, expected %d, found %d",
               #slice_object, #result)
  return result
end

local px_map_bunch = function(slice_object, map_func, ...)
  local result = map_func(slice_object, ...)
  april_assert(not result or #result == #slice_object,
               "Incorrect number of returned values, expected %d, found %d",
               #slice_object, #result)
  return result
end

-- object needs should be a number or an iterable using # and [] operators
local private_map = function(object, bunch, map_func, ...)
  if class.is_a(object, future) then
    error("Not implemented for a future as input")
  else
    local min_task_len = config.min_task_len()
    local engine   = config.engine()
    local futures  = {}
    local N = (type(object)=="number" and object) or #object
    local M = engine:get_max_tasks() or N
    local K = math.max(math.ceil(N/M), min_task_len)
    local slice_map = bunch and px_map_bunch or px_slice_map
    for i=1,M do
      local a,b = math.min(N,(i-1)*K)+1,math.min(N,i*K)
      if b<a then break end
      futures[i] = engine:execute(slice_map,
                                  take_slice(object, a, b),
                                  map_func,
                                  ...)
    end
    return future.all(futures)
  end
end

local px_map = function(object, ...)
  return private_map(object, false, ...)
end

local px_map_bunch = function(object, ...)
  return private_map(object, true, ...)
end

return setmetatable(
  { bunch = px_map_bunch,
    one = px_map, },
  { __call = function(self,...) return px_map(...) end, }
)
