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

-- This module implements map function. Two different maps are possible:
-- standard map, which traverses the given object using # and [] operator and
-- executes map function over length one slices; and bunch map which allow to
-- execute a map function over a whole slice of the input object, normally with
-- length > 1. The bunch map operation is useful to work with matrices, because
-- you can implement a map function over a whole matrix, not only a row, and it
-- can be computationally more efficient.

local future = require "parxe.future"
local common = require "parxe.common"
local sched  = require "parxe.scheduler"

local table_unpack   = table.unpack
local print          = print

local any_future     = common.any_future
local range_object   = common.range_object
local take_slice     = common.take_slice

local px_slice_map = function(map_func, slice_object, ...)
  local result = {}
  for i=1,#slice_object do result[i] = map_func(slice_object[i], ...) end
  april_assert(#result == 0 or #result == #slice_object,
               "Incorrect number of returned values, expected %d, found %d",
               #slice_object, #result)
  return result
end

local px_map_bunch = function(map_func, slice_object, ...)
  local result = map_func(slice_object, ...)
  april_assert(not result or #result == #slice_object,
               "Incorrect number of returned values, expected %d, found %d",
               #slice_object, #result)
  return result
end

-- object needs should be a number or an iterable using # and [] operators
local function private_map(bunch, map_func, object, ...)
  if any_future(object, ...) then
    return future.conditioned(bind(private_map, bunch, map_func), object, ...)
  else
    local slice_map = bunch and px_map_bunch or px_slice_map
    local futures   = {}
    local N,M,K     = common.compute_task_split(object)
    for i=1,M do
      local a,b = math.min(N,(i-1)*K)+1,math.min(N,i*K)
      if b<a then break end
      futures[i] = sched:enqueue(slice_map,
                                 map_func,
                                 take_slice(object, a, b),
                                 ...)
    end
    return future.all(futures)
  end
end

local px_map = function(...)
  return private_map(false, ...)
end

local px_map_bunch = function(...)
  return private_map(true, ...)
end

return setmetatable(
  { bunch = px_map_bunch,
    one = px_map, },
  { __call = function(self,...) return px_map(...) end, }
)
