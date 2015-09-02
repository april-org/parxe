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

local take_slice     = common.take_slice
local px_matrix_join = common.matrix_join

local px_slice_map_table = function(slice_object, map_func, ...)
  local result = {}
  for i=1,#slice_object do result[i] = map_func(slice_object[i], ...) end
  return result
end

local type = type
local px_slice_map_matrix = function(slice_object, map_func, ...)
  local all_matrix = true
  local result = {}
  for i=1,#slice_object do
    local m = map_func(slice_object[i], ...)
    local tt = type(m)
    if tt:find("^matrix") then
      m = m:rewrap(1, table_unpack(m:dim()))
    elseif tt == "number" then
      result[i] = matrix(1,1,{m})
    else
      result[i] = m
      all_matrix = false
    end
  end
  return all_matrix and px_matrix_join(1, result) or result
end

local px_slice_map_bunch = function(slice_object, map_func, ...)
  return map_func(slice_object, ...)
end

-- object needs to be iterable using # and [] operators
local private_map = function(object, bunch, map_func, ...)
  if class.is_a(object, future) then
    error("Not implemented for a future as input")
  else
    local min_task_len = config.min_task_len()
    local engine   = config.engine()
    local futures  = {}
    local N = #object
    local M = engine:get_max_tasks() or N
    local K = math.max(math.ceil(N/M), min_task_len)
    local slice_map
    if bunch then
      slice_map = px_slice_map_bunch
    else
      slice_map = type(object):find("^matrix") and px_slice_map_matrix or px_slice_map_table
    end
    for i=1,M do
      local a,b = math.min(N,(i-1)*K)+1,math.min(N,i*K)
      if b<a then break end
      futures[i] = engine:execute(slice_map,
                                  take_slice(object, a, b),
                                  map_func,
                                  ...)
    end
    -- engine:send()
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
