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

-- This module implements reduce operation. Two different reduce operations are
-- available. The standard reduce receives an object, a function, and splits the
-- object into several pieces which are reduced independently and in a parallel
-- way. This standard reduce returns a table with the reduction output for every
-- slice of the input object. The second one reduce operation is called
-- reduce.self_distributive, and is devoted for reduce functions which are
-- distributive, commutative and idempotent. In this case, the reduce operation
-- instead of returning a table of values, it returns a unique reduced value.

local config = require "parxe.config"
local future = require "parxe.future"
local common = require "parxe.common"

local table_unpack   = table.unpack
local print          = print

local range_object   = common.range_object
local take_slice     = common.take_slice

-- reduces using an aggregated variable and a value
local px_slice_reduce = function(reduce_func, slice_object, ...)
  local arg = table.pack(...)
  local agg = util.clone( table.remove(arg) )
  local first = 1
  if not agg then
    assert(#slice_object > 0, "Found a zero-length object, unable to reduce")
    first=2
    agg = slice_object[1]
  end
  for i=first,#slice_object do
    agg = reduce_func(agg, slice_object[i], table.unpack(arg))
  end
  return agg
end

-- reduces following a binary tree with a binary reducer
local px_slice_binary_reduce = function(reduce_func, slice_object, ...)
  local slice_object = slice_object
  while #slice_object > 1 do
    local result = {}
    for i=1,#slice_object-1,2 do
      result[#result+1] = reduce_func(slice_object[i], slice_object[i+1], ...)
    end
    if #slice_object % 2 ~= 0 then
      result[#result+1] = slice_object[#slice_object]
    end
    slice_object = result
    collectgarbage("collect")
  end
  return slice_object[1]
end

-- object needs should be a number or an iterable using # and [] operators
local function px_reduce(self_distributive, reduce_func, object, ...)
  local arg = table.pack(...)
  assert(self_distributive or arg.n > 0,
         "Needs an init value as last argument (which can be nil but should explicitly be given)")
  if class.is_a(object, future) then
    return future.conditioned(bind(px_slice_reduce, reduce_func), object, ...)
  else
    local engine   = config.engine()
    local futures  = {}
    local N,M,K    = common.compute_task_split(object, engine)
    local slice_reduce = self_distributive and px_slice_binary_reduce or px_slice_reduce
    for i=1,M do
      local a,b = math.min(N,(i-1)*K)+1,math.min(N,i*K)
      if b<a then break end
      futures[i] = engine:execute(slice_reduce,
                                  reduce_func,
                                  take_slice(object, a, b),
                                  ...)
    end
    return future.all(futures)
  end
end

local function px_reduce_self_distributive(reduce_func, object, ...)
  return future.conditioned(bind(px_slice_binary_reduce, reduce_func),
                            px_reduce(true, reduce_func, object, ...), ...)
end

return setmetatable(
  { self_distributive = px_reduce_self_distributive,
    generic = function(...) return px_reduce(false, ...) end, },
  { __call = function(self,...) return px_reduce(false, ...) end, }
)
