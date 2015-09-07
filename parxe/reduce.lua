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

local px_slice_reduce = function(reduce_func, slice_object, ...)
  local arg = table.pack(...)
  local agg = util.clone( table.remove(arg) )
  for i=1,#slice_object do
    agg = reduce_func(agg, slice_object[i], table.unpack(arg))
  end
  return agg
end

-- object needs should be a number or an iterable using # and [] operators
local function px_reduce(reduce_func, object, ...)
  local arg = table.pack(...)
  local init = arg[#arg]
  assert(init ~= nil, "Needs a init object as last argument")
  if class.is_a(object, future) then
    error("Not implemented for a future as input")
  else
    local engine   = config.engine()
    local futures  = {}
    local N,M,K    = common.compute_task_split(object, engine)
    local slice_reduce = px_slice_reduce
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
  return future.conditioned(bind(px_slice_reduce, reduce_func),
                            px_reduce(reduce_func, object, ...), ...)
end

return setmetatable(
  { self_distributive = px_reduce_self_distributive,
    generic = px_reduce, },
  { __call = function(self,...) return px_reduce(...) end, }
)

