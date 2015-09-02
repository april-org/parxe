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

-----------------------------------------------------------------------------

local range_object,range_object_methods = class("parxe.range_object")
function range_object:constructor(a,b) self.a = a self.b = b end
function range_object_methods:ctor_name() return 'class.find("parxe.range_object")' end
function range_object_methods:ctor_params() return self.a,self.b end

-----------------------------------------------------------------------------

local function deserialize(f)
  local line = f:read("*l") if not line then return end
  local n = tonumber(line)
  return util.deserialize{
    read=function(_,m)
      if n > 0 then
        m = math.min(n, m)
        local b = math.min(m, config.block_size())
        n = n - b
        return assert( f:read(b) )
      end
    end
  }
end

local function gettime()
  local s,us = util.gettimeofday()
  return s + us/1.0e6
end

local task_id,next_task_id
do
  task_id = 0 function next_task_id() task_id = task_id + 1 return task_id end
end

local function make_serializer(obj, f)
  return coroutine.wrap(
    function()
      local str = util.serialize(obj)
      local n   = #str
      assert( f:write(n) )
      assert( f:write("\n") )
      local bsize = config.block_size()
      for i=1,n,bsize do
        assert( f:write(str:sub(i,math.min(n,i+config.block_size()-1))) )
        f:flush()
        coroutine.yield()
      end
      return true
    end
  )
end

local function take_slice(obj, a, b)
  if type(obj) == "number" then return range_object(a,b) end
  if a == 1 and b == #obj then return obj end
  local object_slice
  if type(obj):find("^matrix") then
    object_slice = obj[{{a,b}}]:clone()
  else
    object_slice={} for i=a,b do object_slice[#object_slice+1] = obj[i] end
  end
  return object_slice
end

local function matrix_join(n, tbl)
  if #tbl == 1 then return tbl[1] end
  return matrix.join(n, tbl)
end

return {
  deserialize = deserialize,
  gettime = gettime,
  make_serializer = make_serializer,
  matrix_join = matrix_join,
  next_task_id = next_task_id,
  range_object = range_object,
  take_slice = take_slice,
}
