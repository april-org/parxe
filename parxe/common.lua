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
-----------------------------------------------------------------------------

local range_object,range_object_methods = class("parxe.range_object")
function range_object:constructor(a,b) self.a = a self.b = b self.n = b-a+1 end
function range_object_methods:ctor_name() return 'class.find("parxe.range_object")' end
function range_object_methods:ctor_params() return self.a,self.b end
class.extend_metamethod(range_object, "__len", function(self) return self.n end)
class.declare_functional_index(range_object,
                               function(self,key)
                                 if type(key) == "number" then return self.a + key - 1 end
end)
local function ipairs_range_object(self, i)
  i = i + 1
  if i <= #self then return i,self[i] end
end
class.extend_metamethod(range_object, "__ipairs",
                        function(self) return ipairs_range_object,self,0 end)

-----------------------------------------------------------------------------

local function compute_task_split(object, engine)
  local config = require "parxe.config"
  local max_number_tasks = config.max_number_tasks()
  local min_task_len = config.min_task_len()
  local N = (type(object)=="number" and object) or #object
  local M = math.min(engine:get_max_tasks() or N, max_number_tasks)
  local K = math.max(math.ceil(N/M), min_task_len)
  return N,M,K
end

local function deserialize(f)
  local config = require "parxe.config"
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
      local config = require "parxe.config"
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

local function user_conf(filename, ...)
  local HOME = os.getenv("HOME")
  if HOME then
    local CONF = HOME.."/.parxe/default/" .. filename
    local f = io.open(CONF) if f then f:close() loadfile(CONF)(...) end
  end
end

return {
  compute_task_split = compute_task_split,
  deserialize = deserialize,
  gettime = gettime,
  make_serializer = make_serializer,
  next_task_id = next_task_id,
  range_object = range_object,
  take_slice = take_slice,
  user_conf = user_conf,
}
