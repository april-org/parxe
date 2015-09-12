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

-- range_object class, it is constructed giving two numbers (a range [a,b]) and
-- the object implements metamethods to be iterable using # and [] operators,
-- and ipairs function
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

-- Given an object (which allow operators # and []), and a parallel engine, this
-- function computes and returns the length N of the object, the number M of
-- parallel tasks to be executed and the length K of every object slice for every
-- parallel task. The length K is the ceiling of the proportion N/M, so the
-- caller needs to compute properly the length of the last task which can be
-- less than K.
local function compute_task_split(object, engine)
  local config = require "parxe.config"
  local max_number_tasks = config.max_number_tasks()
  local min_task_len = config.min_task_len()
  local N = (type(object)=="number" and object) or #object
  local M = math.min(engine:get_max_tasks() or N, max_number_tasks)
  local K = math.max(math.ceil(N/M), min_task_len)
  return N,M,K
end

-- returns a double number representing the current timestamp with a resolution
-- up to microseconds
local function gettime()
  local s,us = util.gettimeofday()
  return s + us/1.0e6
end

-- returns hostname using the same OS command
local function hostname()
  local f = assert(io.popen("hostname"), "hostname command not found")
  local HOSTNAME = f:read("*l")
  f:close()
  return HOSTNAME
end

-- function which returns the next task id number
local next_task_id
do
  local task_id = 0 function next_task_id() task_id = task_id + 1 return task_id end
end

-- generic wait method, receives a dictionary of pending_futures and executes
-- wait in everyone until all are done
local function parallel_engine_wait_method(pending_futures)
  repeat
    for id,f in pairs(pending_futures) do
      f:wait()
      pending_futures[id] = nil
    end
  until not next(pending_futures)
end

-- Given an iterable object, it returns its slice range [a,b]. If object is
-- a number, it returns a range_object(a,b)
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

-- loads configuration from system files
local function user_conf(filename, ...)
  local f,CONF
  local HOME = os.getenv("HOME")
  if HOME then
    CONF = HOME.."/.parxe/default/" .. filename
    f = io.open(CONF)
  end
  if not f then
    CONF = "/etc/parxe/default/" .. filename
    f = io.open(CONF)
  end
  if f then f:close() loadfile(CONF)(...) end
end

return {
  compute_task_split = compute_task_split,
  gettime = gettime,
  hostname = hostname,
  next_task_id = next_task_id,
  parallel_engine_wait_method = parallel_engine_wait_method,
  range_object = range_object,
  take_slice = take_slice,
  user_conf = user_conf,
}
