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
local common = require "parxe.common"
local config = require "parxe.config"

local PENDING_STATE = "PENDING"
local RUNNING_STATE = "RUNNING"
local READY_STATE   = "READY"
  
local VALID_STATES = {
  [PENDING_STATE] = true,
  [RUNNING_STATE] = true,
  [READY_STATE]   = true,
}

-- Future objects are the result of executing parllel functions. They doesn't
-- contain any value, but the application can be stopped waiting the value in
-- case it is necessary. This objects wrap some data needed to check when the
-- result is available. A function is given during construction to allow future
-- object adaptation to different parallel engines.
--
-- Future objects has some special fields which are responsability of the caller:
--   _result_ will contain the computation output when reday
--   _err_    will contain a string with errors captured at workers
--   _stdout_ will contain a filename where stdout can be retrieved from
--   _stderr_ will contain a filename where stderr can be retrieved from
--   _state_ contains a string with "pending", "running", "ready"
  
local future,future_methods = class("parxe.future")

-- useful functions and variables
local gettime = common.gettime
local elapsed_time = common.elapsed_time
local wait_exists = common.wait_exists
local function aborted_function() error("aborted future execution") end
local function dummy_function() end

-------------------------------------------------------------------------

-- print of any future object indicates it is a future object and its data is
-- ready or not
class.extend_metamethod(future, "__tostring", function(self)
                          return table.concat{"future in ",self:get_state()," state"}
end)

-- do_work is a function responsible for checking when data is available, and
-- responsible of setting the computation result at field _result_
function future:constructor(do_work)
  self._do_work_ = do_work or dummy_function
  self._state_ = PENDING_STATE
end

-- waits until timeout (or infinity if not given) and returns true in case data
-- is available
function future_methods:wait(timeout, sleep_step)
  timeout = timeout or math.huge
  sleep_step = sleep_step or config.wait_step()
  local util_sleep = util.sleep
  local t0_sec = gettime()
  while not self:ready() and elapsed_time(t0_sec) < timeout do
    util_sleep(sleep_step)
  end
  return self:ready()
end

-- waits until timeout (or until state is RUNNING_STATE or READY_STATE) and
-- returns the state of the object
function future_methods:wait_until_running(timeout, sleep_step)
  timeout = timeout or math.huge
  sleep_step = sleep_step or config.wait_step()
  local util_sleep = util.sleep
  local t0_sec = gettime()
  while not self:ready() and self:get_state() == PENDING_STATE and elapsed_time(t0_sec) < timeout do
    util_sleep(sleep_step)
  end
  return self:get_state()
end

-- waits until data is available and returns it
function future_methods:get()
  if not self._result_ then self:wait() end
  return self._result_
end

-- read stderr file content or _err_ field
function future_methods:get_stderr()
  if not self._result_ then self:wait() end
  local err
  if not self._stderr_ then
    err = self._err_
  else
    if wait_exists(self._stderr_, true) then
      local f = io.open(self._stderr_)
      err = f:read("*a")
      f:close()
    end
  end
  return err or ""
end

-- return stdout file content
function future_methods:get_stdout()
  if not self._result_ then self:wait() end
  if not self._stdout_ then return "" end
  local out
  if wait_exists(self._stdout_, true) then
    local f = io.open(self._stdout_)
    out = f:read("*a")
    f:close()
  end
  return out
end

-- does some work and indicates if the result is ready or not
function future_methods:ready()
  self:_do_work_()
  assert( VALID_STATES[self._state_] )
  local is_ready = self._result_ ~= nil
  if is_ready then self._state_ = READY_STATE end
  return is_ready
end

-- returns the state of the object
function future_methods:get_state()
  return assert( self._state_ )
end

-- currently this is not implemented :(
function future_methods:abort()
  assert(self._abort_, "Abort function not available for this future object")
  self._aborted_ = true
  self._do_work_ = aborted_function
end

-- future metamethods, to allow perform operations with them

class.extend_metamethod(future, "__add",
                        function(a,b)
                          return future.conditioned(lambda'|t|t[1]+t[2]',
                                                    future.all{a,b})
end)

class.extend_metamethod(future, "__sub",
                        function(a,b)
                          return future.conditioned(lambda'|t|t[1]-t[2]',
                                                    future.all{a,b})
end)

class.extend_metamethod(future, "__mul",
                        function(a,b)
                          return future.conditioned(lambda'|t|t[1]*t[2]',
                                                    future.all{a,b})
end)

class.extend_metamethod(future, "__div",
                        function(a,b)
                          return future.conditioned(lambda'|t|t[1]/t[2]',
                                                    future.all{a,b})
end)

class.extend_metamethod(future, "__mod",
                        function(a,b)
                          return future.conditioned(lambda'|t|t[1]%t[2]',
                                                    future.all{a,b})
end)

class.extend_metamethod(future, "__pow",
                        function(a,b)
                          return future.conditioned(lambda'|t|t[1]^t[2]',
                                                    future.all{a,b})
end)

class.extend_metamethod(future, "__unm",
                        function(a)
                          return future.conditioned(lambda'|a|-a', a)
end)

class.extend_metamethod(future, "__concat",
                        function(a,b)
                          return future.conditioned(lambda'|t|t[1]..t[2]',
                                                    future.all{a,b})
end)

-------------------------------------------------------------------------

-- this section declares a future.all function which returns a future object
-- configured to wait a table of future objects

-- concatenates all stdout outputs
local all_get_stdout = function(self)
  local tbl = {}
  for i,f in ipairs(self.data) do tbl[i] = f:get_stdout() end
  return table.concat(tbl,"\n")
end

-- concatenates all stderr outputs
local all_get_stderr = function(self)
  local tbl = {}
  for i,f in ipairs(self.data) do tbl[i] = f:get_stderr() end
  return table.concat(tbl,"\n")
end

-- executes ready() function over all futures, in case all are ready, it inserts
-- all the results into a table and assigns _result_ field
local all_do_work = function(self)
  local all_ready   = true
  local all_running = true
  for i,f in ipairs(self.data) do
    if not f:ready() then all_ready = false end
    if f:get_state() ~= RUNNING_STATE then all_running = false end
  end
  if all_running then self._state_ = RUNNING_STATE end
  if not all_ready then return false end
  -- the code below is executed once all futures are ready
  local result = {}
  for i,f in ipairs(self.data) do
    local values = f:get()
    if type(values):find("^matrix") then
      result[#result+1] = values
    else
      local ok = pcall(function()
          for _,v in ipairs(values) do
            table.insert(result, v)
          end
      end)
      if not ok then result[#result+1]= values end
    end
  end
  self._result_ = result
end

-- creates a future with all_do_work function and configures its get_stdout and
-- get_stderr methods, stores the given table of futures at the data field
function future.all(tbl)
  local f = future(all_do_work)
  f.get_stdout = all_get_stdout
  f.get_stderr = all_get_stderr
  f.data = tbl
  f:ready()
  return f
end

-------------------------------------------------------------------------

-- this section declares future.conditioned function, which allow to create
-- future objects which delay the execution of a function over the result
-- of another future object

-- checks the viability of data in the future object at data field
local conditioned_do_work = function(self)
  self._state_ = self.data:get_state()
  if self.data:ready() then
    self._state_ = PENDING_STATE
    for i,v in pairs(self.args) do
      if class.is_a(v,future) then self.args[i] = v:get() end
    end
    local data = self.func( table.unpack(self.args) )
    if not class.is_a(data, future) then
      self._result_ = data
    end
  end
end

-- configures a future object which runs conditioned_do_work function and
-- contains another future object
function future.conditioned(func, ...)
  local f = future(conditioned_do_work)
  f.func = func
  f.args = table.pack(...)
  local f_tbl = {}
  for i,v in pairs(f_tbl) do
    if class.is_a(v,future) then table.insert(f_tbl,v) end
  end
  f.data = future.all(f_tbl)
  f:ready()
  return f
end

-------------------------------------------------------------------------

-- a wrapper over a Lua object value
function future.value(v)
  local f = future()
  f._result_ = v
  f._state_  = READY_STATE
  return f
end

-------------------------------------------------------------------------

future.PENDING_STATE = PENDING_STATE
future.RUNNING_STATE = RUNNING_STATE
future.READY_STATE   = READY_STATE

return future
