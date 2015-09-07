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
local future,future_methods = class("parxe.future")

local gettime = common.gettime

local function elapsed_time(t0_sec) return gettime() - t0_sec end

local function aborted_function() error("aborted future execution") end

local function dummy_function() end

-------------------------------------------------------------------------

class.extend_metamethod(future, "__tostring", function(self)
                          if self._result_ then
                            local err = self:get_stderr()
                            if err then return "future: ready with some warnings or errors" end
                            return "future: ready without errors or warnings"
                          end
                          return "future: not ready, use wait or any get method"
end)

function future:constructor(do_work)
  self._do_work_ = do_work or dummy_function
end

function future:destructor()
  if self._stdout_ then os.remove(self._stdout_) end
  if self._stderr_ then os.remove(self._stderr_) end
end

function future_methods:wait(timeout, sleep_step)
  timeout = timeout or math.huge
  sleep_step = sleep_step or config.wait_step()
  local util_sleep = util.sleep
  local t0_sec = gettime()
  while not self:ready() and elapsed_time(t0_sec) < timeout do
    util_sleep(sleep_step)
  end
end

function future_methods:get()
  if not self._result_ then self:wait() end
  return self._result_
end

function future_methods:get_stderr()
  if not self._result_ then self:wait() end
  local err
  if not self._stderr_ then
    err = self._err_
  else
    local f = io.open(self._stderr_)
    err = f:read("*a")
    f:close()
  end
  if err and #err > 0 then return err end
end

function future_methods:get_stdout()
  if not self._result_ then self:wait() end
  local f = io.open(self._stdout_)
  local out = f:read("*a")
  f:close()
  return out
end

function future_methods:ready()
  self:_do_work_()
  return self._result_ ~= nil
end

function future_methods:abort()
  assert(self._abort_, "Abort function not available for this future object")
  self._aborted_ = true
  self._do_work_ = aborted_function
end

-------------------------------------------------------------------------

local all_get_stdout = function(self)
  local tbl = {}
  for i,f in ipairs(self.data) do tbl[i] = f:get_stdout() end
  return tbl
end

local all_get_stderr = function(self)
  local tbl = {}
  for i,f in ipairs(self.data) do tbl[i] = f:get_stderr() end
  return tbl
end

local all_do_work = function(self)
  local all_ready = true
  for i,f in ipairs(self.data) do
    if not f:ready() then all_ready = false end
  end
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

function future.all(tbl)
  local f = future(all_do_work)
  f.get_stdout = all_get_stdout
  f.get_stderr = all_get_stderr
  f.data = tbl
  return f
end

-------------------------------------------------------------------------

local conditioned_do_work = function(self)
  if self.data:ready() then
    local data = self.func( self.data:get(), table.unpack(self.args) )
    if not class.is_a(data, future) then
      self._result_ = data
    end
  end
end

function future.conditioned(func, other, ...)
  assert(class.is_a(other, future), "Needs a future as second argument")
  local f = future(conditioned_do_work)
  f.func = func
  f.data = other
  f.args = table.pack(...)
  return f
end

-------------------------------------------------------------------------

return future
