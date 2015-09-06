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
                          if f:ready() then
                            local err = f:get_error()
                            if err then return "future: ready with some warnings or errors" end
                            return "future: ready without errors or warnings"
                          end
                          return "future: not ready, use wait or any get method"
end)

function future:constructor(do_work)
  self._do_work_ = do_work or dummy_function
end

function future_methods:wait(timeout, sleep_step)
  self:_do_work_()
  timeout = timeout or math.huge
  sleep_step = sleep_step or config.wait_step()
  local util_sleep = util.sleep
  local t0_sec = gettime()
  while not self:ready() and elapsed_time(t0_sec) < timeout do
    util_sleep(sleep_step)
    self:_do_work_()
  end
end

function future_methods:get()
  if not self._result_ then self:wait() end
  return self._result_
end

function future_methods:get_error()
  if not self._result_ then self:wait() end
  if self._err_ and #self._err_ > 0 then return self._err_ end
  return nil
end

function future_methods:get_stdout()
  if not self._result_ then self:wait() end
  return self._stdout_
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

local all_do_work = function(self)
  local all_ready = true
  for i,f in ipairs(self.data) do
    if not f:ready() then all_ready = false end
  end
  if not all_ready then return false end
  -- the code below is executed once all futures are ready
  local result = {}
  local stdout = {}
  local err    = {}
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
    err[#err+1] = f:get_error()
    stdout[#stdout+1] = f:get_stdout()
  end
  self._result_ = result
  self._err_ = table.concat(err, "\n")
  self._stdout_ = table.concat(stdout, "\n")
end

function future.all(tbl)
  local f = future(all_do_work)
  f.data = tbl
  return f
end

-------------------------------------------------------------------------

return future
