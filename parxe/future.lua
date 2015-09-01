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
local future,future_methods = class("parxe.future")

local gettime = common.gettime
local px_matrix_join = common.matrix_join

local function elapsed_time(t0_sec) return gettime() - t0_sec end

local function aborted_function() error("aborted future execution") end

local function identity_function(self, ...) return ... end

-------------------------------------------------------------------------

function future:constructor(wait_function, ready_function,
                            abort_function, post_process)
  self._wait_  = wait_function
  self._ready_ = ready_function
  self._abort_ = abort_function
  self._post_process_ = post_process or identity_function
  assert(self._wait_, "Not implemented wait function")
  assert(self._ready_, "Not implemented ready function")
end

function future_methods:wait(timeout, sleep_step)
  if not timeout or timeout == math.huge then
    self._result_ = self:_post_process_(self:_wait_())
  else
    sleep_step = sleep_step or 0.1
    local util_sleep = util.sleep
    local t0_sec = gettime()
    while not self:ready() and elapsed_time(t0_sec) < timeout do
      util_sleep(sleep_step)
    end
    if self:ready() then self._result_ = self:_post_process_(self:_wait_()) end
  end
end

function future_methods:get()
  if not self._result_ then self:wait() end
  return self._result_
end

function future_methods:ready()
  return self._result_~=nil or self:_ready_()
end

function future_methods:abort()
  assert(self._abort_, "Abort function not available for this future object")
  self:_abort()
  self._aborted_ = true
  self._wait_  = aborted_function
  self._ready_ = aborted_function
  self._abort_ = aborted_function
end

-------------------------------------------------------------------------

local all_wait_function = function(self)
  local all_matrix = true
  local result = {}
  for i,f in ipairs(self.data) do
    local values = f:get()
    if type(values):find("^matrix") then
      result[i] = values
    else
      all_matrix = false
      for _,v in ipairs(values) do
        table.insert(result, v)
      end
    end
  end
  if all_matrix then return px_matrix_join(1, result) else return result end
end

local all_ready_function = function(self)
  for i,f in ipairs(self.data) do
    if not f:ready() then return false end
  end
  return true
end

local all_abort_function = function(self)
  for i,f in ipairs(self.data) do f:abort() end
end

function future.all(tbl, post_process)
  local f = future(all_wait_function, all_ready_function,
                   all_abort_function, post_process)
  f.data = tbl
  return f
end

-------------------------------------------------------------------------

return future
