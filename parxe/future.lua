-- PARalel eXecution Engine (PARXE) for APRIL-ANN
local common = require "parxe.common"
local future,future_methods = class("future")

local gettime = common.gettime

local function elapsed_time(t0_sec) return gettime() - t0_sec end

local aborted_function = function() error("aborted future execution") end

local function identity_function(self, ...) return ... end

-------------------------------------------------------------------------

function future.constructor(self, wait_function, ready_function,
                            abort_function, post_process)
  self._wait_  = wait_function
  self._ready_ = ready_function
  self._abort_ = abort_function
  self._post_process_ = post_process or identity_function
  assert(self._wait_, "Not implemented wait function")
  assert(self._ready_, "Not implemented ready function")
end

function future_methods.wait(self, timeout, sleep_step)
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

function future_methods.get(self)
  if not self._result_ then self:wait() end
  return self._result_
end

function future_methods.ready(self)
  return self._result_~=nil or self:_ready_()
end

function future_methods.abort(self)
  assert(self._abort_, "Abort function not available for this future object")
  self:_abort()
  self._aborted_ = true
  self._wait_  = aborted_function
  self._ready_ = aborted_function
  self._abort_ = aborted_function
end

-------------------------------------------------------------------------

local all_wait_function = function(self)
  local result = {}
  for i,f in ipairs(self.data) do
    local aux = f:wait()
    local values = aux.values
    for i,key in ipairs(aux.keys) do result[key] = values[i] end
  end
  return result
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

function future.all(self, tbl, post_process)
  local f = future(all_wait_function, all_ready_function,
                   all_abort_function, post_process)
  f.data = tbl
end

-------------------------------------------------------------------------

return future
