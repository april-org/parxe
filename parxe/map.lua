-- PARalel eXecution Engine (PARXE) for APRIL-ANN
local config = require "parxe.config"
local future = require "parxe.future"

local matrix_join = function(tbl) return matrix.join(1, tbl) end

-- object needs to be iterable using # and [] operators
local px_map = function(object, map_func, ...)
  if class.is_a(object, future) then
    error("Not implemented for a future as input")
  else
    local min_task_len = config.min_task_len()
    local engine   = config.engine()
    local map_args = table.pack(...)
    local futures  = {}
    local N = #object
    local M = engine:get_max_tasks() or N
    local K = math.max(math.ceil(N/M), min_task_len)
    for i=1,M do
      local a,b = math.min(N,(i-1)*K)+1,math.min(N,i*K)
      if b<a then break end
      futures[i] = engine:map(object, a, b, map_func, map_args)
    end
    return future.all(futures, type(object):find("^matrix") and join or nil)
  end
end

return px_map
