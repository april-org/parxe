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

-- config variables, they are private to this module, and can be changed through
-- the exported functions

local engine            -- the parallel engine configured and selected by the
                        -- user
local max_number_tasks = 64 -- maximum number of tasks to perform a computation
                            -- (map/reduce over an object)
local min_task_len = 32 -- minimum length of any task
local tmp = "/tmp"      -- temporary directory, used by engines which need to
                        -- read/write into filesystem
local wait_step = 0.1   -- timeout in seconds used to avoid blocking calls
local working_directory -- current working directory
--
local update_wd -- a function which assigns pwd output as working_directory
do
  update_wd = function()
    local aux = io.popen("pwd")
    working_directory = aux:read("*l")
    aux:close()
  end
end
update_wd()

-- interface table to retrieve and update config variables
local api
api = {
  init = function() common.user_conf("config.lua", api) engine = engine or require "parxe.engines.seq" return api end,
  engine = function() return engine end,
  max_number_tasks = function() return max_number_tasks end,
  min_task_len = function() return min_task_len end,
  tmp = function() return tmp end,
  wait_step = function() return wait_step end,
  wd = function() return working_directory end,
  --
  set_engine = function(str) assert(type(str) == "string") engine = require ("parxe.engines."..str) end,
  set_max_number_tasks = function(n) assert(type(n) == "number") max_number_tasks = n end,
  set_min_task_len = function(n) assert(type(n) == "number") min_task_len = n end,
  set_tmp = function(str) assert(type(str) == "string") tmp = str end,
  set_wait_step = function(n) assert(type(n) == "number") wait_step = n end,
  set_wd = function(str) assert(type(str) == "string") os.execute("cd "..str) update_wd() end,
  -- This update function receives a dictionary of keys where every key is a
  -- private variable of this module. This way it is possible to update several
  -- variables at the same time.
  update = function(dict)
    for key,value in dict do
      local f = april_assert(api["set_"..key], "Unknown key %s", key)
      f(value)
    end
  end
}
return api
