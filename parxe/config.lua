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

-- config variables, they are private
local block_size = 2^20
local engine
local exports = "/tmp/exports"
local min_task_len = 32
local tmp = "/tmp"
local wait_step = 0.1

-- interface table to retrieve and update config variables
local api
api = {
  init = function() engine = require "parxe.engines.seq" return api end,
  block_size = function() return block_size end,
  engine = function() return engine end,
  exports = function() assert(os.execute("mkdir -p "..exports)) return exports end,
  min_task_len = function() return min_task_len end,
  tmp = function() return tmp end,
  wait_step = function() return wait_step end,
  --
  set_block_size = function(n) assert(type(n) == "number") block_size = n end,
  set_engine = function(obj) assert(obj) assert(obj.execute) engine = obj end,
  set_exports = function(str) assert(type(str) == "string") exports = str end,
  set_min_task_len = function(n) assert(type(n) == "number") min_task_len = n end,
  set_tmp = function(str) assert(type(str) == "string") tmp = str end,
  set_wait_step = function(n) assert(type(n) == "number") wait_step = n end,
}
common.user_conf("config.lua", api)
return api
