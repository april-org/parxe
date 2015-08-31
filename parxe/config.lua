-- PARalel eXecution Engine (PARXE) for APRIL-ANN

-- config variables, they are private
local engine
local exports = "/tmp/exports"
local min_task_len = 32
local tmp = "/tmp"

-- interface table to retrieve and update config variables
local api
api = {
  init = function() engine = require "parxe.engines.fork" return api end,
  engine = function() return engine end,
  exports = function() assert(os.execute("mkdir -p "..exports)) return exports end,
  min_task_len = function() return min_task_len end,
  tmp = function() return tmp end,
  --
  set_engine = function(obj) assert(obj) assert(obj.execute) engine = obj end,
  set_exports = function(str) assert(type(str) == "string") exports = str end,
  set_min_task_len = function(n) assert(type(n) == "number") min_task_len = n end,
  set_tmp = function(str) assert(type(str) == "string") tmp = str end,
}
return api
