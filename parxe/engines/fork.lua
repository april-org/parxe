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
local future = require "parxe.future"
local lock   = require "parxe.lock"
local pipe   = require "parxe.pipe"

---------------------------------------------------------------------------

local fork,fork_methods = class("parxe.engine.fork")
local singleton
local num_cores = tonumber( assert( io.popen("getconf _NPROCESSORS_ONLN") ):read("*l") )
local scheduler
local pipes = {}
local which,pid
do
  local locks = {}
  local deserialize = common.deserialize
  local make_serializer = common.make_serializer
  
  for i=1,num_cores do pipes[i],locks[i] = pipe(),lock() end
  
  local function run_worker(stream, lock)
    -- remove from global table sensible functions
    while true do
      local task = deserialize(stream) if not task then break end
      local func, args, id = task.func, task.args, task.id
      -- FIXME: MEMORY LEAK POSSIBLE WHEN ERROR IS PRODUCED
      local ok,result = xpcall(func,debug.traceback,table.unpack(args))
      local err = nil
      if not ok then
        err,result = result,{}
        -- FIXME: Is it necessary? singleton = fork()
      end
      lock:make() -- mark as job done
      local serializer = make_serializer({ result=result, id=id, err=err }, stream)
      repeat until serializer()
    end
  end

  local os_exit = os.exit
  local child_gc
  local function child_process(i, pid)
    -- prepare pipes and proper garbage collection
    for j=1,num_cores do
      if j~= i then pipes[j]:close() end
    end
    local stream = pipes[i]:set_as_child()
    local gc = function() stream:close() util.wait() end
    child_gc = setmetatable({}, { __gc = gc })
    run_worker(stream, locks[i]) -- RUN WORKER, RUN!!!
    gc() -- we need explicit call to garbage collection procedure because...
    os_exit(0) -- exist doesn't call it
  end
  
  local function build_scheduler(pipes, locks)
    local in_queue,out_queue = {},{}
    local idle = {} for i=1,#pipes do idle[i] = true end
    local serializer_coroutines = {}
    --
    local function execute_next()
      local all_idle = true
      for i=1,#idle do
        if idle[i] and #in_queue > 0 then
          local serializer = make_serializer(table.remove(in_queue, 1), pipes[i])
          table.insert(serializer_coroutines, serializer)
          idle[i] = false
          all_idle = false
        end
      end
      return all_idle
    end
    local function read_next_result()
      local any_ready = false
      for i=1,#locks do
        local lock  = locks[i]
        local ready = lock:check()
        if ready then
          lock:remove()
          table.insert(out_queue, deserialize(pipes[i]))
          any_ready,idle[i] = true,true
        end
      end
      return any_ready
    end
    --
    local function check_result()
      local ready = read_next_result()
      execute_next()
      return #out_queue>0
    end
    local function push_task(id, func, args)
      table.insert(in_queue, {func=func, args=args, id=id})
      read_next_result()
      execute_next()
    end
    local function pop_result()
      read_next_result()
      execute_next()
      return table.remove(out_queue, 1)
    end
    local function empty()
      local ready = read_next_result()
      local all_idle = execute_next()
      return not ready and all_idle and #in_queue == 0 and #out_queue == 0
    end
    local function do_serialization()
      local new_list = {}
      for _,serializer in ipairs(serializer_coroutines) do
        if not serializer() then table.insert(new_list, serializer) end
      end
      serializer_coroutines = new_list
      return #serializer_coroutines == 0
    end
    return push_task,check_result,pop_result,empty,do_serialization
  end
  
  which,pid = util.split_process(num_cores + 1)
  if which > 1 then
    child_process(which - 1, pid)
  else
    for i=1,num_cores do pipes[i]:set_as_parent() end
    local push,check,pop,empty,do_serialization=build_scheduler(pipes, locks)
    scheduler = { push_task = push,
                  check_result = check,
                  pop_result = pop,
                  empty = empty,
                  do_serialization = do_serialization }
  end
end

---------------------------------------------------------------------------

local check_worker
local pending_futures = {}

---------------------------------------------------------------------------

function fork:constructor()
end

function fork:destructor()
  for i=1,num_cores do pipes[i]:close() end
  util.wait()
end

function fork_methods:execute(func, ...)
  local args = table.pack(...)
  local task_id = common.next_task_id()
  local f = future(check_worker)
  f.task_id = task_id
  pending_futures[task_id] = f
  scheduler.push_task(task_id, func, args)
  return f
end

function fork_methods:wait()
  repeat
    for task_id,f in pairs(pending_futures) do
      f:wait()
      pending_futures[task_id] = nil
    end
  until not next(pending_futures)
end

function fork_methods:get_max_tasks() return num_cores end

function check_worker()
  repeat until scheduler.do_serialization()
  while scheduler.check_result() do
    local r = assert( scheduler.pop_result() )
    pending_futures[r.id]._result_ = r.result or true
    if r.err then fprintf(io.stderr, r.err) end
  end
end

----------------------------------------------------------------------------

singleton = fork() -- local variable taken from the header of this file
class.extend_metamethod(fork, "__call", function() return singleton end)
return singleton
