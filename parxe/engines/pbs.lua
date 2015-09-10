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
local common   = require "parxe.common"
local config   = require "parxe.config"
local future   = require "parxe.future"
local xe       = require "xemsg"
local xe_utils = require "parxe.xemsg_utils"

local serialize   = xe_utils.serialize
local deserialize = xe_utils.deserialize

---------------------------------------------------------------------------
local JOB_TIMEOUT = 120 -- seconds
local TMPNAME = os.tmpname()
local HASH = TMPNAME:match("^.*lua_(.*)$")
local HOSTNAME
do
  local f = io.popen("hostname")
  HOSTNAME = f:read("*l") f:close()
end
---------------------------------------------------------------------------

local pbs,pbs_methods = class("parxe.engine.pbs")
local singleton

---------------------------------------------------------------------------

local check_worker,server
local poll_fds={}
local pending_futures = {}
local allowed_resources = { mem=true, q=true, name=true, omp=true,
                            appname=true, port=true, host=true,
                            properties = true }
local resources = { appname="april-ann", host=HOSTNAME, port=1234 }
local shell_lines = {}

---------------------------------------------------------------------------

local next_port,release_port
do
  local last_port = resources.port
  local client_ports = {}
  function next_port()
    local p = table.remove(client_ports) or last_port
    if p == last_port then last_port = last_port + 1 end
    if p == resources.port then
      return next_port()
    end
    return p
  end
  function release_port(p)
    table.insert(client_ports, p)
  end
end

local function init(singleton)
  server = assert( xe.socket(xe.AF_SP, xe.NN_PULL) )
  assert( xe.bind(server, "tcp://*:%d"%{resources.port}) )
  poll_fds[1] = { fd = server, events = xe.NN_POLLIN }
end

local function execute_qsub(task, tmpname, f)
  local port = next_port()
  local qsub_in,qsub_out = assert( io.popen2("qsub -N %s"%{resources.name or tmpname}) )
  qsub_in:write("#PBS -l nice=19\n")
  qsub_in:write("#PBS -l nodes=1:ppn=%d%s,mem=%s\n"%{resources.omp or 1,
                                                     resources.properties or "",
                                                     resources.mem or "1g"})
  if resources.q then qsub_in:write("#PBS -q %s\n"%{resources.q}) end
  qsub_in:write("#PBS -m a\n")
  qsub_in:write("#PBS -o %s\n"%{f._stdout_})
  qsub_in:write("#PBS -e %s\n"%{f._stderr_})
  for _,v in pairs(shell_lines) do qsub_in:write("%s\n"%{v}) end
  qsub_in:write("cd %s\n"%{task.wd})
  qsub_in:write("export OMP_NUM_THREADS=%d\n"%{resources.omp or 1})
  qsub_in:write("export PARXE_TASKID=%d\n"%{task.id})
  qsub_in:write("export PARXE_SERVER=%s\n"%{resources.host})
  qsub_in:write("export PARXE_SERVER_PORT=%d\n"%{resources.port})
  qsub_in:write("export PARXE_CLIENT_PORT=%d\n"%{port})
  qsub_in:write("export PARXE_HASH=%s\n"%{HASH})
  qsub_in:write("echo \"# SERVER_HOSTNAME: %s\"\n"%{HOSTNAME})
  qsub_in:write("echo \"# WORKER_HOSTNAME: $(hostname)\"\n")
  qsub_in:write("echo \"# DATE:     $(date)\"\n")
  qsub_in:write("echo \"# TASKID:   %d\"\n"%{task.id})
  qsub_in:write("echo \"# SERVER:   %s\"\n"%{resources.host})
  qsub_in:write("echo \"# PORT:     %d\"\n"%{resources.port})
  qsub_in:write("echo \"# PORT:     %d\"\n"%{port})
  qsub_in:write("echo \"# HASH:     %s\"\n"%{HASH})
  qsub_in:write("echo \"# TMPNAME:  %s\"\n"%{tmpname})
  qsub_in:write("echo \"# APPNAME:  %s\"\n"%{resources.appname})
  qsub_in:write("%s -l parxe.engines.templates.pbs_worker_script -e ''\n"%{
               resources.appname,
  })
  qsub_in:close()
  f.jobid = qsub_out:read("*l")
  f.task  = task
  f.port  = port
  qsub_out:close()
end

---------------------------------------------------------------------------

function pbs:constructor()
end

function pbs:destructor()
  os.remove(TMPNAME)
  xe.term()
end

function pbs_methods:execute(func, ...)
  local args    = table.pack(...)
  local task_id = common.next_task_id()
  local tmp = config.tmp()
  local tmpname = "%s/PX_%s_%05d_%s"%{tmp,HASH,task_id,os.date("%Y%m%d%H%M%S")}
  local f = future(check_worker)
  f._stdout_ = tmpname..".OU"
  f._stderr_ = tmpname..".ER"
  f.task_id  = task_id
  f.tmpname  = tmpname
  pending_futures[task_id] = f
  local task = { id=task_id, func=func, args=args, wd=config.wd() }
  execute_qsub(task, tmpname, f)
  return f
end

function pbs_methods:wait()
  repeat
    for task_id,f in pairs(pending_futures) do
      f:wait()
      pending_futures[task_id] = nil
    end
  until not next(pending_futures)
end

function pbs_methods:get_max_tasks() return math.huge end

function pbs_methods:set_resource(key, value)
  april_assert(allowed_resources[key], "Not allowed resources name %s", key)
  resources[key] = value
  if key == "port" then init(singleton) end
end

function pbs_methods:append_shell_line(value)
  table.insert(shell_lines, value)
end

function pbs_methods:get_hash() return HASH end

----------------------------- check worker helpers ---------------------------

local function send_task(f, client)
  local s = assert( xe.socket(xe.AF_SP, xe.NN_PUSH) )
  local b = assert( xe.connect(s, client) )
  serialize(f.task, s)
  assert( xe.shutdown(s, b) )
  f.client = client
  f.task   = nil
end

local function assign_future(pending_futures, r)
  release_port(pending_futures[r.id].port)
  pending_futures[r.id]._result_ = r.result or {false}
  pending_futures[r.id]._err_    = r.err
  pending_futures[r.id].client   = nil
  pending_futures[r.id]          = nil
end

local function process_message(pending_futures, i, s, revents)
  assert(revents == xe.NN_POLLIN)
  if revents == xe.NN_POLLIN then
    local cmd = deserialize(s)
    if cmd.hash then
      if cmd.hash ~= HASH then
        fprintf(io.stderr,"Warning: unknown hash identifier, check that every server has a different port\n")
        if cmd.client then
          send_task({ task={ id=cmd.id, func=function() end,
                             args={}, wd=config.wd() } }, cmd.client)
        end
      else
        if cmd.request then
          -- task request
          send_task(pending_futures[cmd.id], cmd.client)
        else
          assert(cmd.reply)
          -- task reply
          assign_future(pending_futures, cmd)
          if cmd.err then fprintf(io.stderr, "ERROR IN TASK %d: %s\n", cmd.id, cmd.err) end
          return true
        end
      end
    end
  end
end

------------------------ check worker function -------------------------------

function check_worker()
  local n = assert( xe.poll(poll_fds, 0.01) )
  if n > 0 then
    for i,r in ipairs(poll_fds) do
      if r.events == r.revents then
        process_message(pending_futures, i, r.fd, r.revents)
        r.revents = nil
      end
    end
  end
  collectgarbage("collect")
end

----------------------------------------------------------------------------

singleton = pbs()
class.extend_metamethod(pbs, "__call", function() return singleton end)
common.user_conf("pbs.lua", singleton)
init(singleton)
return singleton
