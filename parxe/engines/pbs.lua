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
local TMPNAME  = os.tmpname()
local HASH     = TMPNAME:match("^.*lua_(.*)$")
local HOSTNAME = common.hostname()
---------------------------------------------------------------------------

local pbs,pbs_methods = class("parxe.engine.pbs")
local singleton

---------------------------------------------------------------------------

local check_worker,server
local poll_fds = {}
-- A table with all the futures related with executed processes. The table is
-- indexed as a dictionary using PBS jobids as keys.
local pending_futures = {}
local allowed_resources = { mem=true, q=true, name=true, omp=true,
                            appname=true, host=true,
                            properties = true }
local resources = { appname="april-ann", host=HOSTNAME, port=1234 }
local shell_lines = {}

---------------------------------------------------------------------------

-- initializes the nanomsg SP socket for REQ/REP pattern
local function init()
  server = assert( xe.socket(xe.AF_SP, xe.NN_REP) )
  assert( xe.bind(server, "tcp://*:%d"%{resources.port}) )
  poll_fds[1] = { fd = server, events = xe.NN_POLLIN }
end

-- Executes qsub passing it the worker script and resources
-- configuration. Returns the jobid of the queued worker.
local function execute_qsub(wd, tmpname, f)
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
  qsub_in:write("cd %s\n"%{wd})
  qsub_in:write("export OMP_NUM_THREADS=%d\n"%{resources.omp or 1})
  qsub_in:write("export PARXE_SERVER=%s\n"%{resources.host})
  qsub_in:write("export PARXE_SERVER_PORT=%d\n"%{resources.port})
  qsub_in:write("export PARXE_HASH=%s\n"%{HASH})
  qsub_in:write("echo \"# SERVER_HOSTNAME: %s\"\n"%{HOSTNAME})
  qsub_in:write("echo \"# WORKER_HOSTNAME: $(hostname)\"\n")
  qsub_in:write("echo \"# DATE:     $(date)\"\n")
  qsub_in:write("echo \"# SERVER:   %s\"\n"%{resources.host})
  qsub_in:write("echo \"# PORT:     %d\"\n"%{resources.port})
  qsub_in:write("echo \"# HASH:     %s\"\n"%{HASH})
  qsub_in:write("echo \"# TMPNAME:  %s\"\n"%{tmpname})
  qsub_in:write("echo \"# APPNAME:  %s\"\n"%{resources.appname})
  qsub_in:write("%s -l parxe.engines.templates.pbs_worker_script -e ''\n"%{
               resources.appname,
  })
  qsub_in:close()
  local jobid = qsub_out:read("*l")
  qsub_out:close()
  return jobid
end

---------------------------------------------------------------------------

function pbs:constructor()
end

function pbs:destructor()
  os.remove(TMPNAME)
  xe.term()
end

-- configures a future object to perform the given operation func(...), assigns
-- the future object a task_id and keeps it in pending_futures[jobid], being
-- jobid the PBS jobid as returned by execute_qsub() function
function pbs_methods:execute(func, ...)
  local args    = table.pack(...)
  local task_id = common.next_task_id()
  local tmp = config.tmp()
  local tmpname = "%s/PX_%s_%06d_%s"%{tmp,HASH,task_id,os.date("%Y%m%d%H%M%S")}
  local f = future(check_worker)
  f._stdout_  = tmpname..".OU"
  f._stderr_  = tmpname..".ER"
  f.tmpname   = tmpname
  f.jobid     = execute_qsub(config.wd(), tmpname, f)
  f.task_id   = task_id
  pending_futures[f.jobid] = f
  local task = { id=task_id, func=func, args=args, wd=config.wd() }
  f.task = task
  return f
end

function pbs_methods:wait()
  repeat
    for jobid,f in pairs(pending_futures) do
      f:wait()
      pending_futures[jobid] = nil
    end
  until not next(pending_futures)
end

function pbs_methods:get_max_tasks() return math.huge end

function pbs_methods:set_resource(key, value)
  april_assert(allowed_resources[key], "Not allowed resources name %s", key)
  resources[key] = value
  if key == "port" then init() end
end

function pbs_methods:append_shell_line(value)
  table.insert(shell_lines, value)
end

function pbs_methods:get_hash() return HASH end

----------------------------- check worker helpers ---------------------------

local function send_task(f)
  local task = f.task
  serialize(task, server)
  f.task = nil
end

local function process_reply(pending_futures, r)
  serialize(true, server)
  local f = pending_futures[r.jobid]
  pending_futures[r.id] = nil
  f._result_ = r.result or {false}
  f._err_    = r.err
  assert(f.jobid == r.jobid)
  assert(f.task_id == r.id)
end

-- reads a message request from socket s and executes the corresponding response
local function process_message(pending_futures, s, revents)
  assert(revents == xe.NN_POLLIN)
  if revents == xe.NN_POLLIN then
    local cmd = deserialize(s)
    assert(cmd.hash == HASH,
           "Warning: unknown hash identifier, check that every server has a different port\n")
    if cmd.request then
      -- task request, send a reply with the task
      send_task(pending_futures[cmd.jobid])
    elseif cmd.reply then
      -- task reply, read task result and send ack
      process_reply(pending_futures, cmd)
      if cmd.err then fprintf(io.stderr, "ERROR IN TASK %d: %s\n", cmd.id, cmd.err) end
      return true
    else
      error("Incorrect command")
    end
  end
end

------------------------ check worker function -------------------------------

-- this function is given to pbs future objects in order to check when the data
-- is available
function check_worker()
  local n = assert( xe.poll(poll_fds) )
  if n > 0 then
    for i,r in ipairs(poll_fds) do
      if r.events == r.revents then
        process_message(pending_futures, r.fd, r.revents)
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
init()
return singleton
