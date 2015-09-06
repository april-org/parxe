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

local check_worker
local pending_futures = {}
local allowed_resources = { mem=true, q=true, name=true, omp=true, appname=true }
local resources = { appname="april-ann" }
local shell_lines = {}

---------------------------------------------------------------------------

local function execute_qsub(task, tmpname, f)
  util.serialize(task, f.input)
  local aux = io.popen("pwd")
  local pwd = aux:read("*l")
  aux:close()
  local qsub_in,qsub_out = assert( io.popen2("qsub -N %s"%{resources.name or tmpname}) )
  qsub_in:write("#PBS -l nice=19\n")
  qsub_in:write("#PBS -l nodes=1:ppn=%d,mem=%s\n"%{resources.omp or 1, resources.mem or "1g"})
  if resources.q then qsub_in:write("#PBS -q %s\n"%{resources.q}) end
  qsub_in:write("#PBS -m a\n")
  qsub_in:write("#PBS -d %s\n"%{pwd})
  qsub_in:write("#PBS -o %s\n"%{f._stdout_})
  qsub_in:write("#PBS -e %s\n"%{f._stderr_})
  for _,v in pairs(shell_lines) do qsub_in:write("%s\n"%{v}) end
  qsub_in:write("cd $PBS_O_WORKDIR\n")
  qsub_in:write("export OMP_NUM_THREADS=%d\n"%{resources.omp or 1})
  qsub_in:write("export PARXE_TASKID=%d\n"%{task.id})
  qsub_in:write("export PARXE_INPUT=%s\n"%{f.input})
  qsub_in:write("export PARXE_OUTPUT=%s\n"%{f.output})
  qsub_in:write("echo \"# SERVER_HOSTNAME: %s\"\n"%{HOSTNAME})
  qsub_in:write("echo \"# WORKER_HOSTNAME: $(hostname)\"\n")
  qsub_in:write("echo \"# DATE:     $(date)\"\n")
  qsub_in:write("echo \"# TASKID:   %d\"\n"%{task.id})
  qsub_in:write("echo \"# TMPNAME:  %s\"\n"%{tmpname})
  qsub_in:write("echo \"# APPNAME:  %s\"\n"%{resources.appname})
  qsub_in:write("echo \"# INPUT:    $PARXE_INPUT\"\n")
  qsub_in:write("echo \"# OUTPUT:   $PARXE_OUTPUT\"\n")
  qsub_in:write("%s -l parxe.engines.templates.pbs_worker_script -e ''\n"%{
               resources.appname,
  })
  qsub_in:close()
  f.jobid = qsub_out:read("*l")
  qsub_out:close()
end

---------------------------------------------------------------------------

function pbs:constructor()
end

function pbs:destructor()
  os.remove(TMPNAME)
  util.wait()
end

function pbs_methods:execute(func, ...)
  local args    = table.pack(...)
  local task_id = common.next_task_id()
  local tmp = config.tmp()
  local tmpname = "%s/PX_%s_%05d_%s"%{tmp,HASH,task_id,os.date("%Y%m%d%H%M%S")}
  local f = future(check_worker)
  f._stdout_ = tmpname..".OU"
  f._stderr_ = tmpname..".ER"
  f.input    = tmpname..".IN"
  f.output   = tmpname..".RESULT"
  f.task_id  = task_id
  f.tmpname  = tmpname
  pending_futures[task_id] = f
  local task = { id=task_id, func=func, args=args }
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
end

function pbs_methods:append_shell_line(value)
  table.insert(shell_lines, value)
end

function pbs_methods:get_hash() return HASH end

----------------------------- check worker helpers ---------------------------

local function finished_and_not_marked(f)
  return not f.finish_time and not os.execute("qstat %s > /dev/null 2> /dev/null"%{f.jobid})
end

local function is_stdout_available(f)
  local aux = io.open(f._stdout_)
  if aux then aux:close() end
  return aux
end

local function time_from_finish(f)
  return f.finish_time and common.gettime() - f.finish_time or -math.huge
end

local function read_all(filename)
  local f = io.open(filename)
  if f then local all = f:read("*a") f:close() return all end
  return "Unable to locate stderr file"
end

local function try_deserialize_output(f)
  local f_ou,r = io.open(f.output),nil
  if f_ou then
    -- FIXME: Check sync problems in NFS environments
    r = util.deserialize(f_ou)
    f_ou:close()
    os.remove(f.output)
  end
  f.output = nil
  return r
end

local function assign_future(pending_futures, r)
  pending_futures[r.id]._result_ = r.result or {false}
  pending_futures[r.id]._err_ = r.err
  pending_futures[r.id] = nil
end

------------------------ check worker function -------------------------------

function check_worker()
  for task_id,f in pairs(pending_futures) do
    local r
    if finished_and_not_marked(f) then f.finish_time = common.gettime() end
    if not is_stdout_available(f) then
      if time_from_finish(f) > JOB_TIMEOUT then
        r = { id=task_id, err="Job output not available, check if working directory and tmp is shared with cluster nodes\n" }
      end
    else
      r = try_deserialize_output(f)
      if not r then r = { id = task_id, err = read_all(f._stderr_) }
      else assert(r.id == task_id)
      end
    end
    if r then
      assign_future(pending_futures, r)
      if r.err then fprintf(io.stderr, "ERROR IN TASK %d: %s\n", r.id, r.err) end
    end
  end
end

----------------------------------------------------------------------------

singleton = pbs()
class.extend_metamethod(pbs, "__call", function() return singleton end)
common.user_conf("pbs.lua", singleton)
return singleton
