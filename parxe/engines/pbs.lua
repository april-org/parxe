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
local common    = require "parxe.common"
local config    = require "parxe.config"
local future    = require "parxe.future"

---------------------------------------------------------------------------

local f = io.popen("hostname")
local HOSTNAME = f:read("*l") f:close()
local TMPNAME  = os.tmpname()
local HASH = TMPNAME:match("^.*lua_(.*)$")
local CLEAR_TMP = true

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
  local qsub = io.popen("qsub -N %s > /dev/null"%{resources.name or tmpname}, "w")
  -- local qsub = io.open("/tmp/jarl_"..task.id..".txt", "w")
  qsub:write("#PBS -l nice=19\n")
  qsub:write("#PBS -l nodes=1:ppn=%d,mem=%s\n"%{resources.omp or 1, resources.mem or "1g"})
  if resources.q then qsub:write("#PBS -q %s\n"%{resources.q}) end
  qsub:write("#PBS -m a\n")
  qsub:write("#PBS -d %s\n"%{pwd})
  qsub:write("#PBS -o %s\n"%{f.stdout})
  qsub:write("#PBS -e %s\n"%{f.stderr})
  for _,v in pairs(shell_lines) do qsub:write("%s\n"%{v}) end
  qsub:write("cd $PBS_O_WORKDIR\n")
  qsub:write("export OMP_NUM_THREADS=%d\n"%{resources.omp or 1})
  qsub:write("export PARXE_TASKID=%d\n"%{task.id})
  qsub:write("export PARXE_INPUT=%s\n"%{f.input})
  qsub:write("export PARXE_OUTPUT=%s\n"%{f.output})
  qsub:write("echo \"# SERVER_HOSTNAME: %s\"\n"%{HOSTNAME})
  qsub:write("echo \"# WORKER_HOSTNAME: $(hostname)\"\n")
  qsub:write("echo \"# DATE:     $(date)\"\n")
  qsub:write("echo \"# TASKID:   %d\"\n"%{task.id})
  qsub:write("echo \"# TMPNAME:  %s\"\n"%{tmpname})
  qsub:write("echo \"# APPNAME:  %s\"\n"%{resources.appname})
  qsub:write("echo \"# INPUT:    $PARXE_INPUT\"\n")
  qsub:write("echo \"# OUTPUT:   $PARXE_OUTPUT\"\n")
  qsub:write("%s -l parxe.engines.templates.pbs_worker_script -e ''\n"%{
               resources.appname,
  })
  qsub:close()
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
  f.task_id = task_id
  f.tmpname = tmpname
  f.stdout  = tmpname..".OU"
  f.stderr  = tmpname..".ER"
  f.input   = tmpname..".IN"
  f.output  = tmpname..".RESULT"
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

function pbs_methods:set_clear_tmp(v) CLEAR_TMP = v end

function check_worker()
  for task_id,f in pairs(pending_futures) do
    local aux = io.open(f.stdout)
    if aux then
      aux:close()
      local f_ou,r = io.open(f.output),nil
      if f_ou then
        -- FIXME: Check sync problems in NFS environments
        r = util.deserialize(f_ou)
        f_ou:close()
        os.remove(f.output)
      end
      f.output = nil
      if not r then
        local g_er = assert( io.open(f.stderr) )
        r = { id = task_id, err = g_er:read("*a") }
        g_er:close()
      else
        assert(r.id == task_id)
      end
      pending_futures[r.id]._result_ = r.result or {true}
      pending_futures[r.id]._err_ = r.err
      if CLEAR_TMP then
        local stdout = io.open(f.stdout)
        pending_futures[r.id]._stdout_ = stdout:read("*a")
        stdout:close()
        os.remove(f.stdout)
        os.remove(f.stderr)
      end
      pending_futures[r.id] = nil
      if r.err then fprintf(io.stderr, "%s", r.err) end
    end
  end
end

----------------------------------------------------------------------------

singleton = pbs()
class.extend_metamethod(pbs, "__call", function() return singleton end)
common.user_conf("pbs.lua", singleton)
return singleton
