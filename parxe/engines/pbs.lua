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
local mpi_utils = require "parxe.mpi_utils"

---------------------------------------------------------------------------

local f = io.popen("hostname")
local HOSTNAME = f:read("*l") f:close()
local TMPNAME  = os.tmpname()
local cnn      = mpi_utils.run_server()
local PORT     = tostring(cnn.port_name)
for i=1,#PORT do
  if PORT:sub(i,i):byte() == 0 then PORT = PORT:sub(1,i-1) break end
end

---------------------------------------------------------------------------

local pbs,pbs_methods = class("parxe.engine.pbs")
local singleton

---------------------------------------------------------------------------

local check_worker
local in_dict = {}
local pending_futures = {}
local allowed_resources = { mem=true, q=true, name=true, omp=true, mpiexec=true,
                            appname=true }
local resources = { mpiexec="mpiexec",
                    appname="april-ann" }
local shell_lines = {}

---------------------------------------------------------------------------

local function execute_qsub(id, tmp, tmpname)
  local qsub = io.popen("qsub -N %s > /dev/null"%{resources.name or tmpname}, "w")
  qsub:write("#PBS -l nice=19\n")
  qsub:write("#PBS -l nodes=1:ppn=%d,mem=%s\n"%{resources.omp or 1, resources.mem or "1g"})
  if resources.q then qsub:write("#PBS -q %s\n"%{resources.q}) end
  qsub:write("#PBS -m a\n")
  qsub:write("#PBS -d %s\n"%{tmp})
  qsub:write("#PBS -o %s.OU\n"%{tmpname})
  qsub:write("#PBS -e %s.ER\n"%{tmpname})
  for _,v in pairs(shell_lines) do qsub:write("%s\n"%{v}) end
  qsub:write("cd $PBS_O_WORKDIR\n")
  qsub:write("export OMP_NUM_THREADS=%d\n"%{resources.omp or 1})
  qsub:write("export PARXE_TASKID=%d\n"%{id})
  qsub:write("export PARXE_PORT='%s'\n"%{PORT})
  qsub:write("echo \"# SERVER_HOSTNAME: %s\"\n"%{HOSTNAME})
  qsub:write("echo \"# WORKER_HOSTNAME: $(hostname)\"\n")
  qsub:write("echo \"# DATE:     $(date)\"\n")
  qsub:write("echo \"# TMPNAME:  %s\"\n"%{tmpname})
  qsub:write("echo \"# TASK_ID:  %d\"\n"%{id})
  qsub:write("echo \"# PORT:     '%s'\"\n"%{PORT})
  qsub:write("echo \"# MPIEXEC:  %s\"\n"%{resources.mpiexec})
  qsub:write("echo \"# APPNAME:  %s\"\n"%{resources.appname})
  qsub:write("%s %s -l parxe.engines.templates.pbs_worker_script -e 'print(\"# DONE\")'"%{resources.mpiexec,
                                                                                          resources.appname})
  qsub:close()
end

---------------------------------------------------------------------------

function pbs:constructor()
end

function pbs:destructor()
  mpi_utils.stop_server(cnn)
  os.remove(TMPNAME)
end

function pbs_methods:execute(func, ...)
  local args    = table.pack(...)
  local task_id = common.next_task_id()
  local tmp = config.tmp()
  local tmpname = "%s/PARXE_TASK_%d_%s"%{tmp,task_id,os.date("%Y%m%d%H%M%S")}
  local f = future(check_worker)
  f.task_id = task_id
  f.tmpname = tmpname
  pending_futures[task_id] = f
  in_dict[task_id] = { id=task_id, func=func, args=args }
  execute_qsub(task_id, tmp, tmpname)
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

local running_clients = {}
function check_worker()
  repeat
    local cli = mpi_utils.accept_connection(cnn)
    if cli then
      local task_id = mpi_utils.receive_task_id(cnn, cli)
      local task = in_dict[task_id]
      in_dict[task_id] = nil
      mpi_utils.send_task(cli, task)
      running_clients[task_id] = cli
    end
    local r = mpi_utils.check_any_result(running_clients)
    if r then
      pending_futures[r.id]._result_ = r.result or true
      if r.err then fprintf(io.stderr, r.err) end
    end
  until not cli and not r
end

----------------------------------------------------------------------------

singleton = pbs()
class.extend_metamethod(pbs, "__call", function() return singleton end)
common.user_conf("pbs.lua", singleton)
return singleton
