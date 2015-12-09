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
local xe       = require "xemsg"

local HOSTNAME = common.hostname()
-- Table with all allowed resources for PBS configuration. They can be setup
-- by means of set_resource method in pbs engine object.
local allowed_resources = { mem=true, q=true, name=true, omp=true, port=true,
                            appname=true, host=true, properties = true }
-- Value of resources for PBS configuration.
local resources = { appname="april-ann", host=HOSTNAME, port=1234, omp=1,
                    mem="1g" }
-- Lines of shell script to be executed by PBS script before running worker
local shell_lines = {}

local pbs,pbs_methods = class("parxe.engine.pbs")

function pbs:constructor()
  ---------------------------------------------------------------------------
  -- TMPNAME allows to identify this server, allowing to execute several servers
  -- in the same host. The hash part is the particular random sequence of
  -- characters generated by Lua to distinguish the tmpname file. This hash part
  -- is used to  identify client connections in order to assert possible errors.
  self.TMPNAME  = os.tmpname()
  self.HASH     = self.TMPNAME:match("^.*lua_(.*)$")
  local f = io.open(self.TMPNAME,"w")
  f:write("PARXE pbs engine\n")
  f:close()
  ---------------------------------------------------------------------------
  
  -----------------------------------------------------------------------

  -- Forward declaration of server socket and binded endpoint identifier, for
  -- attention of the reader.
  self.server = nil
  self.endpoint = nil
  self.server_url = nil
  self.client_url = nil

  common.user_conf("pbs.lua", self)
end

function pbs:destructor()
  if self.server then
    self.server:shutdown(self.endpoint)
    self.server:close()
  end
  if self.TMPNAME then os.remove(self.TMPNAME) end
end

function pbs_methods:destroy()
  pbs.destructor(self)
  for k,v in pairs(self) do self[k] = nil end
end

function pbs_methods:init()
  if not self.server then
    self.server_url = "tcp://*:%d"%{resources.port}
    self.client_url = "tcp://%s:%d"%{resources.host,resources.port}
    self.server = assert( xe.socket(xe.NN_REP) )
    self.endpoint = assert( self.server:bind(self.server_url) )
  end
  return self.server
end

function pbs_methods:abort(task)
  error("Not implemented")
end

function pbs_methods:check_asserts(cmd)
  assert(cmd.hash == self.HASH,
         "Warning: unknown hash identifier, check that every server has a different port\n")
end

function pbs_methods:acceptting_tasks()
  return true
end

local function concat_properties(props)
  if not props or #props==0 then return "" end
  return ":" .. table.concat(props, ":")
end

-- enqueues the given task profile into PBS scheduler, and keeps the jobid into
-- task table
function pbs_methods:execute(task, stdout, stderr)
  local qsub_in,qsub_out = assert( io.popen2("qsub -N %s"%{resources.name or "PARXE"}) )
  qsub_in:write("#PBS -l nice=19\n")
  qsub_in:write("#PBS -l nodes=1:ppn=%d%s,mem=%s\n"%{resources.omp,
                                                     concat_properties(resources.properties),
                                                     resources.mem})
  if resources.q then qsub_in:write("#PBS -q %s\n"%{resources.q}) end
  qsub_in:write("#PBS -m a\n")
  qsub_in:write("#PBS -o %s\n"%{stdout})
  qsub_in:write("#PBS -e %s\n"%{stderr})
  qsub_in:write("cd %s\n"%{task.wd})
  for _,v in pairs(shell_lines) do qsub_in:write("%s\n"%{v}) end
  qsub_in:write("export OMP_NUM_THREADS=%d\n"%{resources.omp})
  qsub_in:write("echo \"# SERVER_HOSTNAME: %s\"\n"%{HOSTNAME})
  qsub_in:write("echo \"# WORKER_HOSTNAME: $(hostname)\"\n")
  qsub_in:write("echo \"# DATE:     $(date)\"\n")
  qsub_in:write("echo \"# SERVER:   %s\"\n"%{resources.host})
  qsub_in:write("echo \"# PORT:     %d\"\n"%{resources.port})
  qsub_in:write("echo \"# HASH:     %s\"\n"%{self.HASH})
  qsub_in:write("echo \"# APPNAME:  %s\"\n"%{resources.appname})
  qsub_in:write("%s -l parxe.worker -e \"RUN_WORKER('%s','%s',%d)\"\n"%{
                resources.appname, self.client_url, self.HASH, task.id,
  })
  qsub_in:close()
  local jobid = qsub_out:read("*l")
  qsub_out:close()
  assert(not task.jobid)
  task.jobid = jobid
end

function pbs_methods:finished(task)
end

-- no limit due to PBS
function pbs_methods:get_max_tasks() return math.huge end

-- configure PBS resources for qsub script configuration
function pbs_methods:set_resource(key, value)
  april_assert(allowed_resources[key], "Not allowed resources name %s", key)
  resources[key] = value
  if key == "port" and self.server then
    fprintf(io.stderr, "Unable to change port after any task has been executed")
  end
end

-- appends a new shell line which will be executed by qsub script
function pbs_methods:append_shell_line(value)
  table.insert(shell_lines, value)
end

----------------------------------------------------------------------------

return pbs
