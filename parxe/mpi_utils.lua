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
local MPI    = require "MPI"
local buffer = require "buffer"

-- PBS job Lua script
local worker_script = [[
local mpi_utils = require "parxe.mpi_utils"
local SERVER    = os.getenv("PARXE_SERVER")
local TASK_ID   = os.getenv("PARXE_TASKID")
local cnn,task  = mpi_utils.mpi_child_connect(SERVER, TASK_ID)
--
local func, args, id = task.func, task.args, task.id assert(TASK_ID == id)
-- FIXME: MEMORY LEAK POSSIBLE WHEN ERROR IS PRODUCED
local ok,result = xpcall(func,debug.traceback,table.unpack(args))
local err = nil
if not ok then err,result=result,{} end
mpi_utils.mpi_task_done(cnn, id)
mpi_utils.mpi_disconnect(cnn)
]]

local function accept_connection(cnn)
end

local function check_any_result(cnn, running_clients)
end

local function run_server(server_name)
end

local function send_task(cnn, cli, task)
end

return {
  accept_connection = accept_connection,
  check_any_result = check_any_result,
  run_server = run_server,
  send_task = send_task,
}
