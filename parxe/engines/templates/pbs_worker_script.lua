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
local mpi_utils = require "parxe.mpi_utils"
local SERVER    = os.getenv("PARXE_SERVER")
local TASK_ID   = os.getenv("PARXE_TASKID")
local PORT      = os.getenv("PARXE_PORT")
--
io = nil
os = nil
local cnn,task = mpi_utils.child_connect(SERVER, PORT, TASK_ID)
local func, args, id = task.func, task.args, task.id
print(TASK_ID, id)
assert(TASK_ID == id)
local ok,result = xpcall(func,debug.traceback,table.unpack(args))
local err = nil
if not ok then err,result=result,{} end
mpi_utils.task_done(cnn, {id=id, result=result, err=err})
