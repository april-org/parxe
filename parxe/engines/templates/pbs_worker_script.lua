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
local px = require "parxe"
local TASKID = tonumber( os.getenv("PARXE_TASKID") )
local INPUT  = assert( os.getenv("PARXE_INPUT") )
local OUTPUT = assert( os.getenv("PARXE_OUTPUT") )
local function RUN_WORKER(TASKID, INPUT, OUTPUT)
  local f = io.open(INPUT)
  -- retry because of NFS sync problems
  while not f do f = io.open(INPUT) util.sleep(0.1) end
  local task = assert( util.deserialize(f) )
  f:close() os.remove(INPUT)
  local func, args, id = task.func, task.args, task.id
  assert(TASKID == id)
  local ok,result = xpcall(func,debug.traceback,table.unpack(args))
  local err = nil
  if not ok then err,result=result,{} end
  util.serialize({id=id, result=result, err=err}, OUTPUT)
end
RUN_WORKER(TASKID, INPUT, OUTPUT)
