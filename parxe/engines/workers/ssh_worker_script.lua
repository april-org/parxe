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
local px          = require "parxe"
local common      = require "parxe.common"
local xe          = require "xemsg"
local xe_utils    = require "parxe.xemsg_utils"
local deserialize = xe_utils.deserialize
local serialize   = xe_utils.serialize
--
local HASH     = assert( os.getenv("PARXE_HASH") )
local HOSTNAME = common.hostname()
local PORT     = assert( tonumber( os.getenv("PARXE_SERVER_PORT") ) )
local SERVER   = assert( os.getenv("PARXE_SERVER") )
local TASKID   = assert( tonumber( os.getenv("PARXE_TASKID") ) )
local TIMEOUT  = 1800000 -- 30 minutes in milliseconds
print("# TASKID: ", TASKID)
-- socket creation and connection
local client = assert( xe.socket(xe.NN_REQ) )
assert( xe.setsockopt(client, xe.NN_SOL_SOCKET, xe.NN_RCVTIMEO, TIMEOUT) )
assert( xe.setsockopt(client, xe.NN_SOL_SOCKET, xe.NN_SNDTIMEO, TIMEOUT) )
local endpoint = assert( xe.connect(client, "tcp://%s:%d"%{SERVER, PORT}) )
-- request a new job
serialize({ id=TASKID, hash=HASH, request=true }, client)
-- response with task data
local task = deserialize(client)
local func, args, id = task.func, task.args, task.id
assert(TASKID == id)
-- execute the task
local ok,result = xpcall(func,debug.traceback,table.unpack(args))
local err = nil
if not ok then err,result=result,{} end
-- request returning the task result
serialize({ id=id, result=result, err=err, hash=HASH, reply=true }, client)
assert( deserialize(client) ) -- ASK
xe.shutdown(client, endpoint)
xe.close(client)
xe.term()