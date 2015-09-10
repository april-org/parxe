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
local xe = require "xemsg"
local xe_utils    = require "parxe.xemsg_utils"
local deserialize = xe_utils.deserialize
local serialize   = xe_utils.serialize
--
local TASKID = assert( tonumber( os.getenv("PARXE_TASKID") ) )
local SERVER = assert( os.getenv("PARXE_SERVER") )
local SERVER_PORT = assert( tonumber( os.getenv("PARXE_SERVER_PORT") ) )
local CLIENT_PORT = assert( tonumber( os.getenv("PARXE_CLIENT_PORT") ) )
local HASH = assert( os.getenv("PARXE_HASH") )
local HOSTNAME
do
  local f = io.popen("hostname")
  HOSTNAME = f:read("*l") f:close()
end
local function RUN_WORKER(TASKID, SERVER, HOSTNAME,
                          SERVER_PORT, CLIENT_PORT, HASH)
  local client_string,binded
  local s_client = assert( xe.socket(xe.AF_SP, xe.NN_PULL) )
  local binded = assert( xe.bind(s_client, "tcp://*:"..CLIENT_PORT) )
  local client_string = "tcp://%s:%d"%{HOSTNAME, CLIENT_PORT}
  --
  local server_string = "tcp://%s:%d"%{SERVER, SERVER_PORT}
  local s_server = assert( xe.socket(xe.AF_SP, xe.NN_PUSH) )
  assert( xe.connect(s_server, server_string) )
  serialize({ id=TASKID, client=client_string, hash=HASH, request=true }, s_server)
  local task
  repeat task = deserialize( s_client ) until task.id
  assert( xe.shutdown(s_client, binded) )
  local func, args, id = task.func, task.args, task.id
  -- print(TASKID, id)
  -- assert(TASKID == id)
  local ok,result = xpcall(func,debug.traceback,table.unpack(args))
  local err = nil
  if not ok then err,result=result,{} end
  serialize({ id=id, result=result, err=err,
              hash=HASH, client=client_string, reply=true }, s_server)
end
RUN_WORKER(TASKID, SERVER, HOSTNAME, SERVER_PORT, CLIENT_PORT, HASH)
