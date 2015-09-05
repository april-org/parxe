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

local ANY_SOURCE = MPI.ANY_SOURCE
local COMM_WORLD = MPI.COMM_WORLD
local INFO_NULL  = MPI.INFO_NULL
local MAX_PORT_NAME = MPI.MAX_PORT_NAME

signal.register(signal.SIGALRM, function() end)
signal.register(signal.SIGINT, function() error("Received SIGINT") end)

-- private functions

local function recv_with_status(status, cli)
  local num_recvb = buffer.new_buffer(buffer.sizeof(buffer.int))
  MPI.Get_count(status, MPI.BYTE, num_recvb)
  local num_recv = buffer.get_typed(num_recvb, buffer.int, 0)
  print("receiving", num_recv)
  local message = buffer.new_buffer(num_recv)
  MPI.Recv(message, #message, MPI.BYTE, ANY_SOURCE, 0, cli or COMM_WORLD, status)
  print("RECEIVED", #message)
  return message,status
end

local function recv_with_client(cli)
  local status = MPI.Status()
  MPI.Probe(ANY_SOURCE, 0, cli, status)
  return recv_with_status(status, cli)
end

local function send(cli, str, rank)
  local message = buffer.new_buffer(str)
  print("sending", #str)
  MPI.Send(message, #message, MPI.BYTE, rank, 0, cli)
  print("SENDED")
end

-------------------------------------------------------------------------------

local function accept_connection(cnn)
  local client = MPI.Comm()
  util.alarm(0.01)
  local r = MPI.Comm_accept(cnn.port_name, INFO_NULL, 0, COMM_WORLD, client)
  if r == MPI.SUCCESS then
    print("CONNECTION", client)
    return client
  else
    MPI.Comm_free(client)
  end
end

local function check_any_result(running_clients, pending_futures)
  local flagb = buffer.new_buffer(buffer.sizeof(buffer.int))
  local status = MPI.Status()
  MPI.Iprobe(ANY_SOURCE, 0, COMM_WORLD, flagb, status)
  local flag = buffer.get_typed(flagb, buffer.int, 0)
  if flag == 1 then
    local b = recv_with_status(status)
    print("RECEIVED RESULT", #b)
    local result = util.deserialize(tostring(b))
    local client = running_clients[result.id]
    running_clients[result.id] = nil
    MPI.Comm_disconnect(client)
    MPI.Comm_free(client)
    return result
  end
end

local function child_connect(port_name, id)
  MPI.Init()
  print("CONNECTION", port_name, id)
  local port_name = buffer.new_buffer(port_name)
  local client    = MPI.Comm()
  MPI.Comm_connect(port_name, INFO_NULL, 0, COMM_WORLD, client)
  print("CONNECTED")
  send(client, tostring(id), 0)
  print("ID SENDED")
  local str = tostring(recv_with_client(client))
  print("TASK RECEIVED")
  local task = util.deserialize(str)
  return client,task
end

local function receive_task_id(server, worker)
  local task_id = tonumber(tostring(recv_with_client(worker)))
  return task_id
end

local function run_server()
  MPI.Init()
  local port_name = buffer.new_buffer(MAX_PORT_NAME+1)
  MPI.Open_port(INFO_NULL, port_name)
  return { port_name=port_name }
end

local function send_task(cli, task)
  send(cli, util.serialize(task), 0)
end

local function stop_server(cnn)
  MPI.Close_port(cnn.port_name)
  MPI.Finalize()
end

local function task_done(cnn, result)
  send(cnn, util.serialize(result), 0)
  print("DISCONNECTING")
  MPI.Comm_disconnect(cnn)
  print("FINALIZING")
  MPI.Finalize()
  print("GOOD BYE!")
end

return {
  accept_connection = accept_connection,
  check_any_result = check_any_result,
  child_connect = child_connect,
  disconnect = disconnect,
  receive_task_id = receive_task_id,
  run_server = run_server,
  send_task = send_task,
  task_done = task_done,
}
