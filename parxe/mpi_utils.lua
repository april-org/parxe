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
local SUCCESS = MPI.SUCCESS

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
  util.alarm(0.05)
  print("WAITING COMM")
  local r = MPI.Comm_accept(cnn.port_name, INFO_NULL, 0, COMM_WORLD, client)
  print("ALARM")
  if r == MPI.SUCCESS then
    print("CONNECTION", client)
    return client
  else
    MPI.Comm_free(client)
  end
  util.sleep(0.05)
end

local function check_any_result(running_clients)
  print("CHECKING")
  local flagb = buffer.new_buffer(buffer.sizeof(buffer.int))
  local status = MPI.Status()
  print("IPROBE")
  MPI.Iprobe(ANY_SOURCE, 0, COMM_WORLD, flagb, status)
  local flag = buffer.get_typed(flagb, buffer.int, 0)
  print("FLAG",flag)
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

local function get_task(port_name, id)
  MPI.Init()
  local port_name = buffer.new_buffer(port_name)
  local client    = MPI.Comm()
  local ret = MPI.Comm_connect(port_name, INFO_NULL, 0, COMM_WORLD, client)
  local i = 0
  while ret ~= SUCCESS and i < 1000 do
    util.sleep(1.0)
    ret = MPI.Comm_connect(port_name, INFO_NULL, 0, COMM_WORLD, client)
    i=i+1
  end
  send(client, tostring(id), 0)
  local str = tostring(recv_with_client(client))
  print("TASK RECEIVED")
  local task = util.deserialize(str)
  MPI.Comm_disconnect(client)
  print("DISCONNECTED")
  return task,port_name
end

local function receive_task_id(server, worker)
  local task_id = tonumber(tostring(recv_with_client(worker)))
  return task_id
end

local function send_task(cli, task)
  send(cli, util.serialize(task), 0)
end

local function start_server()
  MPI.Init()
  local port_name = buffer.new_buffer(MAX_PORT_NAME+1)
  MPI.Open_port(INFO_NULL, port_name)
  return { port_name=port_name }
end

local function stop_server(cnn)
  MPI.Close_port(cnn.port_name)
  MPI.Finalize()
end

local function task_done(port_name, result)
  MPI.Init()
  local port_name = buffer.new_buffer(port_name)
  local client = MPI.Comm()
  local ret = MPI.Comm_connect(port_name, INFO_NULL, 0, COMM_WORLD, client)
  local i = 0
  while ret ~= SUCCESS and i < 1000 do
    util.sleep(1.0)
    ret = MPI.Comm_connect(port_name, INFO_NULL, 0, COMM_WORLD, client)
    i=i+1
  end
  send(client, util.serialize(result), 0)
  print("RESULT SENDED")
  MPI.Comm_disconnect(client)
  print("DISCONNECTED")
  MPI.Finalize()
end

return {
  accept_connection = accept_connection,
  check_any_result = check_any_result,
  get_task = get_task,
  receive_task_id = receive_task_id,
  send_task = send_task,
  start_server = start_server,
  stop_server = stop_server,
  task_done = task_done,
}
