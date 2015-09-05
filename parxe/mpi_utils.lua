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

local INFO_NULL  = MPI.INFO_NULL
local COMM_WORLD = MPI.COMM_WORLD
local MAX_PORT_NAME = MPI.MAX_PORT_NAME

signal.register(signal.SIGALRM, function() end)
signal.register(signal.SIGINT, function() error("Received SIGINT") end)

local function accept_connection(cnn)
  local client = MPI.Comm()
  util.alarm(0.01)
  local r = MPI.Comm_accept(cnn.port_name, INFO_NULL, 0, COMM_WORLD, client)
  if r == MPI.SUCCESS then
    return client
  else
    MPI.Comm_free(client)
  end
end

local function check_any_result(cnn, running_clients)
  MPI.Comm_free(cnn.client)
end

local function child_connect(server_name, id)
end

local function disconnect(cnn)
end

local function run_server(server_name)
  -- local sizeb = buffer.new_buffer(buffer.sizeof(buffer.int))
  -- local rankb = buffer.new_buffer(buffer.sizeof(buffer.int))
  MPI.Init()
  -- MPI.Comm_rank(MPI.COMM_WORLD, rankb)
  -- MPI.Comm_size(MPI.COMM_WORLD, sizeb)
  -- local size = buffer.get_typed(sizeb, buffer.int, 0)
  -- local rank = buffer.get_typed(rankb, buffer.int, 0)
  local pub_name  = buffer.new_buffer(server_name)
  local port_name = buffer.new_buffer(MAX_PORT_NAME+1)
  MPI.Open_port(INFO_NULL, port_name)
  MPI.Publish_name(pub_name, INFO_NULL, port_name)
  print("Port:", pub_name, port_name)
  return { port_name=port_name, pub_name=pub_name }
end

local function send_task(cnn, cli, task)
end

local function stop_server(cnn)
  MPI.Unpublish_name(cnn.pub_name, INFO_NULL, cnn.port_name)
  MPI.Close_port(cnn.port_name)
  MPI.Finalize()
end

local function task_done(cnn, id)
end

return {
  accept_connection = accept_connection,
  check_any_result = check_any_result,
  child_connect = child_connect,
  disconnect = disconnect,
  run_server = run_server,
  send_task = send_task,
  task_done = task_done,
}
