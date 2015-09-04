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

local SERVER = os.tmpname()
local cnn    = mpi_utils.run_server(SERVER)

---------------------------------------------------------------------------

local pbs,pbs_methods = class("parxe.engine.pbs")
local singleton

---------------------------------------------------------------------------

local check_worker
local in_dict = {}
local pending_futures = {}

---------------------------------------------------------------------------

function pbs:constructor()
end

function pbs:destructor()
end

function pbs_methods:execute(func, ...)
  local args = table.pack(...)
  local task_id = common.next_task_id()
  local f = future(check_worker)
  f.task_id = task_id
  pending_futures[task_id] = f
  in_dict[task_id] = { id=task_id, func=func, args=args }
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

local running_clients = {}
function check_worker()
  repeat
    local cli = mpi_utils.accept_connection(cnn)
    if cli then
      local task_id = mpi_utils.receive_task_id(cnn, cli)
      local task = in_dict[task_id]
      in_dict[task_id] = nil
      mpi_utils.send_task(cnn, cli, task)
      running_clients[task_id] = cli
    end
    local r = mpi_utils.check_any_result(cnn, running_clients)
    if r then
      running_clients[r.id] = nil
      pending_futures[r.id]._result_ = r.result or true
      if r.err then fprintf(io.stderr, r.err) end
    end
  until not next(running_clients)
end

----------------------------------------------------------------------------

singleton = pbs()
class.extend_metamethod(pbs, "__call", function() return singleton end)
return singleton
