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
local lock,lock_methods = class("parxe.lock")

function lock:constructor()
  local name = os.tmpname()
  os.remove(name)
  self.name = name .. ".lock"
end

function lock_methods:check()
  local f = assert( io.open(self.name, "r") )
  if f then f:close() end
  return f ~= nil
end

function lock_methods:remove()
  os.remove(self.name)
end

local io_open = io.open
function lock_methods:make(name)
  assert( io_open(self.name, "w") ):close()
end

return lock
