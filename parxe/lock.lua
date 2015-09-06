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

local io_open    = io.open
local os_remove  = os.remove
local os_tmpname = os.tmpname

function lock:constructor(_tmpname_)
  if _tmpname_ then io.open(_tmpname_, "w"):close() end
  local tmpname = _tmpname_ or os_tmpname()
  self.tmpname  = tmpname
  self.name     = tmpname .. ".lock"
end

function lock:destructor()
  os_remove(self.tmpname and self.tmpname)
  os_remove(self.name and self.name)
end

function lock_methods:destroy()
  self.tmpname = nil
  self.name = nil
end

function lock_methods:check()
  local f = io.open(self.name, "r")
  if f then f:close() end
  return f ~= nil
end

function lock_methods:remove()
  os_remove(self.name)
end

function lock_methods:make()
  assert( io_open(self.name, "w") ):close()
end

return lock
