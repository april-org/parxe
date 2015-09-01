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
local pipe,pipe_methods = class("parxe.pipe")

function pipe:constructor()
  self.p1 = { util.pipe() }
  self.p2 = { util.pipe() }
end

function pipe:destructor()
  self:close()
end

function pipe_methods:set_as_child()
  self.IN  = self.p2[1] self.p2[2]:close()
  self.OUT = self.p1[2] self.p1[1]:close()
  self.p1  = nil
  self.p2  = nil
  return self
end

function pipe_methods:set_as_parent()
  self.IN  = self.p1[1] self.p1[2]:close()
  self.OUT = self.p2[2] self.p2[1]:close()
  self.p1  = nil
  self.p2  = nil
  return self
end

function pipe_methods:close()
  if self.IN then self.IN:close() end
  if self.OUT then self.OUT:close() end
  if self.p1 then self.p1[1]:close() self.p1[2]:close() end
  if self.p2 then self.p2[1]:close() self.p2[2]:close() end
end

function pipe_methods:write(...) return self.OUT:write(...) end

function pipe_methods:read(...) return self.IN:read(...) end

function pipe_methods:flush() self.OUT:flush() end

return pipe
