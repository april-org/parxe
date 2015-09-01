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
local common = require "parxe.common"
local config = require "parxe.config"
local future = require "parxe.future"

---------------------------------------------------------------------------

local function wait(self) return self.data end
local function ready(self) return true end

---------------------------------------------------------------------------

local seq,seq_methods = class("parxe.engines.seq")

function seq:constructor()
end

function seq:destructor()
end

function seq_methods:execute(func, ...)
  local f = future(wait, ready)
  f.data = func(...)
  return f
end

function seq_methods:wait() end

function seq_methods:get_max_tasks() return 1 end

local singleton = seq()
class.extend_metamethod(seq, "__call", function() return singleton end)
return singleton
