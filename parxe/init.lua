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
require "aprilann"
local parxe = {
  _NAME     = "parxe",
  _VERSION  = "0.1",
  -- utilities
  config    = require "parxe.config".init(),
  future    = require "parxe.future",
  scheduler = require "parxe.scheduler",
  -- parallel functions
  boot      = require "parxe.boot",
  map       = require "parxe.map",
  reduce    = require "parxe.reduce",
  run       = require "parxe.run",
}

return parxe
