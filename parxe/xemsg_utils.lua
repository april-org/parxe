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

-- This module reimplements functions serialize and deserialize to work with
-- nanomsg SP sockets, replacing implementation done at common module.

local config = require "parxe.config"
local xe     = require "xemsg"

local blacklist = table.invert{
  "debug",
  "get_lua_properties_table",
  "io",
  "libpng",
  "load",
  "loadfile",
  "loadstring",
  "module",
  "os",
  "package",
  "parallel_foreach",
  "require",
  "require",
  "set_lua_properties_table",
  "signal",
  "util",
}

local function deserialize(s, safe)
  local str = assert( s:recv() )
  local env = _G
  if safe then
    env = {}
    for k,v in pairs(_G) do
      if not blacklist[k] then env[k] = v end
    end
  end
  local loader = assert( load(str, nil, nil, env) )
  return loader()
end

local function serialize(data, s)
  local str = util.serialize( data )
  assert( (assert( s:send(str) )) == #str )
end

return {
  deserialize = deserialize,
  serialize   = serialize,
}
