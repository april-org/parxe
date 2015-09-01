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
local DEFAULT_BLOCK_SIZE = 4096

local function deserialize(f)
  print("deserialize")
  local line = f:read("*l") if not line then return end
  local n = tonumber(line)
  print("line", line)
  return util.deserialize{
    read=function()
      if n > 0 then
        local b = math.min(n, DEFAULT_BLOCK_SIZE)
        n = n - b
        return assert( f:read(b) )
      end
    end
  }
end

local function gettime()
  local s,us = util.gettimeofday()
  return s + us/1.0e6
end

local task_id,next_task_id
do
  task_id = 0 function next_task_id() task_id = task_id + 1 return task_id end
end

local function serialize(obj, f)
  local str = util.serialize(obj)
  assert( f:write(#str) )
  assert( f:write("\n") )
  assert( f:write(str) )
  f:flush()
end

local function take_slice(obj, a, b)
  if a == 1 and b == #obj then return obj end
  local object_slice
  if type(obj):find("^matrix") then
    object_slice = obj[{{a,b}}]:clone()
  else
    object_slice={} for i=a,b do object_slice[#object_slice+1] = obj[i] end
  end
  return object_slice
end

local function matrix_join(n, tbl)
  if #tbl == 1 then return tbl[1] end
  return matrix.join(n, tbl)
end

return {
  deserialize = deserialize,
  gettime = gettime,
  next_task_id = next_task_id,
  serialize = serialize,
  take_slice = take_slice,
  matrix_join = matrix_join,
}
