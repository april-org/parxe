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

local px_map = require "parxe.map"

-- reimplementation of stats.boot of APRIL-ANN for PARXE
local function boot(params,...)
  local params = get_table_fields(
    {
      size        = { mandatory = true, },
      resample    = { mandatory = false, type_match = "number", default = 1 },
      R           = { type_match = "number",   mandatory = true },
      statistic   = { type_match = "function", mandatory = true },
      k           = { type_match = "number", mandatory = false, default = 1 },
      verbose     = { mandatory = false },
      seed        = { mandatory = false, type_match = "number" },
      random      = { mandatory = false, isa_match  = random },
    },
    params)
  assert(not params.seed or not params.random,
         "Fields 'seed' and 'random' are forbidden together")
  local extra       = table.pack(...)
  local size        = params.size
  local resample    = params.resample
  local repetitions = params.R
  local statistic   = params.statistic
  local seed        = params.seed
  local rnd         = params.random or random(seed)
  local tsize       = type(size)
  local k           = params.k
  assert(resample > 0.0 and resample <= 1.0, "Incorrect resample value")
  assert(tsize == "number" or tsize == "table",
         "Needs a 'size' field with a number or a table of numbers")
  if tsize == "number" then size = { size } end
  local resamples = {}
  for i=1,#size do
    resamples[i] = math.round(size[i]*resample)
    assert(resamples[i] > 0, "Resample underflow, increase resample value")
  end
  local get_row,N
  -- resample function executed in parallel using parallel_foreach
  local last_i = 0
  local rnd_matrix = function(sz,rsmpls) return matrixInt32(rsmpls):uniform(1,sz,rnd) end
  local resample = function(i)
    collectgarbage("collect")
    -- this loop allows to synchronize the random number generator, allowing to
    -- produce the same exact result independently of number of cores
    for j=last_i+1,i-1 do
      for r=1,#size do
        for k=1,resamples[r] do rnd:randInt(size[r]-1) end
      end
    end
    last_i = i
    --
    local sample = iterator.zip(iterator(size),
                                iterator(resamples)):map(rnd_matrix):table()
    local r = table.pack( statistic(multiple_unpack(sample, extra)) )
    april_assert(#r == k,
                 "Unexpected number of returned values in statistic, expected %d, found %d",
                 k, #r)
    return matrix(r)
  end
  local ok,result = xpcall(px_map, debug.traceback, resample, repetitions)
  if not ok then error(result) end
  if params.verbose then
    fprintf(io.stderr, ".") io.stderr:flush() 
    while not result:wait(10) do fprintf(io.stderr, ".") io.stderr:flush() end
  end
  local result = matrix.join(0, result:get())
  if params.verbose then fprintf(io.stderr, " done\n") end
  return result
end

return boot
