local DEFAULT_BLOCK_SIZE = 4096

local function deserialize(f)
  local line = f:read("*l") if not line then return end
  local n = tonumber(line)
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
end

local function take_slice(obj, a, b)
  local object_slice
  if type(obj):find("^matrix") then
    object_slice = obj[{{a,b}}]:clone()
  else
    object_slice={} for i=a,b do object_slice[#object_slice+1] = object[i] end
  end
  return object_slice
end

return {
  deserialize = deserialize,
  gettime = gettime,
  next_task_id = next_task_id,
  serialize = serialize,
  take_slice = take_slice,
}
