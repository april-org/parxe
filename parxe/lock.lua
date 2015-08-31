local lock,lock_methods = class("lock")

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
