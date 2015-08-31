local pipe,pipe_methods = class("pipe")

function pipe:constructor()
  self.p1 = { util.pipe() }
  self.p2 = { util.pipe() }
end

function pipe:destructor()
  self:close()
end

function pipe_methods:set_as_child()
  self.IN  = self.p1[1] self.p1[2]:close()
  self.OUT = self.p2[2] self.p2[1]:close()
  self.p1  = nil
  self.p2  = nil
  return self
end

function pipe_methods:set_as_parent()
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

return pipe
