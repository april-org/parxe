-- PARalel eXecution Engine (PARXE) for APRIL-ANN
local parxe = {
  _NAME     = "parse",
  _VERSION  = "0.1",
  config    = require "parse.config".init(),
  export    = require "parxe.export",
  --
  apply     = require "parxe.apply", -- ignore returned values
  filter    = require "parxe.filter",
  map       = require "parxe.map",
  reduce    = require "parxe.reduce",
  --
  wait      = require "parxe.wait",
}
return parxe
