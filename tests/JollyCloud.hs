module Main where

import qualified Control.Distributed.Backend.P2P as P2P

import System.Environment (getArgs)

main :: IO ()
main = do
  args <- getArgs

  case args of
    ["-h"] -> putStrLn "Usage: jollycloud addr port [<seed>..]"
    host:port:seeds -> P2P.bootstrap host port $ map P2P.makeNodeId seeds
