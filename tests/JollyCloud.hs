module Main where

import qualified Control.Distributed.Backend.P2P as P2P
import           Control.Distributed.Process as DP
import           Control.Distributed.Process.Node as DPN

import System.Environment (getArgs)
import System.IO

import Control.Monad
import Control.Monad.Trans (liftIO)

main :: IO ()
main = do
  args <- getArgs

  case args of
    ["-h"] -> putStrLn "Usage: jollycloud addr port [<seed>..]"
    host:port:seeds -> P2P.bootstrap host port (map P2P.makeNodeId seeds) mainProcess

mainProcess :: LocalNode -> ProcessId -> Process ()
mainProcess myNode peerContId = do
    unregister "logger"
    getSelfPid >>= register "logger"

    forever $ do
        (time, pid, msg) <- expect :: Process (String, ProcessId, String)
        liftIO $ putStrLn $ time ++ " " ++ show pid ++ " " ++ msg
        return ()
