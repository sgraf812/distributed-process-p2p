{-# LANGUAGE OverloadedStrings, RecordWildCards, DeriveDataTypeable #-}

-- | Peer-to-peer node discovery backend for Cloud Haskell based on the TCP
-- transport. Provided with a known node address it discovers and maintains
-- the knowledge of it's peers.
--
-- > import qualified Control.Distributed.Backend.P2P as P2P
-- > import           Control.Monad.Trans (liftIO)
-- > import           Control.Concurrent (threadDelay)
-- >
-- > main = P2P.bootstrap "myhostname" "9001" [P2P.makeNodeId "seedhost:9000"] $ do
-- >     liftIO $ threadDelay 1000000 -- give dispatcher a second to discover other nodes
-- >     P2P.nsendPeers "myService" ("some", "message")

module Control.Distributed.Backend.P2P (
    bootstrap,
    makeNodeId,
    getPeers,
    getCapable,
    nsendPeers,
    nsendCapable
) where

import Control.Distributed.Process                as DP
import Control.Distributed.Process.Node           as DPN
import Control.Distributed.Process.Internal.Types as DPT
import Control.Distributed.Process.Serializable (Serializable)
import Network.Transport (EndPointAddress(..))
import Network.Transport.TCP (createTransport, defaultTCPParameters)

import Control.Monad
import Control.Applicative
import Control.Monad.Trans
import Control.Concurrent.MVar
import Control.Concurrent (threadDelay)

import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BSL
import qualified Data.Set as S
import Data.Typeable
import Data.Binary
import Data.Maybe (isJust)

-- * Peer-to-peer API

peerControllerService = "P2P:Controller"

type Peers = S.Set ProcessId

data PeerState = PeerState { p2pPeers :: MVar Peers }

initPeerState :: Process PeerState
initPeerState = do
    self <- getSelfPid
    peers <- liftIO $ newMVar (S.singleton self)
    return $! PeerState peers

-- ** Initialization

-- | Make a NodeId from "host:port" string.
makeNodeId :: String -> NodeId
makeNodeId addr = NodeId . EndPointAddress . BS.concat $ [BS.pack addr, ":0"]

-- | Start a controller service process and aquire connections to a swarm.
bootstrap :: String -> String -> [NodeId] -> Process () -> IO ()
bootstrap host port seeds proc = do
    transport <- either (error . show) id `fmap` createTransport host port defaultTCPParameters
    node <- newLocalNode transport initRemoteTable

    pcPid <- forkProcess node $ do
        state <- initPeerState

        let waitRegister = do
            liftIO $ threadDelay 250000
            res <- whereis peerControllerService
            case res of
                Nothing -> waitRegister
                Just _ -> return ()
        getSelfPid >>= register peerControllerService >> waitRegister

        mapM_ doDiscover seeds
        say "P2P controller started."
        forever $ receiveWait [ matchIf isPeerDiscover $ onDiscover state
                              , match $ onMonitor state
                              , match $ onPeerRequest state
                              , match $ onPeerQuery state
                              , match $ onPeerCapable
                              ]

    runProcess node proc

-- ** Discovery

doDiscover :: NodeId -> Process ()
doDiscover node = do
    say $ "Examining node: " ++ show node
    whereisRemoteAsync node peerControllerService

doRegister :: PeerState -> ProcessId -> Process ()
doRegister state@PeerState{..} pid = do
    pids <- liftIO $ takeMVar p2pPeers
    if S.member pid pids
        then liftIO $ putMVar p2pPeers pids
        else do
            say $ "Registering peer:" ++ show pid
            monitor pid

            liftIO $ putMVar p2pPeers (S.insert pid pids)
            say $ "New node: " ++ show pid
            doDiscover $ processNodeId pid

doUnregister :: PeerState -> Maybe MonitorRef -> ProcessId -> Process ()
doUnregister PeerState{..} mref pid = do
    say $ "Unregistering peer: " ++ show pid
    maybe (return ()) unmonitor mref
    peers <- liftIO $ takeMVar p2pPeers
    liftIO $ putMVar p2pPeers (S.delete pid peers)

isPeerDiscover :: WhereIsReply -> Bool
isPeerDiscover (WhereIsReply service pid) =
    service == peerControllerService && isJust pid

onDiscover :: PeerState -> WhereIsReply -> Process ()
onDiscover state (WhereIsReply _ (Just seedPid)) = do
    say $ "Peer discovered: " ++ show seedPid

    (sp, rp) <- newChan
    self <- getSelfPid
    send seedPid (self, sp :: SendPort Peers)
    say $ "Waiting for peers..."
    peers <- receiveChan rp

    known <- liftIO $ readMVar (p2pPeers state)
    mapM_ (doRegister state) (S.toList $ S.difference peers known)

onPeerRequest :: PeerState -> (ProcessId, SendPort Peers) -> Process ()
onPeerRequest PeerState{..} (peer, replyTo) = do
    say $ "Peer exchange with " ++ show peer
    peers <- liftIO $ takeMVar p2pPeers
    if S.member peer peers
        then liftIO $ putMVar p2pPeers peers
        else do
            monitor peer
            liftIO $ putMVar p2pPeers (S.insert peer peers)

    sendChan replyTo peers

onPeerQuery :: PeerState -> SendPort Peers -> Process ()
onPeerQuery PeerState{..} replyTo = do
    say $ "Local peer query."
    liftIO (readMVar p2pPeers) >>= sendChan replyTo

onPeerCapable :: (String, SendPort ProcessId) -> Process ()
onPeerCapable (service, replyTo) = do
    say $ "Capability request: " ++ service
    res <- whereis service
    case res of
        Nothing -> say "I can't."
        Just pid -> say "I can!" >> sendChan replyTo pid

onMonitor :: PeerState -> ProcessMonitorNotification -> Process ()
onMonitor state (ProcessMonitorNotification mref pid reason) = do
    say $ "Monitor event: " ++ show (pid, reason)
    doUnregister state (Just mref) pid

-- ** Discovery

-- | Get a list of currently available peer nodes.
getPeers :: Process [NodeId]
getPeers = do
    say $ "Requesting peer list from local controller..."
    (sp, rp) <- newChan
    nsend peerControllerService (sp :: SendPort Peers)
    receiveChan rp >>= return . map processNodeId . S.toList

-- | Poll a network for a list of specific service providers.
getCapable :: String -> Process [ProcessId]
getCapable service = do
    (sp, rp) <- newChan
    nsendPeers peerControllerService (service, sp)
    say "Waiting for capable nodes..."
    go rp []

    where go rp acc = do res <- receiveChanTimeout 100000 rp
                         case res of Just pid -> say "cap hit" >> go rp (pid:acc)
                                     Nothing -> say "cap done" >> return acc

-- ** Messaging

-- | Broadcast a message to a specific service on all peers.
nsendPeers :: Serializable a => String -> a -> Process ()
nsendPeers service msg = getPeers >>= mapM_ (\peer -> nsendRemote peer service msg)

-- | Broadcast a message to a service of on nodes currently running it.
nsendCapable :: Serializable a => String -> a -> Process ()
nsendCapable service msg = getCapable service >>= mapM_ (\pid -> send pid msg)

