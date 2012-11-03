{-# LANGUAGE OverloadedStrings, DeriveDataTypeable #-}

module Control.Distributed.Backend.P2P where

import           Control.Distributed.Process                as DP
import qualified Control.Distributed.Process.Node           as DPN
import qualified Control.Distributed.Process.Internal.Types as DPT
import           Network.Transport (EndPointAddress(..))
import           Network.Transport.TCP (createTransport, defaultTCPParameters)

import Control.Monad
import Control.Applicative
import Control.Monad.Trans
import Control.Concurrent.MVar

import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BSL
import qualified Data.Set as S
import Data.Typeable
import Data.Binary

bootstrap :: String -> String -> [DPT.NodeId] -> IO ()
bootstrap host port seeds = do
    transport <- either (error . show) id `fmap` createTransport host port defaultTCPParameters
    node <- DPN.newLocalNode transport DPN.initRemoteTable

    DPN.runProcess node $ do
        pid <- getSelfPid
        register "peerController" pid
        peerSet <- liftIO $ newMVar (S.singleton pid)

        forM_ seeds $ flip whereisRemoteAsync "peerController"

        forever $ do
            receiveTimeout 5000000 [ match $ onPeerMsg peerSet
                                   , match $ onMonitor peerSet
                                   , match $ onDiscover pid
                                   ]
            liftIO $ readMVar peerSet >>= print

data PeerMessage = PeerPing
                 | PeerExchange [DPT.ProcessId]
                 | PeerJoined DPT.ProcessId
                 | PeerLeft DPT.ProcessId
                 deriving (Eq, Show, Typeable)

instance Binary PeerMessage where
    put PeerPing = putWord8 0
    put (PeerExchange ps) = putWord8 1 >> put ps
    put (PeerJoined pid)  = putWord8 2 >> put pid
    put (PeerLeft pid)    = putWord8 3 >> put pid
    get = do
        mt <- getWord8
        case mt of
            0 -> return PeerPing
            1 -> PeerExchange <$> get
            2 -> PeerJoined <$> get
            3 -> PeerLeft <$> get

onPeerMsg :: MVar (S.Set DPT.ProcessId) -> PeerMessage -> Process ()

onPeerMsg _ PeerPing = return ()

onPeerMsg peers (PeerExchange ps) = do
    liftIO $ do
        current <- takeMVar peers
        putMVar peers $ S.union current (S.fromList ps)
    mapM_ monitor ps

onPeerMsg peers (PeerJoined pid) = do
    (seen, mine) <- liftIO $ do
        mine <- takeMVar peers
        seen <- return $! S.member pid mine
        if seen then putMVar peers mine
                else putMVar peers (S.insert pid mine)
        return (seen, S.toList mine)

    send pid $ PeerExchange mine
    monitor pid

    if seen
        then return ()
        else do
            myPid <- getSelfPid
            forM_ mine $ \peer -> when (peer /= myPid) $ do
                send peer $ PeerJoined pid

onPeerMsg peers (PeerLeft pid) = liftIO $ do
    current <- takeMVar peers
    putMVar peers $ S.delete pid current

onDiscover :: DPT.ProcessId -> WhereIsReply -> Process ()
onDiscover myPid (WhereIsReply "peerController" (Just seed)) =
    send seed $ PeerJoined myPid

onMonitor :: MVar (S.Set DPT.ProcessId) -> ProcessMonitorNotification -> Process ()
onMonitor peerSet (ProcessMonitorNotification mref pid reason) = do
    peers <- liftIO $ do
        peers <- takeMVar peerSet
        putMVar peerSet $! S.delete pid peers
        return (S.toList peers)
    forM_ peers $ \peer -> send peer (PeerLeft pid)

makeNodeId :: String -> DPT.NodeId
makeNodeId addr = DPT.NodeId . EndPointAddress . BS.concat $ [BS.pack addr, ":0"]
