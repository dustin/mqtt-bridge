{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

-- | Counters collected without making message delivery wait for their
-- aggregation, following mqttd's stats mechanism.
module Stats (
  StatKey(..), StatStore, statKeyName, newStatStore, applyStats, flushStats,
  incrementStat, incrementStatSTM, retrieveStats
  ) where

import           Control.Concurrent.STM (STM, TBQueue, TVar, check, flushTBQueue, isEmptyTBQueue,
                                         modifyTVar')
import           Control.Monad          (forever, unless)
import           Control.Monad.IO.Class (MonadIO)
import           Data.Map.Strict        (Map)
import qualified Data.Map.Strict        as Map
import           Data.Text              (Text)
import           Network.MQTT.Topic     (Topic, unTopic)
import           UnliftIO               (atomically, newTBQueueIO, newTVarIO, readTVarIO, writeTBQueue)

import           BridgeConf             (Server)

data StatKey = StatMsgSent Server
             | StatMsgRcvd Server
             | StatMsgFailed Server
  deriving (Eq, Show, Ord)

statKeyName :: Topic -> StatKey -> Text
statKeyName prefix (StatMsgRcvd n)   = unTopic prefix <> "/" <> n <> "/messages/received"
statKeyName prefix (StatMsgSent n)   = unTopic prefix <> "/" <> n <> "/messages/sent"
statKeyName prefix (StatMsgFailed n) = unTopic prefix <> "/" <> n <> "/messages/failed"

type Increment = (StatKey, Int)

data StatStore = StatStore {
  statsQueue :: TBQueue Increment,
  statsMap   :: TVar (Map StatKey Int)
  }

newStatStore :: MonadIO m => m StatStore
newStatStore = StatStore <$> newTBQueueIO 100 <*> newTVarIO mempty

-- | Apply queued increments until cancelled.
applyStats :: MonadIO m => StatStore -> m ()
applyStats StatStore{..} = forever . atomically $ do
  check . not =<< isEmptyTBQueue statsQueue
  todo <- flushTBQueue statsQueue
  modifyTVar' statsMap (Map.unionWith (+) (Map.fromListWith (+) todo))

-- | Apply queued increments synchronously.  Publishers use this so they
-- never report a snapshot older than the increments already made.
flushStats :: MonadIO m => StatStore -> m ()
flushStats StatStore{..} = atomically $ do
  empty <- isEmptyTBQueue statsQueue
  unless empty $ do
    todo <- flushTBQueue statsQueue
    modifyTVar' statsMap (Map.unionWith (+) (Map.fromListWith (+) todo))

incrementStatSTM :: StatKey -> Int -> StatStore -> STM ()
incrementStatSTM k i StatStore{..} = writeTBQueue statsQueue (k, i)

incrementStat :: MonadIO m => StatKey -> Int -> StatStore -> m ()
incrementStat k i = atomically . incrementStatSTM k i

retrieveStats :: MonadIO m => StatStore -> m (Map StatKey Int)
retrieveStats = readTVarIO . statsMap
