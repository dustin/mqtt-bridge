{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TupleSections     #-}

module Bridge (
  Env(..), Bridge, Metrics(..), destFilters, connectMQTT, copyMsg, subscribeSink, mcs,
  raceABunch_, logAt, logInfo, logDbg, logErr, lstr
  ) where

import           Control.Concurrent.STM   (TVar, atomically, readTVar, retry)
import           Control.Monad            (void, when)
import           Control.Monad.IO.Class   (MonadIO (..))
import           Control.Monad.IO.Unlift  (MonadUnliftIO)
import           Control.Monad.Logger     (LogLevel (..), LoggingT, MonadLogger, logWithoutLoc)
import           Control.Monad.Reader     (ReaderT (..), asks)
import qualified Data.ByteString.Lazy     as BL
import           Data.Map.Strict          (Map)
import qualified Data.Map.Strict          as Map
import qualified Data.Set                 as Set
import           Data.Text                (Text, pack)
import qualified Data.Text.Encoding       as TE
import           Network.MQTT.Client
import           Network.MQTT.Topic       (Filter, match, mkTopic, unTopic)
import           Network.MQTT.Types       (PublishRequest (..), RetainHandling (..))
import           Network.URI
import           System.Remote.Counter    (Counter, inc)
import           UnliftIO.Async           (async, waitAnyCancel)
import           UnliftIO.Exception       (SomeException, tryAny)

import           BridgeConf

destFilters :: [Dest] -> [Filter]
destFilters = Set.toList . Set.fromList . map (\(Dest t _ _) -> t)

data Env = Env {
  conns   :: TVar (Map Server MQTTClient),
  dests   :: Map Server [Dest],
  metrics :: Metrics
  }

type Bridge = ReaderT Env (LoggingT IO)

data Metrics = Metrics {
  srcCounters  :: Map Server Counter,
  destCounters :: Map Server Counter,
  -- | Messages that couldn't be delivered, by destination.
  errCounters  :: Map Server Counter
  }

logAt :: MonadLogger m => LogLevel -> Text -> m ()
logAt = logWithoutLoc ""

logInfo :: MonadLogger m => Text -> m ()
logInfo = logAt LevelInfo

logErr :: MonadLogger m => Text -> m ()
logErr = logAt LevelError

logDbg :: MonadLogger m => Text -> m ()
logDbg = logAt LevelDebug

lstr :: Show a => a -> Text
lstr = pack . show

connectMQTT :: OrderedType -> URI -> Map Text Int -> (MQTTClient -> PublishRequest -> IO ()) -> IO MQTTClient
connectMQTT ot uri opts f =
  connectURI mqttConfig{_cleanSession=opt "session-expiry-interval" 0 == (0::Int),
                        _protocol=protocol,
                        _msgCB=con f,
                        _connProps=[PropSessionExpiryInterval $ opt "session-expiry-interval" 0,
                                    PropTopicAliasMaximum $ opt "topic-alias-maximum" 2048,
                                    PropMaximumPacketSize $ opt "maximum-packet-size" 65536,
                                    PropReceiveMaximum $ opt "receive-maximum" 256,
                                    PropRequestResponseInformation 1,
                                    PropRequestProblemInformation 1]
                       } uri

  where
    protocol = if opt "protocol" 5 == (3::Int)
               then Protocol311
               else Protocol50

    opt t d = toEnum $ Map.findWithDefault d t opts

    con = case ot of
            Unordered -> LowLevelCallback
            Ordered   -> OrderedLowLevelCallback

mcs :: Bridge (Map Server MQTTClient)
mcs = do
  c <- asks conns
  liftIO . atomically $ do
    m <- readTVar c
    when (null m) retry
    pure m

-- MQTT message callback that will look up a destination and deliver a message to it.
copyMsg :: Server -> (Bridge () -> IO ()) -> (MQTTClient -> PublishRequest -> IO ())
copyMsg n unl _ PublishRequest{..} = unl $ do
  dm <- asks dests
  Metrics{srcCounters} <- asks metrics

  liftIO $ inc (srcCounters Map.! n)
  case (mkTopic . TE.decodeUtf8 . BL.toStrict) _pubTopic of
    Nothing -> logErr $ mconcat ["Dropping a message from ", lstr n, " with an invalid topic: ", lstr _pubTopic]
    Just topic -> do
      let dests = map (\(Dest _ s (TransFun _ f)) -> (s,f)) $ filter (\(Dest t _ _) -> match t topic) (dm Map.! n)
      mapM_ (contained topic) dests

  where
    -- A failure delivering to one destination (e.g. it refuses the
    -- message, or it's disconnected) is logged and counted, and doesn't
    -- escape into the source connection.
    contained :: Topic -> (Server, Text -> Maybe Topic) -> Bridge ()
    contained topic (d, f) = tryAny (deliver topic (d, f)) >>= either (failed topic d) pure

    failed :: Topic -> Server -> SomeException -> Bridge ()
    failed topic d e = do
      Metrics{errCounters} <- asks metrics
      liftIO $ mapM_ inc (Map.lookup d errCounters)
      logErr $ mconcat ["Failed to deliver ", unTopic topic, " to ", lstr d, ": ", lstr e]

    deliver :: Topic -> (Server, Text -> Maybe Topic) -> Bridge ()
    deliver topic (d,f) = do
      mcs' <- mcs
      let mc = mcs' Map.! d
          dtopic = f (unTopic topic)
      dt <- maybe (fail ("no destination topic for " <> show topic)) pure dtopic
      logDbg $ mconcat ["Delivering ", unTopic topic, rewritten dt,
                        " (r=", lstr _pubRetain, ", props=", lstr _pubProps, ") to ", lstr d]
      Metrics{destCounters} <- asks metrics
      liftIO $ do
        inc (destCounters Map.! d)
        pubAliased mc dt _pubBody _pubRetain _pubQoS (filter cleanProps _pubProps)

        where
          cleanProps (PropTopicAlias _) = False
          cleanProps _                  = True
          rewritten dtopic'
            | topic == dtopic' = ""
            | otherwise        = " as " <> lstr dtopic'

raceABunch_ :: (MonadUnliftIO m, MonadLogger m) => [m a] -> m ()
raceABunch_ is = mapM async is >>= void.waitAnyCancel

-- | Subscribe a sink's filters at its source.
subscribeSink :: Sink -> Bridge ()
subscribeSink (Sink n dests) = do
  m <- mcs
  logInfo $ mconcat ["subscribing ", lstr dests, " at ", lstr n]
  subrv <- liftIO $ subscribe (m Map.! n) [(t,subOptions{_subQoS=QoS2,
                                                         _noLocal=True,
                                                         _retainHandling=SendOnSubscribeNew,
                                                         _retainAsPublished=True}) | t <- destFilters dests] mempty
  logInfo $ mconcat ["Sub response from ", lstr n, ": ", lstr subrv]
