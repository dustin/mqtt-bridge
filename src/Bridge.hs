{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TupleSections     #-}

module Bridge (
  Env(..), Bridge, Metrics(..), destFilters, connectMQTT, copyMsg, subscribeSink, mcs, fitTo,
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
import           Data.Text                (Text, pack, unpack)
import qualified Data.Text.Encoding       as TE
import           Network.MQTT.Client
import           Network.MQTT.Topic       (Filter, match, mkTopic, unTopic)
import           Network.MQTT.Types       (MQTTPkt (..), PublishRequest (..), RetainHandling (..), toByteString)
import           Network.URI
import           System.Remote.Counter    (Counter, inc)
import           UnliftIO.Async           (async, waitAnyCancel)
import           UnliftIO.Exception       (SomeException, throwIO, tryAny)

import           BridgeConf

-- | The filters to subscribe to for some destinations.  One that another
-- covers is left out, since the broker may deliver a copy of a message per
-- matching subscription, and each would be forwarded.  (Filters that only
-- partly overlap, like x/+ and +/y, can still both match.)
destFilters :: [Dest] -> [Filter]
destFilters ds = [f | f <- fs, not (any (\g -> g /= f && filterCovers g f) fs)]
  where fs = Set.toList . Set.fromList $ map (\(Dest t _ _) -> t) ds

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
  case ((mkTopic . TE.decodeUtf8 . BL.toStrict) _pubTopic, Map.lookup n dm) of
    (Nothing, _) -> logErr $ mconcat ["Dropping a message from ", lstr n, " with an invalid topic: ", lstr _pubTopic]
    -- No from block for this conn, so it's from a subscription that's no
    -- longer configured (a resumed session's).
    (Just topic, Nothing) -> logDbg $ mconcat ["Dropping ", unTopic topic, " from ", lstr n, ", which has nothing to sync"]
    (Just topic, Just ds) -> do
      let dests = map (\(Dest _ s (TransFun _ f)) -> (s,f)) $ filter (\(Dest t _ _) -> match t topic) ds
      mapM_ (contained topic) dests

  where
    -- A destination refusing a message (or its being unsendable, e.g. too
    -- big) is logged and counted, and doesn't escape into the source
    -- connection: it would fail the same way every time, so retrying would
    -- only crash-loop the bridge.  But if the destination's gone, a QoS 1
    -- or 2 message is failed rather than acked, so the source redelivers
    -- it (ending the source connection, and so the bridge, until it's
    -- restarted).  A QoS 0 one can't be redelivered, so it's dropped.
    contained :: Topic -> (Server, Text -> Maybe Topic) -> Bridge ()
    contained topic (d, f) = tryAny (deliver topic (d, f)) >>= either (failed topic d) pure

    failed :: Topic -> Server -> SomeException -> Bridge ()
    failed topic d e = do
      up <- maybe (pure False) (liftIO . isConnected) . Map.lookup d =<< mcs
      if not up && _pubQoS > QoS0
        then do
          logErr $ mconcat ["Lost ", lstr d, " delivering ", unTopic topic, ", so not acking it: ", lstr e]
          throwIO e
        else do
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
      sp <- liftIO $ svrProps mc
      PublishRequest{_pubBody=ob, _pubRetain=orr, _pubQoS=oq, _pubProps=op} <- either (fail . unpack) pure $
        fitTo sp PublishRequest{_pubTopic=BL.fromStrict (TE.encodeUtf8 (unTopic dt)),
                                _pubProps=filter cleanProps _pubProps, ..}
      Metrics{destCounters} <- asks metrics
      liftIO $ do
        inc (destCounters Map.! d)
        pubAliased mc dt ob orr oq op

        where
          cleanProps (PropTopicAlias _) = False
          cleanProps _                  = True
          rewritten dtopic'
            | topic == dtopic' = ""
            | otherwise        = " as " <> lstr dtopic'

-- | Fit a PUBLISH to a destination's CONNACK properties: its QoS is at
-- most the Maximum QoS [MQTT-3.2.2-11], it's not retained if retain isn't
-- available [MQTT-3.2.2-14], and it's not sent at all if it's bigger than
-- the Maximum Packet Size [MQTT-3.2.2-15].
fitTo :: [Property] -> PublishRequest -> Either Text PublishRequest
fitTo props p
  | tooBig = Left ("message too big for the destination (" <> pack (show size) <> " bytes)")
  | otherwise = Right fitted
  where
    fitted = p{_pubQoS=min (_pubQoS p) maxQoS, _pubRetain=_pubRetain p && retainOK}
    maxQoS = last (QoS2 : [if q == 0 then QoS0 else QoS1 | PropMaximumQoS q <- props, q < 2])
    retainOK = 0 `notElem` [r | PropRetainAvailable r <- props]
    -- Measured with the full topic, though an alias may make it smaller.
    size = BL.length (toByteString Protocol50 (PublishPkt fitted))
    tooBig = any (\m -> size > fromIntegral m) [m | PropMaximumPacketSize m <- props]

raceABunch_ :: (MonadUnliftIO m, MonadLogger m) => [m a] -> m ()
raceABunch_ is = mapM async is >>= void.waitAnyCancel

-- | Subscribe a sink's filters at its source.
subscribeSink :: Sink -> Bridge ()
subscribeSink (Sink n dests) = do
  m <- mcs
  logInfo $ mconcat ["subscribing ", lstr dests, " at ", lstr n]
  let filters = destFilters dests
  -- QoS 1, not 2: an inbound QoS 1 message is acked once it's been
  -- forwarded, so a restart redelivers it, but net-mqtt sends PUBREC for a
  -- QoS 2 one before handing it over, so one caught in between is lost.
  subrv@(results, _) <- liftIO $ subscribe (m Map.! n) [(t,subOptions{_subQoS=QoS1,
                                                                      _noLocal=True,
                                                                      _retainHandling=SendOnSubscribeNew,
                                                                      _retainAsPublished=True}) | t <- filters] mempty
  logInfo $ mconcat ["Sub response from ", lstr n, ": ", lstr subrv]
  -- A refused filter would leave the bridge running without it.
  case [(f, e) | (f, Left e) <- zip filters results] of
    [] -> pure ()
    refused -> do
      logErr $ mconcat [lstr n, " refused subscriptions: ", lstr refused]
      fail ("subscriptions refused at " <> unpack n <> ": " <> show refused)
