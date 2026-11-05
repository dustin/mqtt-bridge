{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TupleSections     #-}

module Bridge (
  Env(..), Bridge, Metrics(..), destFilters, routes, connectMQTT, copyMsg, subscribeSink, mcs, fitTo,
  publishStats, statsPublisher, raceABunch_, logAt, logInfo, logDbg, logErr, lstr
  ) where

import           Control.Concurrent       (threadDelay)
import           Control.Concurrent.STM   (TVar, atomically, readTVar, retry)
import           Control.Monad            (forever, void, when, zipWithM)
import           Control.Monad.IO.Class   (MonadIO (..))
import           Control.Monad.IO.Unlift  (MonadUnliftIO)
import           Control.Monad.Logger     (LogLevel (..), LoggingT, MonadLogger, logWithoutLoc)
import           Control.Monad.Reader     (ReaderT (..), asks)
import qualified Data.ByteString.Lazy     as BL
import           Data.Containers.ListUtils (nubOrd)
import           Data.List                (findIndex)
import           Data.Map.Strict          (Map)
import qualified Data.Map.Strict          as Map
import           Data.Maybe               (fromMaybe)
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
import           Stats

-- | The filters to subscribe to for some destinations.  One that another
-- covers is left out, since the broker may deliver a copy of a message per
-- matching subscription, and each would be forwarded.  (Filters that only
-- partly overlap, like x/+ and +/y, can still both match.)
destFilters :: [Dest] -> [Filter]
destFilters ds = [f | f <- fs, not (any (\g -> g /= f && filterCovers g f) fs)]
  where fs = Set.toList . Set.fromList $ map (\(Dest t _ _) -> t) ds

-- | Where a message on a topic goes: each destination server and
-- rewritten topic at most once, however many 'Dest's match.
--
-- Each of 'destFilters' is subscribed with its (1-based) position as its
-- Subscription Identifier, so the broker tells us which subscriptions a
-- copy is for.  Filters that partly overlap (x/+ and +/y) can each bring
-- a copy of a message, so only the copy for the first matching filter is
-- delivered.  A message without identifiers (e.g. from a broker that
-- doesn't support them) is delivered.
routes :: [Dest] -> [Int] -> Topic -> [(Server, Maybe Topic)]
routes ds subIDs topic
  | wanted = nubOrd [(s, f (unTopic topic)) | Dest t s (TransFun _ f) <- ds, match t topic]
  | otherwise = []
  where
    wanted = null subIDs || maybe False ((`elem` subIDs) . (+1)) (findIndex (`match` topic) (destFilters ds))

data Env = Env {
  conns        :: TVar (Map Server MQTTClient),
  dests        :: Map Server [Dest],
  metrics      :: Metrics,
  statPrefixes :: Map Server Topic,
  stats        :: StatStore
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

connectMQTT :: OrderedType -> URI -> ConnOptions -> (MQTTClient -> PublishRequest -> IO ()) -> IO MQTTClient
connectMQTT ot uri ConnOptions{..} f =
  connectURI mqttConfig{_cleanSession=fromMaybe 0 coSessionExpiryInterval == (0::Int),
                        _protocol=protocol,
                        _msgCB=con f,
                        _connProps=[PropSessionExpiryInterval $ opt coSessionExpiryInterval 0,
                                    PropTopicAliasMaximum $ opt coTopicAliasMaximum 2048,
                                    PropMaximumPacketSize $ opt coMaximumPacketSize 65536,
                                    PropReceiveMaximum $ opt coReceiveMaximum 256,
                                    PropRequestResponseInformation 1,
                                    PropRequestProblemInformation 1]
                       } uri

  where
    protocol = if fromMaybe 5 coProtocol == (3::Int)
               then Protocol311
               else Protocol50

    opt v d = fromIntegral (fromMaybe d v)

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

-- | Publish the current counters as retained values on connections with a
-- configured stat prefix.  These are sent directly, rather than through
-- 'copyMsg', so reporting stats doesn't itself increment them, but they're
-- still fitted to the broker's limits (e.g. not retained if it doesn't
-- support that).
publishStats :: Bridge ()
publishStats = do
  ss <- asks stats
  flushStats ss
  counts <- retrieveStats ss
  clients <- mcs
  prefixes <- asks statPrefixes
  sequence_ [publishOne n prefix mc kv | (n, prefix) <- Map.toList prefixes,
                                          mc <- maybe [] pure (Map.lookup n clients),
                                          kv <- Map.assocs counts]
  where
    publishOne n prefix mc (k, v) = case mkTopic (statKeyName prefix k) of
      Nothing -> logErr $ "invalid stats topic for " <> lstr k
      Just t  -> do
        r <- liftIO $ sendFitted mc t PublishRequest{_pubDup=False, _pubQoS=QoS0, _pubPktID=0, _pubRetain=True,
                                                     _pubTopic=mempty, _pubBody=BL.fromStrict . TE.encodeUtf8 . lstr $ v,
                                                     _pubProps=[PropMessageExpiryInterval 60]}
        either (\e -> logErr $ mconcat ["Not publishing ", unTopic t, " to ", lstr n, ": ", e]) pure r

-- | Publish to a topic, fitted to the client's server properties (see
-- 'fitTo').  A message that can't be fitted isn't sent, and its reason is
-- returned.
sendFitted :: MQTTClient -> Topic -> PublishRequest -> IO (Either Text ())
sendFitted mc t p = do
  sp <- svrProps mc
  case fitTo sp p{_pubTopic=BL.fromStrict (TE.encodeUtf8 (unTopic t))} of
    Left e -> pure (Left e)
    Right PublishRequest{..} -> Right <$> pubAliased mc t _pubBody _pubRetain _pubQoS _pubProps

-- | Refresh the retained stats periodically.
statsPublisher :: Bridge ()
statsPublisher = forever $ publishStats *> liftIO (threadDelay 15000000)

-- MQTT message callback that will look up a destination and deliver a message to it.
copyMsg :: Server -> (Bridge () -> IO ()) -> (MQTTClient -> PublishRequest -> IO ())
copyMsg n unl _ PublishRequest{..} = unl $ do
  dm <- asks dests
  Metrics{srcCounters} <- asks metrics

  liftIO $ inc (srcCounters Map.! n)
  incrementStat (StatMsgRcvd n) 1 =<< asks stats
  case ((mkTopic . TE.decodeUtf8 . BL.toStrict) _pubTopic, Map.lookup n dm) of
    (Nothing, _) -> logErr $ mconcat ["Dropping a message from ", lstr n, " with an invalid topic: ", lstr _pubTopic]
    -- No from block for this conn, so it's from a subscription that's no
    -- longer configured (a resumed session's).
    (Just topic, Nothing) -> logDbg $ mconcat ["Dropping ", unTopic topic, " from ", lstr n, ", which has nothing to sync"]
    (Just topic, Just ds) ->
      mapM_ (contained topic) (routes ds [i | PropSubscriptionIdentifier i <- _pubProps] topic)

  where
    -- A destination refusing a message (or its being unsendable, e.g. too
    -- big) is logged and counted, and doesn't escape into the source
    -- connection: it would fail the same way every time, so retrying would
    -- only crash-loop the bridge.  But if the destination's gone, a QoS 1
    -- or 2 message is failed rather than acked, so the source redelivers
    -- it after the supervisor reconnects the bridge.  A QoS 0 one can't be
    -- redelivered, so it's dropped.
    contained :: Topic -> (Server, Maybe Topic) -> Bridge ()
    contained topic (d, dt) = tryAny (deliver topic (d, dt)) >>= either (failed topic d) pure

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
          incrementStat (StatMsgFailed d) 1 =<< asks stats
          logErr $ mconcat ["Failed to deliver ", unTopic topic, " to ", lstr d, ": ", lstr e]

    deliver :: Topic -> (Server, Maybe Topic) -> Bridge ()
    deliver topic (d, dtopic) = do
      mcs' <- mcs
      let mc = mcs' Map.! d
      dt <- maybe (fail ("no destination topic for " <> show topic)) pure dtopic
      logDbg $ mconcat ["Delivering ", unTopic topic, rewritten dt,
                        " (r=", lstr _pubRetain, ", props=", lstr _pubProps, ") to ", lstr d]
      Metrics{destCounters} <- asks metrics
      ss <- asks stats
      liftIO $ do
        inc (destCounters Map.! d)
        either (fail . unpack) pure =<< sendFitted mc dt PublishRequest{_pubProps=filter cleanProps _pubProps, ..}
        incrementStat (StatMsgSent d) 1 ss

        where
          cleanProps (PropTopicAlias _)             = False
          cleanProps (PropSubscriptionIdentifier _) = False
          cleanProps _                              = True
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
    -- Measured with the full topic, and with the Topic Alias 'pubAliased'
    -- adds to a topic's first PUBLISH when the destination allows aliases.
    size = BL.length (toByteString Protocol50 (PublishPkt measured))
    measured
      | any (> 0) [n | PropTopicAliasMaximum n <- props] = fitted{_pubProps=PropTopicAlias maxBound : _pubProps fitted}
      | otherwise = fitted
    tooBig = any (\m -> size > fromIntegral m) [m | PropMaximumPacketSize m <- props]

-- | How sinks subscribe.  QoS 1, not 2: an inbound QoS 1 message is acked
-- once it's been forwarded, so a restart redelivers it, but net-mqtt sends
-- PUBREC for a QoS 2 one before handing it over, so one caught in between
-- is lost.
sinkSubOptions :: SubOptions
sinkSubOptions = subOptions{_subQoS=QoS1, _noLocal=True, _retainHandling=SendOnSubscribeNew, _retainAsPublished=True}

raceABunch_ :: (MonadUnliftIO m, MonadLogger m) => [m a] -> m ()
raceABunch_ is = mapM async is >>= void.waitAnyCancel

-- | Subscribe a sink's filters at its source.
subscribeSink :: Sink -> Bridge ()
subscribeSink (Sink n dests) = do
  m <- mcs
  logInfo $ mconcat ["subscribing ", lstr dests, " at ", lstr n]
  let filters = destFilters dests
      mc = m Map.! n
  sp <- liftIO $ svrProps mc
  -- A SUBSCRIBE's identifier applies to all its filters, so each filter
  -- gets its own, numbered as 'routes' expects.
  results <- if PropSubscriptionIdentifierAvailable 0 `elem` sp
    then do
      subrv@(rs, _) <- liftIO $ subscribe mc [(t, sinkSubOptions) | t <- filters] mempty
      logInfo $ mconcat ["Sub response from ", lstr n, ": ", lstr subrv]
      pure rs
    else concat <$> zipWithM (subscribeOne mc) [1..] filters
  -- A refused filter would leave the bridge running without it.
  case [(f, e) | (f, Left e) <- zip filters results] of
    [] -> pure ()
    refused -> do
      logErr $ mconcat [lstr n, " refused subscriptions: ", lstr refused]
      fail ("subscriptions refused at " <> unpack n <> ": " <> show refused)
  where
    subscribeOne mc i t = do
      subrv@(rs, _) <- liftIO $ subscribe mc [(t, sinkSubOptions)] [PropSubscriptionIdentifier i]
      logInfo $ mconcat ["Sub response from ", lstr n, " for ", lstr t, ": ", lstr subrv]
      pure rs
