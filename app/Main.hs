{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TupleSections     #-}

module Main where

import           Control.Concurrent.STM   (TVar, atomically, newTVarIO,
                                           readTVar, retry, writeTVar)
import           Control.Monad            (void, when)
import           Control.Monad.IO.Class   (MonadIO (..))
import           Control.Monad.IO.Unlift  (MonadUnliftIO, withRunInIO)
import           Control.Monad.Logger     (LogLevel (..), MonadLogger,
                                           filterLogger, logWithoutLoc,
                                           runStderrLoggingT)
import qualified Data.ByteString          as BS
import qualified Data.ByteString.Lazy     as BL
import           Data.Map.Strict          (Map)
import qualified Data.Map.Strict          as Map
import           Data.Text                (Text, pack)
import qualified Data.Text.Encoding       as TE
import           Data.Validation          (Validation (..))
import           Network.MQTT.Client
import           Network.MQTT.Topic       (match)
import           Network.MQTT.Types       (PublishRequest (..),
                                           RetainHandling (..))
import           Network.URI
import           Options.Applicative      (Parser, auto, execParser, fullDesc,
                                           help, helper, info, long, option,
                                           progDesc, short, showDefault,
                                           strOption, switch, value, (<**>))
import           System.Remote.Counter    (Counter, inc)
import qualified System.Remote.Monitoring as RM
import           UnliftIO.Async           (async, mapConcurrently,
                                           mapConcurrently_, waitAnyCancel)

import           Bridge
import           BridgeConf


type Counters = (Counter, Counter)

data Metrics = Metrics {
  srcCounters  :: Map Server Counter,
  destCounters :: Map Server Counter
  }

data Options = Options {
  optConfFile :: String,
  optEKGAddr  :: BS.ByteString,
  optEKGPort  :: Int,
  optVerbose  :: Bool
  }

options :: Parser Options
options = Options
  <$> strOption (long "conf" <> showDefault <> value "bridge.conf" <> help "config file")
  <*> strOption (long "ekgaddr" <> showDefault <> value "localhost" <> help "EKG listen address")
  <*> option auto (long "ekgport" <> showDefault <> value 8000 <> help "EKG listen port")
  <*> switch (short 'v' <> long "verbose" <> help "enable debug logging")

logAt :: MonadLogger m => LogLevel -> Text -> m ()
logAt l = logWithoutLoc "" l

logInfo :: MonadLogger m => Text -> m ()
logInfo = logAt LevelInfo

logDbg :: MonadLogger m => Text -> m ()
logDbg = logAt LevelDebug

lstr :: Show a => a -> Text
lstr = pack . show

connectMQTT :: URI -> Map Text Int -> (MQTTClient -> PublishRequest -> IO ()) -> IO MQTTClient
connectMQTT uri opts f = connectURI mqttConfig{_cleanSession=opt "session-expiry-interval" 0 == (0::Int),
                                               _protocol=protocol,
                                               _msgCB=LowLevelCallback f,
                                               _connProps=[PropSessionExpiryInterval $ opt "session-expiry-interval" 0,
                                                           PropTopicAliasMaximum $ opt "topic-alias-maximum" 2048,
                                                           PropMaximumPacketSize $ opt "maximum-packet-size" 65536,
                                                           PropReceiveMaximum $ opt "receive-maximum" 256,
                                                           PropRequestResponseInformation 1,
                                                           PropRequestProblemInformation 1]
                                              } uri

  where
    protocol = if (opt "protocol" 5) == (3::Int)
               then Protocol311
               else Protocol50

    opt t d = toEnum $ Map.findWithDefault d t opts


-- MQTT message callback that will look up a destination and deliver a message to it.
copyMsg :: (MonadIO m, MonadLogger m) => TVar (Map Server MQTTClient) -> Map Server [Dest] -> Server -> Metrics -> (m () -> IO ()) -> (MQTTClient -> PublishRequest -> IO ())
copyMsg mcs dm n Metrics{..} unl _ PublishRequest{..} = do
  mcs' <- atomically $ do
    m <- readTVar mcs
    when (null m) retry
    pure m

  inc (srcCounters Map.! n)
  let dests = map (\(Dest _ s (TransFun _ f)) -> (s,f)) $ filter (\(Dest t _ _) -> match t topic) (dm Map.! n)
  unl $ mapM_ (deliver mcs') dests

  where
    topic = (TE.decodeUtf8 . BL.toStrict) _pubTopic
    deliver :: (MonadLogger m, MonadIO m) => Map Server MQTTClient -> (Server, Text -> Text) -> m ()
    deliver mcs' (d,f) = do
      let mc = mcs' Map.! d
          dtopic = f topic
      logDbg $ mconcat ["Delivering ", lstr topic, rewritten dtopic,
                        " (r=", lstr _pubRetain, ", props=", lstr _pubProps, ") to ", lstr d]
      liftIO $ inc (destCounters Map.! d)
      liftIO $ pubAliased mc dtopic _pubBody _pubRetain _pubQoS _pubProps

        where
          rewritten dtopic
            | topic == dtopic = ""
            | otherwise       = " as " <> lstr dtopic

raceABunch_ :: (MonadUnliftIO m, MonadLogger m) => [m a] -> m ()
raceABunch_ is = mapM async is >>= liftIO.void.waitAnyCancel

-- Do all the bridging.
run :: Options -> IO ()
run Options{..} = runStderrLoggingT . logfilt $ do
  -- Metrics
  metricServer <- liftIO $ RM.forkServer optEKGAddr optEKGPort

  vc <- validateConfig <$> liftIO (parseConfFile optConfFile)
  let fullConf@(BridgeConf conns sinks) = case vc of
                                            Success c -> c
                                            Failure f -> (error . show) f

  metrics <- liftIO $ makeMetrics metricServer fullConf
  let dests = Map.fromList $ map (\(Sink n d) -> (n,d)) sinks
  cmtv <- liftIO $ newTVarIO mempty
  mcs <- Map.fromList <$> mapConcurrently (connect cmtv dests metrics) conns
  liftIO . atomically $ writeTVar cmtv mcs
  mapConcurrently_ (sub mcs) sinks
  raceABunch_ $ map (liftIO . waitForClient) (Map.elems mcs)

  where
    sub :: (MonadIO m, MonadLogger m) => Map Server MQTTClient -> Sink -> m ()
    sub m (Sink n dests) = do
      logInfo $ mconcat ["subscribing ", lstr dests, " at ", lstr n]
      subrv <- liftIO $ subscribe (m Map.! n) [(t,subOptions{_subQoS=QoS2,
                                                             _noLocal=True,
                                                             _retainHandling=SendOnSubscribeNew,
                                                             _retainAsPublished=True}) | t <- destTopics dests] mempty
      logInfo $ mconcat ["Sub response from ", lstr n, ": ", lstr subrv]

    connect cm dm metrics (Conn n u o) = do
      logInfo $ mconcat ["Connecting to ", lstr u, " with ", lstr (Map.toList o)]
      mc <- withRunInIO $ \unl -> do
        mc <- connectMQTT u o (copyMsg cm dm n metrics unl)
        props <- svrProps mc
        unl . logInfo $ mconcat ["Connected to ", lstr u, " - server properties: ", lstr props]
        pure mc
      pure (n, mc)

    makeMetrics :: RM.Server -> BridgeConf -> IO Metrics
    makeMetrics svr (BridgeConf conns _) = Metrics <$> srcConfs <*> dstConfs
      where
        names = map (\(Conn s _ _) -> s) conns
        srcConfs = Map.fromList <$> traverse (\s -> (s,) <$> RM.getCounter ("mqtt-bridge.from." <> s) svr) names
        dstConfs = Map.fromList <$> traverse (\s -> (s,) <$> RM.getCounter ("mqtt-bridge.to." <> s) svr) names

    logfilt = filterLogger (\_ -> flip (if optVerbose then (>=) else (>)) LevelDebug)

main :: IO ()
main = run =<< execParser opts

  where opts = info (options <**> helper)
          ( fullDesc <> progDesc "Move stuff.")
