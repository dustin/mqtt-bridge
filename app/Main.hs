{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TupleSections     #-}

module Main where

import           Control.Concurrent.STM   (TVar, atomically, newTVarIO, readTVar, retry, writeTVar)
import           Control.Monad            (void, when)
import           Control.Monad.IO.Class   (MonadIO (..))
import           Control.Monad.IO.Unlift  (MonadUnliftIO, withRunInIO)
import           Control.Monad.Logger     (LogLevel (..), LoggingT, MonadLogger, filterLogger, logWithoutLoc,
                                           runStderrLoggingT)
import           Control.Monad.Reader     (ReaderT (..), asks, runReaderT)
import qualified Data.ByteString          as BS
import qualified Data.ByteString.Lazy     as BL
import           Data.Map.Strict          (Map)
import qualified Data.Map.Strict          as Map
import           Data.Text                (Text, pack)
import qualified Data.Text.Encoding       as TE
import           Data.Validation          (Validation (..))
import           Network.MQTT.Client
import           Network.MQTT.Topic       (match)
import           Network.MQTT.Types       (ConnACKFlags (..), PublishRequest (..), RetainHandling (..))
import           Network.URI
import           Options.Applicative      (Parser, auto, execParser, fullDesc, help, helper, info, long, option,
                                           progDesc, short, showDefault, strOption, switch, value, (<**>))
import           System.Remote.Counter    (Counter, inc)
import qualified System.Remote.Monitoring as RM
import           UnliftIO.Async           (async, mapConcurrently, mapConcurrently_, waitAnyCancel)

import           Bridge
import           BridgeConf

data Env = Env {
  conns   :: TVar (Map Server MQTTClient),
  dests   :: Map Server [Dest],
  metrics :: Metrics
  }

type Bridge = ReaderT Env (LoggingT IO)

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
  let dests = map (\(Dest _ s (TransFun _ f)) -> (s,f)) $ filter (\(Dest t _ _) -> match t topic) (dm Map.! n)
  mapM_ deliver dests

  where
    topic = (TE.decodeUtf8 . BL.toStrict) _pubTopic
    deliver :: (Server, Text -> Text) -> Bridge ()
    deliver (d,f) = do
      mcs' <- mcs
      let mc = mcs' Map.! d
          dtopic = f topic
      logDbg $ mconcat ["Delivering ", lstr topic, rewritten dtopic,
                        " (r=", lstr _pubRetain, ", props=", lstr _pubProps, ") to ", lstr d]
      Metrics{destCounters} <- asks metrics
      liftIO $ do
        inc (destCounters Map.! d)
        pubAliased mc dtopic _pubBody _pubRetain _pubQoS (filter cleanProps _pubProps)

        where
          cleanProps (PropTopicAlias _) = False
          cleanProps _                  = True
          rewritten dtopic
            | topic == dtopic = ""
            | otherwise       = " as " <> lstr dtopic

raceABunch_ :: (MonadUnliftIO m, MonadLogger m) => [m a] -> m ()
raceABunch_ is = mapM async is >>= void.waitAnyCancel

-- Do all the bridging.
run :: Options -> IO ()
run Options{..} = runStderrLoggingT . logfilt $ do
  -- Metrics
  metricServer <- liftIO $ RM.forkServer optEKGAddr optEKGPort

  vc <- validateConfig <$> liftIO (parseConfFile optConfFile)
  let fullConf@(BridgeConf conns sinks) = case vc of
                                            Success c -> c
                                            Failure f -> (error . show) f

  mets <- liftIO $ makeMetrics metricServer fullConf
  cmtv <- liftIO $ newTVarIO mempty
  let env = Env cmtv (Map.fromList $ map (\(Sink n d) -> (n,d)) sinks) mets
  flip runReaderT env $ do
    mcs' <- Map.fromList <$> mapConcurrently connect conns
    liftIO . atomically $ writeTVar cmtv mcs'
    mapConcurrently_ sub sinks
    raceABunch_ $ map (liftIO . waitForClient) (Map.elems mcs')

  where
    sub :: Sink -> Bridge ()
    sub (Sink n dests) = do
      m <- mcs
      logInfo $ mconcat ["subscribing ", lstr dests, " at ", lstr n]
      subrv <- liftIO $ subscribe (m Map.! n) [(t,subOptions{_subQoS=QoS2,
                                                             _noLocal=True,
                                                             _retainHandling=SendOnSubscribeNew,
                                                             _retainAsPublished=True}) | t <- destTopics dests] mempty
      logInfo $ mconcat ["Sub response from ", lstr n, ": ", lstr subrv]

    connect (Conn n u o) = do
      logInfo $ mconcat ["Connecting to ", lstr u, " with ", lstr (Map.toList o)]
      mc <- withRunInIO $ \unl -> do
        mc <- connectMQTT u o (copyMsg n unl)
        (ConnACKFlags sp _ props) <- connACK mc
        unl . logInfo $ mconcat ["Connected to ", lstr u, " ", lstr sp,  " - server properties: ", lstr props]
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
