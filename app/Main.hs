{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TupleSections     #-}

module Main where

import           Control.Concurrent       (threadDelay)
import           Control.Concurrent.STM   (atomically, modifyTVar', newTVarIO,
                                           readTVarIO, writeTVar)
import           Control.Monad            (forever)
import           Control.Monad.IO.Class   (MonadIO (..))
import           Control.Monad.IO.Unlift  (withRunInIO)
import           Control.Monad.Logger     (LogLevel (..), filterLogger,
                                           runStderrLoggingT)
import           Control.Monad.Reader     (runReaderT)
import qualified Data.ByteString          as BS
import qualified Data.Map.Strict          as Map
import           Network.MQTT.Client
import           Network.MQTT.Types       (ConnACKFlags (..))
import           Options.Applicative      (Parser, auto, execParser, fullDesc,
                                           help, helper, info, long, option,
                                           progDesc, short, showDefault,
                                           strOption, switch, value, (<**>))
import qualified System.Remote.Monitoring as RM
import           UnliftIO.Async           (mapConcurrently, mapConcurrently_)
import           UnliftIO.Exception       (SomeAsyncException (..), finally,
                                           fromException, throwIO, tryAny)

import           Bridge
import           BridgeConf
import           Stats

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

-- Do all the bridging.
run :: Options -> IO ()
run Options{..} = runStderrLoggingT . logfilt $ do
  -- Metrics
  metricServer <- liftIO $ RM.forkServer optEKGAddr optEKGPort

  fullConf@(BridgeConf conns sinks) <- liftIO (parseConfFile optConfFile)

  mets <- liftIO $ makeMetrics metricServer fullConf
  ss <- newStatStoreWithKeys [k n | n <- Map.keys conns, k <- [StatMsgRcvd, StatMsgSent, StatMsgFailed]]
  mapM_ (logAt LevelWarn) (confWarnings fullConf)
  cmtv <- liftIO $ newTVarIO mempty
  liveClients <- liftIO $ newTVarIO mempty
  let prefixes = Map.mapMaybe (\(Conn _ _ o) -> coStatPrefix o) conns
      env = Env cmtv (Map.fromList $ map (\(Sink n d) -> (n,d)) sinks) mets prefixes ss
      session = finally runSession cleanup
      runSession = do
        clients <- Map.fromList <$> mapConcurrently connect (Map.elems conns)
        -- Persistent sources can deliver as soon as they connect.  Keep their
        -- callbacks blocked until every destination is available.
        liftIO . atomically $ writeTVar cmtv clients
        raceABunch_ $ [applyStats ss, runSubscriptions] <> map (liftIO . waitForClient) (Map.elems clients)
      runSubscriptions = do
        mapConcurrently_ subscribeSink sinks
        statsPublisher
      cleanup = do
        clients <- liftIO $ readTVarIO liveClients
        liftIO $ mapM_ stopClient (Map.elems clients)
        liftIO . atomically $ writeTVar cmtv mempty *> writeTVar liveClients mempty
      connect (Conn n u o) = do
        logInfo $ mconcat ["Connecting to ", lstr u, " with ", lstr o]
        mc <- withRunInIO $ \unl -> do
          mc <- connectMQTT (ordering o) u o (copyMsg n unl)
          (ConnACKFlags sp _ props) <- connACK mc
          unl . logInfo $ mconcat ["Connected to ", lstr u, " ", lstr sp,  " - server properties: ", lstr props]
          pure mc
        liftIO . atomically $ modifyTVar' liveClients (Map.insert n mc)
        pure (n, mc)
  flip runReaderT env $ forever $ do
    tryAny session >>= \case
      Right _ -> logInfo "A connection ended; reconnecting"
      Left e -> case fromException e of
        Just (SomeAsyncException _) -> throwIO e
        Nothing -> logErr $ mconcat ["Bridge session failed: ", lstr e, "; reconnecting"]
    liftIO $ threadDelay 1000000

  where
    makeMetrics :: RM.Server -> BridgeConf -> IO Metrics
    makeMetrics svr (BridgeConf conns _) = Metrics <$> counters "from" <*> counters "to" <*> counters "errors"
      where
        names = Map.keys conns
        counters k = Map.fromList <$> traverse (\s -> (s,) <$> RM.getCounter ("mqtt-bridge." <> k <> "." <> s) svr) names

    logfilt = filterLogger (\_ -> flip (if optVerbose then (>=) else (>)) LevelDebug)

main :: IO ()
main = run =<< execParser opts

  where opts = info (options <**> helper)
          ( fullDesc <> progDesc "Move stuff.")
