{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TupleSections     #-}

module Main where

import           Control.Concurrent.STM   (atomically, newTVarIO, writeTVar)
import           Control.Monad.IO.Class   (MonadIO (..))
import           Control.Monad.IO.Unlift  (withRunInIO)
import           Control.Monad.Logger     (LogLevel (..), filterLogger, runStderrLoggingT)
import           Control.Monad.Reader     (runReaderT)
import qualified Data.ByteString          as BS
import qualified Data.Map.Strict          as Map
import           Network.MQTT.Client
import           Network.MQTT.Types       (ConnACKFlags (..))
import           Options.Applicative      (Parser, auto, execParser, fullDesc, help, helper, info, long, option,
                                           progDesc, short, showDefault, strOption, switch, value, (<**>))
import qualified System.Remote.Monitoring as RM
import           UnliftIO.Async           (mapConcurrently, mapConcurrently_)

import           Bridge
import           BridgeConf

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
  cmtv <- liftIO $ newTVarIO mempty
  let env = Env cmtv (Map.fromList $ map (\(Sink n d) -> (n,d)) sinks) mets
  flip runReaderT env $ do
    mcs' <- Map.fromList <$> mapConcurrently connect (Map.elems conns)
    liftIO . atomically $ writeTVar cmtv mcs'
    mapConcurrently_ subscribeSink sinks
    raceABunch_ $ map (liftIO . waitForClient) (Map.elems mcs')

  where
    connect (Conn n ot u o) = do
      logInfo $ mconcat ["Connecting to ", lstr u, " with ", lstr (Map.toList o)]
      mc <- withRunInIO $ \unl -> do
        mc <- connectMQTT ot u o (copyMsg n unl)
        (ConnACKFlags sp _ props) <- connACK mc
        unl . logInfo $ mconcat ["Connected to ", lstr u, " ", lstr sp,  " - server properties: ", lstr props]
        pure mc
      pure (n, mc)

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
