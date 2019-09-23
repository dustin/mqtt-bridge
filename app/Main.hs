{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Main where

import           Control.Concurrent.Async (mapConcurrently, mapConcurrently_)
import           Control.Concurrent.STM   (STM, TVar, atomically, newTVarIO,
                                           readTVar, retry, writeTVar)
import           Control.Monad            (when)
import qualified Data.ByteString.Lazy     as BL
import           Data.Map.Strict          (Map)
import qualified Data.Map.Strict          as Map
import qualified Data.Text.Encoding       as TE
import           Network.MQTT.Client
import           Network.MQTT.Topic       (match)
import           Network.MQTT.Types       (PublishRequest (..),
                                           RetainHandling (..))
import           Network.URI
import           Options.Applicative      (Parser, execParser, fullDesc, help,
                                           helper, info, long, option, progDesc,
                                           showDefault, strOption, value,
                                           (<**>))
import           System.Log.Logger        (Priority (INFO), infoM,
                                           rootLoggerName, setLevel,
                                           updateGlobalLogger)

import           BridgeConf


data Options = Options {
  optConfFile      :: String
  }

options :: Parser Options
options = Options
  <$> strOption (long "conf" <> showDefault <> value "bridge.conf" <> help "config file")

connectMQTT :: URI -> (MQTTClient -> PublishRequest -> IO ()) -> IO MQTTClient
connectMQTT uri f = connectURI mqttConfig{_connID=cid (uriFragment uri),
                                           _cleanSession=False,
                                           _protocol=Protocol50,
                                           _msgCB=LowLevelCallback f,
                                           _connProps=[PropSessionExpiryInterval 900,
                                                       PropTopicAliasMaximum 500,
                                                       PropRequestResponseInformation 1,
                                                       PropRequestProblemInformation 1]
                                           }
                    uri

  where
    cid ['#']    = "mqttbridge"
    cid ('#':xs) = xs
    cid _        = "mqttbridge"


-- MQTT message callback that will look up a destination and deliver a message to it.
copyMsg :: TVar (Map Server MQTTClient) -> Map Server [Dest] -> Server -> (MQTTClient -> PublishRequest -> IO ())
copyMsg mcs dm n _ PublishRequest{..} = do
  mcs' <- atomically $ do
    m <- readTVar mcs
    when (null m) retry
    pure m

  let dests = map (\(Dest _ s) ->s) $ filter (\(Dest t _) -> match t topic) (dm Map.! n)
  mapM_ (deliver mcs') dests

  where
    topic = (TE.decodeUtf8 . BL.toStrict) _pubTopic
    deliver mcs d = do
      let mc = mcs Map.! d
      infoM rootLoggerName $ mconcat ["Delivering ", show topic,
                                      " (r=", show _pubRetain, ", props=", show _pubProps, ") to ", show d]
      pubAliased mc topic _pubBody _pubRetain _pubQoS _pubProps

-- Do all the bridging.
run :: Options -> IO ()
run Options{..} = do
  (BridgeConf conns sinks) <- parseConfFile optConfFile
  let dests = Map.fromList $ map (\(Sink n d) -> (n,d)) sinks
  cmtv <- newTVarIO mempty
  mcs <- Map.fromList <$> mapConcurrently (connect cmtv dests) conns
  atomically $ writeTVar cmtv mcs
  mapConcurrently_ (sub mcs) sinks
  mapConcurrently_ waitForClient (Map.elems mcs)

  where
    sub :: Map Server MQTTClient -> Sink -> IO ()
    sub m (Sink n dests) = do
      infoM rootLoggerName $  mconcat ["subscribing ", show dests, " at ", show n]
      subrv <- subscribe (m Map.! n) [(t,subOptions{_subQoS=QoS2,
                                                    _noLocal=True,
                                                    _retainHandling=SendOnSubscribeNew,
                                                    _retainAsPublished=True}) | (Dest t _) <- dests] mempty
      infoM rootLoggerName $ mconcat ["Sub response from ", show n, ": ", show subrv]

    connect :: TVar (Map Server MQTTClient) -> Map Server [Dest] -> Conn -> IO (Server, MQTTClient)
    connect cm dm (Conn n u) = do
      infoM rootLoggerName $ mconcat ["Connecting to ", show u]
      mc <- connectMQTT u (copyMsg cm dm n)
      pure (n, mc)

main :: IO ()
main = do
  updateGlobalLogger rootLoggerName (setLevel INFO)
  run =<< execParser opts

  where opts = info (options <**> helper)
          ( fullDesc <> progDesc "Move stuff.")
