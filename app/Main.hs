{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Main where

import           Control.Concurrent.Async (mapConcurrently, mapConcurrently_)
import           Control.Concurrent.STM   (TVar, atomically, newTVarIO,
                                           readTVar, retry, writeTVar)
import           Control.Monad            (when)
import qualified Data.ByteString.Lazy     as BL
import           Data.Map.Strict          (Map)
import qualified Data.Map.Strict          as Map
import           Data.Text                (Text)
import qualified Data.Text.Encoding       as TE
import           Network.MQTT.Client
import           Network.MQTT.Topic       (match)
import           Network.MQTT.Types       (PublishRequest (..),
                                           RetainHandling (..))
import           Network.URI
import           Options.Applicative      (Parser, execParser, fullDesc, help,
                                           helper, info, long, progDesc,
                                           showDefault, strOption, value,
                                           (<**>))
import           System.Log.Logger        (Priority (INFO), infoM,
                                           rootLoggerName, setLevel,
                                           updateGlobalLogger)

import           Bridge
import           BridgeConf


data Options = Options {
  optConfFile      :: String
  }

options :: Parser Options
options = Options
  <$> strOption (long "conf" <> showDefault <> value "bridge.conf" <> help "config file")

connectMQTT :: URI -> Map Text Int -> (MQTTClient -> PublishRequest -> IO ()) -> IO MQTTClient
connectMQTT uri opts f = connectURI mqttConfig{_connID=cid protocol (uriFragment uri),
                                           _cleanSession=opt "session-expiry-interval" 0 == (0::Int),
                                           _protocol=protocol,
                                           _msgCB=LowLevelCallback f,
                                           _connProps=[PropSessionExpiryInterval $ opt "session-expiry-interval" 0,
                                                       PropTopicAliasMaximum $ opt "topic-alias-maximum" 2048,
                                                       PropMaximumPacketSize $ opt "maximum-packet-size" 65536,
                                                       PropReceiveMaximum $ opt "receive-maximum" 256,
                                                       PropRequestResponseInformation 1,
                                                       PropRequestProblemInformation 1]
                                           }
                    uri

  where
    protocol = if (opt "protocol" 5) == (3::Int)
               then Protocol311
               else Protocol50

    cid Protocol311 ['#'] = "mqttbridge"
    cid _ ('#':xs)        = xs
    cid Protocol311 _     = "mqttbridge"
    cid Protocol50 _      = ""

    opt t d = toEnum $ Map.findWithDefault d t opts


-- MQTT message callback that will look up a destination and deliver a message to it.
copyMsg :: TVar (Map Server MQTTClient) -> Map Server [Dest] -> Server -> (MQTTClient -> PublishRequest -> IO ())
copyMsg mcs dm n _ PublishRequest{..} = do
  mcs' <- atomically $ do
    m <- readTVar mcs
    when (null m) retry
    pure m

  let dests = map (\(Dest _ s (TransFun _ f)) -> (s,f)) $ filter (\(Dest t _ _) -> match t topic) (dm Map.! n)
  mapM_ (deliver mcs') dests

  where
    topic = (TE.decodeUtf8 . BL.toStrict) _pubTopic
    deliver :: Map Server MQTTClient -> (Server, Text -> Text) -> IO ()
    deliver mcs' (d,f) = do
      let mc = mcs' Map.! d
          dtopic = f topic
      infoM rootLoggerName $ mconcat ["Delivering ", show topic, rewritten dtopic,
                                      " (r=", show _pubRetain, ", props=", show _pubProps, ") to ", show d]
      pubAliased mc dtopic _pubBody _pubRetain _pubQoS _pubProps

        where
          rewritten dtopic
            | topic == dtopic = ""
            | otherwise       = " as " <> show dtopic

-- Do all the bridging.
run :: Options -> IO ()
run Options{..} = do
  fullConf@(BridgeConf conns sinks) <- parseConfFile optConfFile
  validateConfig fullConf
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
                                                    _retainAsPublished=True}) | t <- destTopics dests] mempty
      infoM rootLoggerName $ mconcat ["Sub response from ", show n, ": ", show subrv]

    connect :: TVar (Map Server MQTTClient) -> Map Server [Dest] -> Conn -> IO (Server, MQTTClient)
    connect cm dm (Conn n u o) = do
      infoM rootLoggerName $ mconcat ["Connecting to ", show u, " with ", show (Map.toList o)]
      mc <- connectMQTT u o (copyMsg cm dm n)
      props <- svrProps mc
      infoM rootLoggerName $ mconcat ["Connected to ", show u, " - server properties: ", show props]
      pure (n, mc)

main :: IO ()
main = do
  updateGlobalLogger rootLoggerName (setLevel INFO)
  run =<< execParser opts

  where opts = info (options <**> helper)
          ( fullDesc <> progDesc "Move stuff.")
