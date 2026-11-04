{-# LANGUAGE OverloadedStrings #-}

-- | Fake brokers for driving the bridge without a network.
module FakeBroker (
  FakeBroker(..), fakeBroker, connectFake, nthWritten, writtenSince, runBridge, quiet
  ) where

import           Control.Concurrent              (threadDelay)
import           Control.Concurrent.STM          (TVar, atomically, check, modifyTVar', newTChanIO, newTVarIO,
                                                  readTChan, readTVar, writeTChan)
import           Control.Monad                   (forever)
import           Control.Monad.IO.Class          (liftIO)
import           Control.Monad.Logger            (LoggingT, runLoggingT)
import           Control.Monad.Reader            (runReaderT)
import qualified Data.Attoparsec.ByteString.Lazy as A
import qualified Data.ByteString                 as BS
import qualified Data.ByteString.Lazy            as BL
import           Data.Conduit                    (awaitForever, yield)
import           System.Timeout                  (timeout)
import           Test.Tasty.HUnit                (assertFailure)

import           Network.MQTT.Client
import           Network.MQTT.Types

import           Bridge

data FakeBroker = FakeBroker {
  -- | Everything the client wrote after its CONNECT, one packet per chunk.
  written :: TVar [BS.ByteString],
  -- | Send the client a packet.
  inject  :: MQTTPkt -> IO (),
  conduit :: (MQTTConduit -> IO ()) -> IO ()
  }

-- | A fake broker that accepts a connection with the given CONNACK
-- properties.
fakeBroker :: [Property] -> IO FakeBroker
fakeBroker props = do
  w <- newTVarIO []
  ch <- newTChanIO
  let enc = BL.toStrict . toByteString Protocol50
      src = yield (enc $ ConnACKPkt (ConnACKFlags NewSession ConnAccepted props))
            >> forever (liftIO (atomically (readTChan ch)) >>= yield . enc)
      sink = awaitForever $ \bs -> liftIO . atomically $ modifyTVar' w (<> [bs])
  pure $ FakeBroker w (atomically . writeTChan ch) (\f -> f (src, sink))

-- | Connect a 5.0 client to a fake broker.
connectFake :: FakeBroker -> IO MQTTClient
connectFake fb = runMQTTConduit (conduit fb) mqttConfig{_protocol=Protocol50}

-- | Wait for the nth packet the client wrote after its CONNECT.
nthWritten :: Int -> FakeBroker -> IO MQTTPkt
nthWritten n fb = do
  got <- timeout 5000000 . atomically $ do
    ws <- drop 1 <$> readTVar (written fb)
    check (length ws >= n)
    pure (ws !! (n - 1))
  bs <- maybe (assertFailure ("packet " <> show n <> " never arrived")) pure got
  either (assertFailure . ("unparseable packet: " <>)) pure . A.eitherResult . A.parse (parsePacket Protocol50) $ BL.fromStrict bs

-- | The packets written after the first n, after waiting a moment for
-- any that are coming.
writtenSince :: Int -> FakeBroker -> IO [MQTTPkt]
writtenSince n fb = do
  threadDelay 200000
  ws <- drop (n + 1) <$> atomically (readTVar (written fb))
  pure [p | Right p <- A.eitherResult . A.parse (parsePacket Protocol50) . BL.fromStrict <$> ws]

quiet :: LoggingT IO a -> IO a
quiet = flip runLoggingT (\_ _ _ _ -> pure ())

-- | Run a bridge action in an environment, without logging.
runBridge :: Env -> Bridge a -> IO a
runBridge env = quiet . flip runReaderT env
