{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}

module DeliverySpec (tests) where

import           Control.Concurrent.STM (newTVarIO)
import           Control.Concurrent.Async (concurrently)
import           Control.Exception      (SomeException, try)
import           Data.Word              (Word8)
import qualified Data.Map.Strict        as Map
import qualified System.Metrics.Counter as Counter
import           Test.Tasty
import           Test.Tasty.HUnit

import           Network.MQTT.Client
import           Network.MQTT.Topic     (mkTopic)
import           Network.MQTT.Types

import           Bridge
import           BridgeConf
import           FakeBroker

-- | A bridge from "src" to "dst" for the given filters, with clients
-- connected to fake brokers.
withBridge :: [Property] -> [Dest] -> (Env -> MQTTClient -> FakeBroker -> IO a) -> IO a
withBridge dstProps ds f = do
  srcB <- fakeBroker []
  dstB <- fakeBroker dstProps
  src <- connectFake srcB
  dst <- connectFake dstB
  cv <- newTVarIO (Map.fromList [("src", src), ("dst", dst)])
  let names = ["src", "dst"]
      counters = Map.fromList <$> traverse (\n -> (,) n <$> Counter.new) names
  mets <- Metrics <$> counters <*> counters <*> counters
  f (Env cv (Map.singleton "src" ds) mets) src dstB

pub :: QoS -> PublishRequest
pub q = PublishRequest{_pubDup=False, _pubQoS=q, _pubPktID=1, _pubRetain=False,
                       _pubTopic="a/b", _pubBody="x", _pubProps=[]}

forward :: Env -> MQTTClient -> PublishRequest -> IO (Either SomeException ())
forward env src p = try $ copyMsg "src" (runBridge env) src p

-- | Forward a QoS 1 message from src while the fake destination acks
-- its nth PUBLISH with the given reason code.
forwardAcked :: Env -> MQTTClient -> FakeBroker -> Int -> Word8 -> IO (Either SomeException ())
forwardAcked env src dstB n rc = fst <$> concurrently (forward env src (pub QoS1)) ack
  where ack = nthWritten n dstB >>= \case
          PublishPkt PublishRequest{_pubPktID=pid} -> inject dstB (PubACKPkt (PubACK pid rc []))
          x -> assertFailure ("expected a PUBLISH, got " <> show x)

-- H3: A destination refusing a message doesn't take the source (and so
-- the process) down with it, and later messages are still forwarded.
unit_destinationErrorContained :: Assertion
unit_destinationErrorContained = withBridge [] [Dest "a/#" "dst" (TransFun "id" mkTopic)] $ \env src dstB -> do
  forwardAcked env src dstB 1 0x87 >>= either (\e -> assertFailure ("delivery error escaped: " <> show e)) pure
  forwardAcked env src dstB 2 0 >>= either (\e -> assertFailure ("second delivery failed: " <> show e)) pure

tests :: [TestTree]
tests = [
  testCase "destination error contained" unit_destinationErrorContained
  ]
