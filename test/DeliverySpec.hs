{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DeliverySpec (tests) where

import           Control.Concurrent.STM (newTVarIO, readTVarIO)
import           Control.Concurrent.Async (concurrently)
import           Control.Exception      (SomeException, try)
import qualified Data.ByteString.Lazy   as BL
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
import           Stats

-- | A bridge from "src" to "dst" for the given filters, with clients
-- connected to fake brokers.
withBridge :: [Property] -> [Dest] -> (Env -> MQTTClient -> FakeBroker -> IO a) -> IO a
withBridge dstProps ds f = withBridge' dstProps ds (\env src _ dstB -> f env src dstB)

-- | withBridge, with the source's fake broker too.
withBridge' :: [Property] -> [Dest] -> (Env -> MQTTClient -> FakeBroker -> FakeBroker -> IO a) -> IO a
withBridge' dstProps ds f = do
  srcB <- fakeBroker []
  dstB <- fakeBroker dstProps
  src <- connectFake srcB
  dst <- connectFake dstB
  cv <- newTVarIO (Map.fromList [("src", src), ("dst", dst)])
  let names = ["src", "dst"]
      counters = Map.fromList <$> traverse (\n -> (,) n <$> Counter.new) names
  mets <- Metrics <$> counters <*> counters <*> counters
  ss <- newStatStore
  f (Env cv (Map.singleton "src" ds) mets mempty ss) src srcB dstB

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

-- H5: Forwards fit the destination's CONNACK limits [5.0 §3.2.2.3].
toDst :: [Dest]
toDst = [Dest "a/#" "dst" (TransFun "id" mkTopic)]

-- A QoS 2 message goes to a destination with Maximum QoS 1 at QoS 1
-- [MQTT-3.2.2-11].
unit_maximumQoS :: Assertion
unit_maximumQoS = withBridge [PropMaximumQoS 1] toDst $ \env src dstB -> do
  let ack = nthWritten 1 dstB >>= \case
        PublishPkt PublishRequest{_pubQoS, _pubPktID} -> do
          assertEqual "forwarded QoS" QoS1 _pubQoS
          inject dstB (PubACKPkt (PubACK _pubPktID 0 []))
        x -> assertFailure ("expected a PUBLISH, got " <> show x)
  (r, _) <- concurrently (forward env src (pub QoS2)) ack
  either (\e -> assertFailure (show e)) pure r

-- A retained message goes to a destination without retain support as
-- not retained [MQTT-3.2.2-14].
unit_retainAvailable :: Assertion
unit_retainAvailable = withBridge [PropRetainAvailable 0] toDst $ \env src dstB -> do
  _ <- forward env src (pub QoS0){_pubRetain=True}
  nthWritten 1 dstB >>= \case
    PublishPkt PublishRequest{_pubRetain} -> assertBool "forwarded retained" (not _pubRetain)
    x -> assertFailure ("expected a PUBLISH, got " <> show x)

-- A message bigger than the destination's Maximum Packet Size isn't sent
-- [MQTT-3.2.2-15], and later ones still are.
unit_maximumPacketSize :: Assertion
unit_maximumPacketSize = withBridge [PropMaximumPacketSize 100] toDst $ \env src dstB -> do
  _ <- forward env src (pub QoS0){_pubBody=BL.replicate 200 0x78}
  _ <- forward env src (pub QoS0){_pubBody="small"}
  nthWritten 1 dstB >>= \case
    PublishPkt PublishRequest{_pubBody} -> assertEqual "first forwarded" "small" _pubBody
    x -> assertFailure ("expected a PUBLISH, got " <> show x)

-- M4: A message on a connection with no from block (e.g. from a resumed
-- session's old subscriptions) is dropped, not a crash.
unit_unconfiguredSource :: Assertion
unit_unconfiguredSource = withBridge [] toDst $ \env _ _ -> do
  dst <- (Map.! "dst") <$> readTVarIO (conns env)
  r <- try $ copyMsg "dst" (runBridge env) dst (pub QoS0)
  either (\e -> assertFailure ("crashed: " <> show (e :: SomeException))) pure r

-- M5: A filter the source refuses fails startup, rather than leaving the
-- bridge running without it.
unit_subscribeRefused :: Assertion
unit_subscribeRefused = withBridge' [] toDst $ \env _ srcB _ -> do
  let refuse = nthWritten 1 srcB >>= \case
        SubscribePkt (SubscribeRequest pid _ _) ->
          inject srcB (SubACKPkt (SubscribeResponse pid [Left SubErrNotAuthorized] []))
        x -> assertFailure ("expected a SUBSCRIBE, got " <> show x)
  (r, _) <- concurrently (try (runBridge env (subscribeSink (Sink "src" toDst)))) refuse
  either (\(_ :: SomeException) -> pure ()) (const $ assertFailure "subscribe failure ignored") r

-- M1: Sinks subscribe at QoS 1, which is acked only once the message is
-- forwarded, so a restart redelivers it.  (At QoS 2, net-mqtt sends PUBREC
-- before the message is handed over, so one in between is lost.)
unit_subscribesAtQoS1 :: Assertion
unit_subscribesAtQoS1 = withBridge' [] toDst $ \env _ srcB _ -> do
  let grant = nthWritten 1 srcB >>= \case
        SubscribePkt (SubscribeRequest pid subs _) -> do
          assertEqual "subscription QoS" [QoS1] [_subQoS o | (_, o) <- subs]
          inject srcB (SubACKPkt (SubscribeResponse pid [Right QoS1] []))
        x -> assertFailure ("expected a SUBSCRIBE, got " <> show x)
  _ <- concurrently (runBridge env (subscribeSink (Sink "src" toDst))) grant
  pure ()

-- M3: A filter another one in the sink covers isn't subscribed too, since
-- the broker may deliver a copy per matching subscription, and each copy
-- would be forwarded.
unit_coveredFiltersCollapsed :: Assertion
unit_coveredFiltersCollapsed = do
  let d f = Dest f "dst" (TransFun "id" mkTopic)
  assertEqual "subscribed" ["+/z", "a/#", "x/+"] (destFilters (map d ["a/#", "a/b", "a/b/#", "x/+", "x/y", "+/z", "a/#"]))
  -- A wildcard first level doesn't cover a $ topic [MQTT-4.7.2-1].
  assertEqual "$ topics" ["#", "$SYS/x"] (destFilters (map d ["#", "$SYS/x"]))

-- N1: A QoS 1 message that couldn't be delivered because the destination's
-- gone isn't acked to the source, so it's redelivered.  (A destination
-- refusing it is different: that's acked and dropped, above.)
unit_lostDestinationNotAcked :: Assertion
unit_lostDestinationNotAcked = withBridge [] toDst $ \env src _ -> do
  dst <- (Map.! "dst") <$> readTVarIO (conns env)
  stopClient dst
  forward env src (pub QoS1) >>= either (const (pure ())) (const $ assertFailure "delivered to a lost destination")

-- ...but a QoS 0 one can't be redelivered, so it's just dropped.
unit_lostDestinationQoS0 :: Assertion
unit_lostDestinationQoS0 = withBridge [] toDst $ \env src _ -> do
  dst <- (Map.! "dst") <$> readTVarIO (conns env)
  stopClient dst
  forward env src (pub QoS0) >>= either (\e -> assertFailure ("failed: " <> show e)) pure

-- Delivery updates the per-connection counters without conflating a failed
-- send with a successful one.
unit_deliveryStats :: Assertion
unit_deliveryStats = withBridge [] toDst $ \env src _ -> do
  _ <- forward env src (pub QoS0)
  flushStats (stats env)
  counts <- retrieveStats (stats env)
  assertEqual "successful delivery counters"
    (Map.fromList [(StatMsgRcvd "src", 1), (StatMsgSent "dst", 1)]) counts

unit_failedDeliveryStats :: Assertion
unit_failedDeliveryStats = withBridge [] toDst $ \env src dstB -> do
  forwardAcked env src dstB 1 0x87 >>= either (\e -> assertFailure ("delivery error escaped: " <> show e)) pure
  flushStats (stats env)
  counts <- retrieveStats (stats env)
  assertEqual "failed delivery counters"
    (Map.fromList [(StatMsgRcvd "src", 1), (StatMsgFailed "dst", 1)]) counts

-- Stats go only to a broker with a configured prefix, as retained values,
-- without being forwarded as ordinary bridge traffic.
unit_publishesStats :: Assertion
unit_publishesStats = withBridge' [] toDst $ \env _ srcB dstB -> do
  incrementStat (StatMsgRcvd "src") 7 (stats env)
  let env' = env{statPrefixes=Map.singleton "dst" "monitor/bridge"}
  runBridge env' publishStats
  nthWritten 1 dstB >>= \case
    PublishPkt PublishRequest{_pubTopic, _pubBody, _pubRetain, _pubQoS} -> do
      assertEqual "stats topic" "monitor/bridge/src/messages/received" _pubTopic
      assertEqual "counter value" "7" _pubBody
      assertBool "retained" _pubRetain
      assertEqual "QoS" QoS0 _pubQoS
    x -> assertFailure ("expected a stats PUBLISH, got " <> show x)
  assertEqual "no stats on an unconfigured connection" [] =<< writtenSince 0 srcB

-- #1: A message that fits only without the Topic Alias the first send of a
-- topic carries isn't sent to a destination that allows aliases (where
-- it would get the bridge disconnected), and smaller ones still are.
unit_aliasCounted :: Assertion
unit_aliasCounted = withBridge [PropMaximumPacketSize limit, PropTopicAliasMaximum 10] toDst $ \env src dstB -> do
  _ <- forward env src (pub QoS0){_pubBody=big}
  _ <- forward env src (pub QoS0){_pubBody="x"}
  nthWritten 1 dstB >>= \case
    PublishPkt PublishRequest{_pubBody} -> assertEqual "first forwarded" "x" _pubBody
    x -> assertFailure ("expected a PUBLISH, got " <> show x)
  where
    big = BL.replicate 10 0x78
    limit = fromIntegral . BL.length . toByteString Protocol50 $ PublishPkt (pub QoS0){_pubBody=big}

-- #2: Stats aren't retained on a broker without retain support.
unit_statsRetainAvailable :: Assertion
unit_statsRetainAvailable = withBridge [PropRetainAvailable 0] toDst $ \env _ dstB -> do
  runBridge env{statPrefixes=Map.singleton "dst" "monitor"} publishStats
  incrementStat (StatMsgRcvd "src") 1 (stats env)
  runBridge env{statPrefixes=Map.singleton "dst" "monitor"} publishStats
  nthWritten 1 dstB >>= \case
    PublishPkt PublishRequest{_pubRetain} -> assertBool "stats retained" (not _pubRetain)
    x -> assertFailure ("expected a stats PUBLISH, got " <> show x)

tests :: [TestTree]
tests = [
  testCase "alias counted in packet size" unit_aliasCounted,
  testCase "stats respect retain available" unit_statsRetainAvailable,
  testCase "publishes stats" unit_publishesStats,
  testCase "failed delivery stats" unit_failedDeliveryStats,
  testCase "delivery stats" unit_deliveryStats,
  testCase "lost destination not acked" unit_lostDestinationNotAcked,
  testCase "lost destination, QoS 0" unit_lostDestinationQoS0,
  testCase "covered filters collapsed" unit_coveredFiltersCollapsed,
  testCase "subscribes at QoS 1" unit_subscribesAtQoS1,
  testCase "subscribe refused" unit_subscribeRefused,
  testCase "unconfigured source" unit_unconfiguredSource,
  testCase "destination error contained" unit_destinationErrorContained,
  testCase "maximum QoS" unit_maximumQoS,
  testCase "retain available" unit_retainAvailable,
  testCase "maximum packet size" unit_maximumPacketSize
  ]
