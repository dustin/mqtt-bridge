{-# LANGUAGE OverloadedStrings #-}

import           Test.Tasty
import           Test.Tasty.HUnit

import qualified Data.Map.Strict    as Map
import           Network.MQTT.Topic
import           Network.URI        (parseURI)

import           BridgeConf
import qualified DeliverySpec

testParser :: Assertion
testParser = do
  cfg <- parseConfFile "test/test.conf"
  let (Just u1) = parseURI "mqtt://localhost/"
      (Just u2) = parseURI "ws://test.mosquitto.org:8080/"
  assertEqual "" (BridgeConf
                  (Map.fromList
                  [("local", Conn "local" Ordered u1
                     (Map.fromList [("maximum-packet-size",8192),
                                    ("receive-maximum",64),
                                    ("session-expiry-interval",1800),
                                    ("topic-alias-maximum",4096)])),
                   ("test", (Conn "test" Ordered u2
                     (Map.fromList [("session-expiry-interval",187)])))])
                   [Sink "test" [Dest "VirtualTopic/#" "local" (TransFun "id" mkTopic),
                                 Dest "VirtualTopic/#" "local"
                                  (TransFun "rewrite" (const (Just "junkpile")))],
                    Sink "local" [Dest "tmp/#" "test" (TransFun "id" mkTopic)]]) cfg

-- H4: Connections deliver in order unless told otherwise, since MQTT
-- requires messages on a topic to arrive in order [5.0 §4.6].
testOrdering :: Assertion
testOrdering = do
  BridgeConf conns _ <- parseConfFile "test/ordering.conf"
  assertEqual "ordering" [("ord", Ordered), ("plain", Ordered), ("unord", Unordered)]
    [(n, ot) | (n, Conn _ ot _ _) <- Map.toList conns]

tests :: [TestTree]
tests = [
  testCase "example conf" testParser,
  testCase "ordering" testOrdering,
  testGroup "delivery" DeliverySpec.tests
  ]

main :: IO ()
main = defaultMain $ testGroup "All Tests" tests
