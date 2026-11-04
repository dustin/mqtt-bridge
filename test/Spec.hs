{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

import           Test.Tasty
import           Test.Tasty.HUnit

import           Control.Exception  (IOException, try)
import qualified Data.Map.Strict    as Map
import           Data.Text          (Text)
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

-- M4: Two from blocks for one conn would both subscribe, but only one
-- would be used, so the config is rejected.
testDuplicateFrom :: Assertion
testDuplicateFrom = try (parseConfFile "test/dupfrom.conf") >>= \case
  Left (_ :: IOException) -> pure ()
  Right c -> assertFailure ("parsed: " <> show c)

-- L1: Bad config is rejected when it's read, not when it's used (or never).
confWith :: Text -> Text
confWith c = c <> "\nconn other mqtt://localhost/\n\nfrom other\n     sync \"a/#\" -> other\n"

rejected :: String -> Text -> TestTree
rejected name c = testCase name $ case parseConfText name (confWith c) of
  Left _  -> pure ()
  Right x -> assertFailure ("accepted: " <> show x)

-- The configs in the repo (bridge.conf ships in the Docker image) parse.
shippedConfs :: [TestTree]
shippedConfs = [testCase f (() <$ parseConfFile f) | f <- ["bridge.conf", "test.conf"]]

badConfs :: [TestTree]
badConfs = [
  rejected "invalid URI" "conn a not-a-uri",
  rejected "unknown option" "conn a mqtt://localhost/\n     session-expiry=1800",
  rejected "protocol" "conn a mqtt://localhost/\n     protocol=4",
  rejected "receive maximum 0" "conn a mqtt://localhost/\n     receive-maximum=0",
  rejected "receive maximum too big" "conn a mqtt://localhost/\n     receive-maximum=65536",
  rejected "maximum packet size 0" "conn a mqtt://localhost/\n     maximum-packet-size=0",
  rejected "topic alias maximum too big" "conn a mqtt://localhost/\n     topic-alias-maximum=70000",
  rejected "session expiry too big" "conn a mqtt://localhost/\n     session-expiry-interval=4294967296"
  ]

tests :: [TestTree]
tests = [
  testCase "example conf" testParser,
  testCase "ordering" testOrdering,
  testCase "duplicate from" testDuplicateFrom,
  testGroup "bad config" badConfs,
  testGroup "shipped configs" shippedConfs,
  testGroup "delivery" DeliverySpec.tests
  ]

main :: IO ()
main = defaultMain $ testGroup "All Tests" tests
