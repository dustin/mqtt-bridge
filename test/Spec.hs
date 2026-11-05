{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

import           Test.Tasty
import           Test.Tasty.HUnit

import           Control.Exception  (IOException, try)
import qualified Data.Map.Strict    as Map
import           Data.Text          (Text)
import qualified Data.Text          as T
import           Network.MQTT.Topic
import           Network.URI        (parseURI)

import           BridgeConf
import qualified DeliverySpec
import qualified StatsSpec

testParser :: Assertion
testParser = do
  cfg <- parseConfFile "test/test.conf"
  let (Just u1) = parseURI "mqtt://localhost/#bridge"
      (Just u2) = parseURI "ws://test.mosquitto.org:8080/#bridge"
  assertEqual "" (BridgeConf
                  (Map.fromList
                  [("local", Conn "local" u1
                     (mempty{coMaximumPacketSize=Just 8192,
                             coReceiveMaximum=Just 64,
                             coSessionExpiryInterval=Just 1800,
                             coTopicAliasMaximum=Just 4096})),
                   ("test", Conn "test" u2
                     (mempty{coSessionExpiryInterval=Just 187,
                             coOrdering = Just Ordered}))])
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
    [(n, ordering o) | (n, Conn _ _ o) <- Map.toList conns]

testStatPrefix :: Assertion
testStatPrefix = case parseConfText "stat-prefix" conf of
  Left e -> assertFailure e
  Right (BridgeConf conns _) -> case conns Map.! "a" of
    Conn _ _ opts -> assertEqual "last stat prefix wins" (Just "two") (coStatPrefix opts)
  where
    conf = T.unlines ["conn a mqtt://a/", "     stat-prefix=\"one\"", "     stat-prefix=\"two\"",
                      "from a", "     sync \"x/#\" -> a"]

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
  rejected "invalid stat prefix" "conn a mqtt://localhost/\n     stat-prefix=\"stats/#\"",
  rejected "protocol" "conn a mqtt://localhost/\n     protocol=4",
  rejected "receive maximum 0" "conn a mqtt://localhost/\n     receive-maximum=0",
  rejected "receive maximum too big" "conn a mqtt://localhost/\n     receive-maximum=65536",
  rejected "maximum packet size 0" "conn a mqtt://localhost/\n     maximum-packet-size=0",
  rejected "topic alias maximum too big" "conn a mqtt://localhost/\n     topic-alias-maximum=70000",
  rejected "session expiry too big" "conn a mqtt://localhost/\n     session-expiry-interval=4294967296",
  -- M7: A persistent session needs a client ID to come back to.
  rejected "persistent session without client ID" "conn a mqtt://localhost/\n     session-expiry-interval=60"
  ]

-- M7: The default Maximum Packet Size gets a warning, since the source
-- drops anything bigger without saying so.
testPacketSizeWarning :: Assertion
testPacketSizeWarning = do
  let Right (BridgeConf conns _) = parseConfText "warn" (confWith "conn a mqtt://localhost/\n     receive-maximum=10")
      warned n = not . null . connWarnings $ conns Map.! n
  assertBool "default packet size warned about" (warned "a")
  let Right (BridgeConf conns' _) = parseConfText "nowarn" (confWith "conn a mqtt://localhost/\n     maximum-packet-size=1000000")
  assertBool "explicit packet size not warned about" (null . connWarnings $ conns' Map.! "a")

-- H1: 3.1.1 has no No Local, so forwards to a 3.1.1 conn that also syncs
-- an overlapping filter come back, and no Retain As Published, so its
-- live messages arrive unretained.
testFiltersOverlap :: Assertion
testFiltersOverlap = do
  let o a b = filtersOverlap a b && filtersOverlap b a
  assertBool "x/# and x/y" (o "x/#" "x/y")
  assertBool "x/# and x" (o "x/#" "x")
  assertBool "+/y and x/+" (o "+/y" "x/+")
  assertBool "# and anything" (o "#" "a/b/c")
  assertBool "x/y and x/z" (not (o "x/y" "x/z"))
  assertBool "x/+ and x/y/z" (not (o "x/+" "x/y/z"))
  assertBool "+ and $SYS" (not (o "+" "$SYS"))

confWarned :: Text -> Int
confWarned c = either (const (-1)) (length . filter (not . T.isInfixOf "maximum-packet-size") . confWarnings) $
  parseConfText "h1" c

testProtocol311Warnings :: Assertion
testProtocol311Warnings = do
  let conf p g = T.unlines ["conn a mqtt://a/", "     maximum-packet-size=1000",
                            "conn b mqtt://b/", "     maximum-packet-size=1000", "     protocol=" <> p,
                            "from a", "     sync \"x/#\" -> b", "from b", "     sync \"" <> g <> "\" -> a"]
  -- b echoes a's forwards back, and its messages aren't retained.
  assertEqual "3.1.1, overlapping" 2 (confWarned (conf "3" "x/y"))
  -- Only the retain warning.
  assertEqual "3.1.1, separate" 1 (confWarned (conf "3" "z/#"))
  assertEqual "5.0" 0 (confWarned (conf "5" "x/y"))

-- #7: A rewrite from a wildcard collapses its topics into one retained
-- value, which gets a warning.
testRewriteWarning :: Assertion
testRewriteWarning = do
  let conf f = T.unlines ["conn a mqtt://a/", "     maximum-packet-size=1000",
                          "conn b mqtt://b/", "     maximum-packet-size=1000",
                          "from a", "     sync \"" <> f <> "\" -> b \"y\""]
  assertEqual "from a wildcard" 1 (confWarned (conf "x/#"))
  assertEqual "from a topic" 0 (confWarned (conf "x/1"))

tests :: [TestTree]
tests = [
  testCase "rewrite warning" testRewriteWarning,
  testCase "example conf" testParser,
  testCase "ordering" testOrdering,
  testCase "stat prefix" testStatPrefix,
  testCase "duplicate from" testDuplicateFrom,
  testGroup "bad config" badConfs,
  testCase "packet size warning" testPacketSizeWarning,
  testCase "filters overlap" testFiltersOverlap,
  testCase "3.1.1 warnings" testProtocol311Warnings,
  testGroup "shipped configs" shippedConfs,
  testGroup "delivery" DeliverySpec.tests,
  testGroup "stats" StatsSpec.tests
  ]

main :: IO ()
main = defaultMain $ testGroup "All Tests" tests
