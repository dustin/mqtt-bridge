{-# LANGUAGE OverloadedStrings #-}

module StatsSpec (tests) where

import qualified Data.Map.Strict as Map
import           Test.Tasty
import           Test.Tasty.HUnit

import           Stats

testStatStore :: Assertion
testStatStore = do
  ss <- newStatStore
  incrementStat (StatMsgRcvd "src") 2 ss
  incrementStat (StatMsgRcvd "src") 3 ss
  incrementStat (StatMsgSent "dst") 1 ss
  assertEqual "increments wait in the queue" Map.empty =<< retrieveStats ss
  flushStats ss
  assertEqual "increments aggregate"
    (Map.fromList [(StatMsgRcvd "src", 5), (StatMsgSent "dst", 1)]) =<< retrieveStats ss
  incrementStat (StatMsgRcvd "src") 4 ss
  flushStats ss
  assertEqual "later increments add to the totals" (Just 9)
    . Map.lookup (StatMsgRcvd "src") =<< retrieveStats ss

testStatKeyName :: Assertion
testStatKeyName =
  assertEqual "MQTT topic" "monitor/bridge/src/messages/failed"
    (statKeyName "monitor/bridge" (StatMsgFailed "src"))

tests :: [TestTree]
tests = [
  testCase "stat store aggregates queued increments" testStatStore,
  testCase "stat key names" testStatKeyName
  ]
