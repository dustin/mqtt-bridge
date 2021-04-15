{-# LANGUAGE OverloadedStrings #-}

import           Test.QuickCheck
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck as QC

import qualified Data.Map.Strict       as Map
import           Network.URI           (parseURI)

import           BridgeConf

testParser :: Assertion
testParser = do
  cfg <- parseConfFile "test/test.conf"
  let (Just u1) = parseURI "mqtt://localhost/"
      (Just u2) = parseURI "ws://test.mosquitto.org:8080/"
  assertEqual "" (BridgeConf
                  [Conn "local" u1
                    (Map.fromList [("maximum-packet-size",8192),
                                   ("receive-maximum",64),
                                   ("session-expiry-interval",1800),
                                   ("topic-alias-maximum",4096)]),
                   Conn "test" u2
                    (Map.fromList [("session-expiry-interval",187)])]
                   [Sink "test" [Dest "VirtualTopic/#" "local" (TransFun "id" id),
                                 Dest "VirtualTopic/#" "local"
                                  (TransFun "rewrite" (const "junkpile"))],
                    Sink "local" [Dest "tmp/#" "test" (TransFun "id" id)]]) cfg

tests :: [TestTree]
tests = [
  testCase "example conf" testParser
  ]

main :: IO ()
main = defaultMain $ testGroup "All Tests" tests
