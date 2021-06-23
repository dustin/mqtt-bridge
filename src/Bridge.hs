module Bridge (destTopics) where

import qualified Data.Set           as Set

import           Network.MQTT.Topic (Topic)

import           BridgeConf

destTopics :: [Dest] -> [Topic]
destTopics = Set.toList . Set.fromList . map (\(Dest t _ _) -> t)
