module Bridge (destFilters) where

import qualified Data.Set           as Set

import           Network.MQTT.Topic (Filter)

import           BridgeConf

destFilters :: [Dest] -> [Filter]
destFilters = Set.toList . Set.fromList . map (\(Dest t _ _) -> t)
