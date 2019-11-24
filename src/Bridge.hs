module Bridge (validateConfig, destTopics) where

import           Data.Either        (fromLeft, isLeft)
import           Data.List          (intercalate)
import qualified Data.Set           as Set
import           Data.Text          (unpack)
import           Data.Validation

import           Network.MQTT.Topic (Topic)

import           BridgeConf

destTopics :: [Dest] -> [Topic]
destTopics = Set.toList . Set.fromList . map (\(Dest t _ _) -> t)

data ValidationError = DuplicateConnection String
                     | UnknownSinks String
                     | UnknownDests String
                     deriving (Show)

validateConfig :: BridgeConf -> Validation [ValidationError] BridgeConf
validateConfig conf@(BridgeConf conns sinks) =
  check (isLeft dups) (DuplicateConnection $ unpack (fromLeft undefined dups)) <*
  check ((not.null) unknownsinks) (UnknownSinks $ showset unknownsinks) <*
  check ((not.null) unknowndests) (UnknownDests $ showset unknowndests)

  where
    findDup :: Server -> Either Server (Set.Set Server) -> Either Server (Set.Set Server)
    findDup x s = case Set.member x <$> s of
                    Right True -> Left x
                    _          -> Set.insert x <$> s

    check True  = Failure . (:[])
    check False = const (Success conf)

    namel = map (\(Conn s _ _) -> s) conns
    dups = foldr findDup (Right mempty) namel
    names = Set.fromList namel
    sinknames = Set.fromList $ map (\(Sink s _) -> s) sinks
    unknownsinks = sinknames `Set.difference` names
    destnames = Set.fromList . concatMap (\(Sink _ dests) -> map (\(Dest _ s _) -> s) dests) $ sinks
    unknowndests = destnames `Set.difference` names
    showset s = mconcat ["[", (intercalate ", " . (unpack <$>) . Set.toList) s, "]"]
