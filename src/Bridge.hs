module Bridge (validateConfig, destTopics) where

import           Control.Monad      (when)
import           Data.Either        (fromLeft, isLeft)
import qualified Data.Set           as Set

import           Network.MQTT.Topic (Topic)

import           BridgeConf

destTopics :: [Dest] -> [Topic]
destTopics = Set.toList . Set.fromList . map (\(Dest t _ _) -> t)

validateConfig :: Monad m => BridgeConf -> m ()
validateConfig (BridgeConf conns sinks) = do
  let namel = map (\(Conn s _ _) -> s) conns
      dups = foldr findDup (Right mempty) namel
      names = Set.fromList namel
      sinknames = Set.fromList $ map (\(Sink s _) -> s) sinks
      unknownsinks = sinknames `Set.difference` names
      destnames = Set.fromList . concatMap (\(Sink _ dests) -> map (\(Dest _ s _) -> s) dests) $ sinks
      unknowndests = destnames `Set.difference` names

  when (isLeft dups) $ fail ("duplicate name in conns list: " <> show (fromLeft undefined dups))

  when ((not.null) unknownsinks) $ fail ("undefined server names found in sink declarations: "
                                         <> (show. Set.toList) unknownsinks)

  when ((not.null) unknowndests) $ fail ("undefined server names found in destinations: "
                                         <> (show. Set.toList) unknowndests)

  pure ()

  where
    findDup :: Server -> Either Server (Set.Set Server) -> Either Server (Set.Set Server)
    findDup x s = case Set.member x <$> s of
                    Right True -> Left x
                    _          -> Set.insert x <$> s
