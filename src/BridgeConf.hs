{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}

module BridgeConf (
  parseConfFile, parseConfText, connWarnings, BridgeConf(..), Server, Sink(..), OrderedType(..), Conn(..), Dest(..), TransFun(..)
  ) where

import           Control.Applicative        (empty, (<|>))
import           Control.Monad              (when)
import           Data.Functor               (($>))
import           Data.Map.Strict            (Map)
import qualified Data.Map.Strict            as Map
import           Data.Text                  (Text, pack, unpack)
import           Data.Void                  (Void)
import           Text.Megaparsec            (Parsec, between, eof, noneOf, option, parse, some, try)
import           Text.Megaparsec.Char       (alphaNumChar, space1)
import qualified Text.Megaparsec.Char.Lexer as L
import           Text.Megaparsec.Error      (errorBundlePretty)

import           Network.MQTT.Topic

import           Network.URI

type Parser = Parsec Void Text

data BridgeConf = BridgeConf (Map Text Conn) [Sink] deriving(Show, Eq)

type Server = Text

data OrderedType = Unordered | Ordered deriving (Show, Eq)

data Conn = Conn Server OrderedType URI (Map Text Int) deriving(Show, Eq)

data Sink = Sink Server [Dest] deriving(Show, Eq)

data Dest = Dest Filter Server TransFun deriving(Show, Eq)

data TransFun = TransFun String (Text -> Maybe Topic)

-- This is meant to be enough Eq to get tests useful.
instance Eq TransFun where
  (TransFun a f1) == (TransFun b f2) = (a == b) && (f1 "test" == f2 "test")

instance Show TransFun where
  show (TransFun n _) = n

sc :: Parser ()
sc = L.space space1 (L.skipLineComment "#") empty

lexeme :: Parser a -> Parser a
lexeme = L.lexeme sc

parseBridgeConf :: Parser BridgeConf
parseBridgeConf = do
  cs <- conns mempty
  sinks <- some (parseSink cs) <* eof
  -- Each from block subscribes, but a conn's messages only go to one, so
  -- more than one for a conn would quietly drop the others' traffic.
  case [n | (n, k) <- Map.toList (Map.fromListWith (+) [(n, 1 :: Int) | Sink n _ <- sinks]), k > 1] of
    []   -> pure (BridgeConf cs sinks)
    dups -> fail ("more than one from block for " <> show dups)
  where
    conns m = maybe (pure m) (conns . (<>m)) =<< option Nothing (Just <$> lexeme (parseConn m))

word :: Parser Text
word = pack <$> some alphaNumChar

parseConn :: Map Text Conn -> Parser (Map Text Conn)
parseConn m = do
  ((n, ot, url), opts) <- itemList src stuff
  -- A session can only be resumed under the same client ID, so a
  -- persistent one needs it set (as the URI fragment).
  when (maybe False (> 0) (lookup "session-expiry-interval" opts) && null (drop 1 (uriFragment url))) $
    fail ("conn " <> show n <> " has a session expiry but no client ID (e.g. " <> show (show url <> "#" <> unpack n) <> ")")
  pure $ Map.insert n (Conn n ot url (Map.fromList opts)) m

  where
    src = do
      w <- lexeme "conn" *> lexeme word
      -- Ordered by default, since MQTT requires messages on a topic to be
      -- delivered in order [5.0 §4.6].
      ot <- option Ordered (try (lexeme "ordered" $> Ordered) <|> try (lexeme "unordered" $> Unordered))
      when (w `Map.member` m) $ fail ("duplicate conn: " <> show w)
      ustr <- some (noneOf ['\n', ' '])
      u <- maybe (fail ("invalid URI: " <> show ustr)) pure (parseURI ustr)
      pure (w, ot, u)

    stuff = do
      k <- pack <$> lexeme (some (noneOf ['\n', ' ', '=']))
      v <- lexeme "=" *> lexeme L.decimal
      case lookup k optionRanges of
        Nothing -> fail ("unknown option: " <> show k)
        Just ok -> if ok v then pure (k,v) else fail ("invalid value for " <> show k <> ": " <> show v)

-- | Things worth warning about in a conn's settings.
connWarnings :: Conn -> [Text]
connWarnings (Conn n _ _ opts) =
  [ "conn " <> n <> " uses the default maximum-packet-size of 65536; the broker drops bigger messages for it without saying so"
  | Map.notMember "maximum-packet-size" opts ]

-- | The options a conn can have, and their valid values.  These go into
-- CONNECT, where 0 isn't allowed for the maximums [5.0 §3.1.2.11].
optionRanges :: [(Text, Int -> Bool)]
optionRanges = [
  ("session-expiry-interval", between' 0 4294967295),
  ("topic-alias-maximum", between' 0 65535),
  ("maximum-packet-size", between' 1 4294967295),
  ("receive-maximum", between' 1 65535),
  ("protocol", (`elem` [3, 5]))
  ]
  where between' lo hi v = v >= lo && v <= hi

parseSink :: Map Text Conn -> Parser Sink
parseSink conns = uncurry Sink <$> itemList src stuff
    where
      src = lexeme "from" *> validConn

      validConn = do
        w <- word
        when (w `Map.notMember` conns) $ fail ("invalid conn in sync confs: " <> show w)
        pure w

      aFilter = qstr >>= maybe (fail "bad filter") pure . mkFilter . pack
      aTopic = qstr >>= maybe (fail "bad topic") pure . mkTopic . pack

      stuff = do
        t <- lexeme "sync" *> lexeme aFilter <* lexeme "->"
        w <- lexeme validConn
        tf <- option (TransFun "id" mkTopic) (try parseTF)
        pure $ Dest t w tf

      parseTF = lexeme aTopic >>= \d -> pure $ TransFun "rewrite" (const (Just d))

      qstr = between "\"" "\"" (some $ noneOf ['"'])
             <|> between "'" "'" (some $ noneOf ['\''])

itemList :: Parser a -> Parser b ->  Parser (a, [b])
itemList pa pb = L.nonIndented sc (L.indentBlock sc p)
  where
    p = pa >>= \header -> pure (L.IndentMany Nothing (return . (header, )) pb)

parseFile :: Parser a -> String -> IO a
parseFile f s = readFile s >>= (either (fail . errorBundlePretty) pure . parse f s) . pack

parseConfFile :: String -> IO BridgeConf
parseConfFile = parseFile parseBridgeConf

-- | Parse a config from text, naming it for errors.
parseConfText :: String -> Text -> Either String BridgeConf
parseConfText n = either (Left . errorBundlePretty) Right . parse parseBridgeConf n
