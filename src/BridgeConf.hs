{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}

module BridgeConf (
  parseConfFile, BridgeConf(..), Server, Sink(..), OrderedType(..), Conn(..), Dest(..), TransFun(..)
  ) where

import           Control.Applicative        (empty, (<|>))
import           Control.Monad              (when)
import           Data.Functor               (($>))
import           Data.Map.Strict            (Map)
import qualified Data.Map.Strict            as Map
import           Data.Text                  (Text, pack)
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
parseBridgeConf = conns mempty >>= \cs -> BridgeConf cs <$> some (parseSink cs) <* eof
  where
    conns m = maybe (pure m) (conns . (<>m)) =<< option Nothing (Just <$> lexeme (parseConn m))

word :: Parser Text
word = pack <$> some alphaNumChar

parseConn :: Map Text Conn -> Parser (Map Text Conn)
parseConn m = do
  ((n, ot, url), opts) <- itemList src stuff
  pure $ Map.insert n (Conn n ot url (Map.fromList opts)) m

  where
    src = do
      w <- lexeme "conn" *> lexeme word
      ot <- option Unordered (lexeme "ordered" $> Ordered)
      when (w `Map.member` m) $ fail ("duplicate conn: " <> show w)
      ustr <- some (noneOf ['\n', ' '])
      let (Just u) = parseURI ustr
      pure (w, ot, u)

    stuff = do
      k <- pack <$> lexeme (some (noneOf ['\n', ' ', '=']))
      v <- lexeme "=" *> lexeme L.decimal
      pure (k,v)

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
