{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}

module BridgeConf (
  parseConfFile, BridgeConf(..), Server, Sink(..), Conn(..), Dest(..), TransFun(..)
  ) where

import           Control.Applicative        (empty, (<|>))
import           Data.Map.Strict            (Map)
import qualified Data.Map.Strict            as Map
import           Data.Text                  (Text, pack)
import           Data.Void                  (Void)
import           Text.Megaparsec            (Parsec, between, eof, noneOf,
                                             option, parse, some, try)
import           Text.Megaparsec.Char       (alphaNumChar, space, space1)
import qualified Text.Megaparsec.Char.Lexer as L
import           Text.Megaparsec.Error      (errorBundlePretty)

import           Network.URI

type Parser = Parsec Void Text

data BridgeConf = BridgeConf [Conn] [Sink] deriving(Show, Eq)

type Server = Text

data Conn = Conn Server URI (Map Text Int) deriving(Show, Eq)

data Sink = Sink Server [Dest] deriving(Show, Eq)

data Dest = Dest Text Server TransFun deriving(Show, Eq)

data TransFun = TransFun String (Text -> Text)

-- This is meant to be enough Eq to get tests useful.
instance Eq TransFun where
  (TransFun a f1) == (TransFun b f2) = (a == b) && (f1 "test" == f2 "test")

instance Show TransFun where
  show (TransFun n _) = n

parseBridgeConf :: Parser BridgeConf
parseBridgeConf = do
  conns <- some (parseConn <* space)
  sinks <- some parseSink <* eof
  pure $ BridgeConf conns sinks

word :: Parser Text
word = pack <$> some alphaNumChar

parseConn :: Parser Conn
parseConn = do
  ((n, url), opts) <- itemList src stuff
  pure $ Conn n url (Map.fromList opts)

  where
    src = do
      _ <- "conn" *> space
      w <- word
      ustr <- space *>  some (noneOf ['\n', ' '])
      let (Just u) = parseURI ustr
      pure (w, u)

    stuff = do
      k <- pack <$> (space *> some (noneOf ['\n', ' ', '=']))
      v <- space *> "=" *> space *> L.decimal
      pure (k,v)

parseSink :: Parser Sink
parseSink = do
  (n, ss) <- itemList src stuff
  pure $ Sink n ss

    where
      src = "from" *> space *> word

      stuff = do
        t <- "sync" *> space *> qstr
        _ <- space <* "->" <* space
        w <- word
        tf <- option (TransFun "id" id) (try parseTF)
        pure $ Dest (pack t) w tf

      parseTF = do
        d <- space *> qstr
        pure $ TransFun "rewrite" ((const . pack) d)

      qstr = between "\"" "\"" (some $ noneOf ['"'])
             <|> between "'" "'" (some $ noneOf ['\''])

lineComment :: Parser ()
lineComment = L.skipLineComment "#"

itemList :: Parser a -> Parser b ->  Parser (a, [b])
itemList pa pb = L.nonIndented scn (L.indentBlock scn p)
  where
    scn = L.space space1 lineComment empty
    p = do
      header <- pa
      return (L.IndentMany Nothing (return . (header, )) pb)

parseFile :: Parser a -> String -> IO a
parseFile f s = pack <$> readFile s >>= either (fail.errorBundlePretty) pure . parse f s

parseConfFile :: String -> IO BridgeConf
parseConfFile = parseFile parseBridgeConf
