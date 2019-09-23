{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}

module BridgeConf (
  parseConfFile, BridgeConf(..), Server(..), Sink(..), Conn(..), Dest(..)
  ) where

import           Control.Applicative        (empty, (<|>))
import qualified Data.ByteString.Lazy       as BL
import qualified Data.ByteString.Lazy.UTF8  as BU
import           Data.Map.Strict            (Map)
import qualified Data.Map.Strict            as Map
import           Data.Text                  (Text, pack, unpack)
import           Data.Void                  (Void)
import           Text.Megaparsec            (Parsec, between, eof, noneOf,
                                             option, parse, sepBy, some, try)
import           Text.Megaparsec.Char       (alphaNumChar, space, space1)
import qualified Text.Megaparsec.Char.Lexer as L
import           Text.Megaparsec.Error      (errorBundlePretty)

import           Network.URI

type Parser = Parsec Void Text

data BridgeConf = BridgeConf [Conn] [Sink] deriving(Show)

type Server = Text

data Conn = Conn Server URI deriving(Show)

data Sink = Sink Server [Dest] deriving(Show)

data Dest = Dest Text Server deriving(Show)

parseBridgeConf :: Parser BridgeConf
parseBridgeConf = do
  conns <- some (parseConn <* space)
  sinks <- some parseSink <* eof
  pure $ BridgeConf conns sinks

-- Eat whitspace around a parser.
spacey :: Parser a -> Parser a
spacey f = space *> f <* space

word :: Parser Text
word = pack <$> some alphaNumChar

parseConn :: Parser Conn
parseConn = do
  n <- "conn" *> space *> word
  ustr <- space *>  some (noneOf ['\n', ' '])
  let (Just u) = parseURI ustr
  pure $ Conn n u

parseSink :: Parser Sink
parseSink = do
  (n, ss) <- itemList src stuff
  pure $ Sink n ss

    where
      src = do
        _ <- "from" *> space
        w <- word
        pure w

      stuff = do
        t <- "sync" *> space *> qstr
        _ <- space <* "->" <* space
        d <- word
        pure $ Dest (pack t) d

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
parseFile f s = do
  c <- pack <$> readFile s
  case parse f s c of
    (Left x)  -> fail (errorBundlePretty x)
    (Right v) -> pure v

parseConfFile :: String -> IO BridgeConf
parseConfFile = parseFile parseBridgeConf
