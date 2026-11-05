{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}

module BridgeConf (
  parseConfFile, parseConfText, connWarnings, confWarnings, filtersOverlap, filterCovers, BridgeConf(..), Server, Sink(..), OrderedType(..), Conn(..), ConnOptions(..), ordering, Dest(..), TransFun(..)
  ) where

import           Control.Applicative        (empty, (<|>))
import           Control.Monad              (when)
import           Data.Functor               (($>))
import           Data.Map.Strict            (Map)
import qualified Data.Map.Strict            as Map
import           Data.Maybe                 (fromMaybe)
import           Data.Text                  (Text, pack, unpack)
import qualified Data.Text                  as T
import           Data.Void                  (Void)
import           Text.Megaparsec            (Parsec, between, choice, eof,
                                             noneOf, option, parse, some, try)
import           Text.Megaparsec.Char       (alphaNumChar, space1, string)
import qualified Text.Megaparsec.Char.Lexer as L
import           Text.Megaparsec.Error      (errorBundlePretty)

import           Network.MQTT.Topic

import           Network.URI

type Parser = Parsec Void Text

data BridgeConf = BridgeConf (Map Text Conn) [Sink] deriving(Show, Eq)

type Server = Text

data OrderedType = Unordered | Ordered deriving (Show, Eq)

data Conn = Conn Server URI ConnOptions deriving(Show, Eq)

data ConnOptions = ConnOptions {
  coSessionExpiryInterval :: Maybe Int,
  coTopicAliasMaximum     :: Maybe Int,
  coMaximumPacketSize     :: Maybe Int,
  coReceiveMaximum        :: Maybe Int,
  coProtocol              :: Maybe Int,
  coStatPrefix            :: Maybe Topic,
  coOrdering              :: Maybe OrderedType
  } deriving (Show, Eq)

-- Later occurrences override earlier ones, as they did in the old option map.
instance Semigroup ConnOptions where
  a <> b = ConnOptions {
    coSessionExpiryInterval = coSessionExpiryInterval b <|> coSessionExpiryInterval a,
    coTopicAliasMaximum     = coTopicAliasMaximum b     <|> coTopicAliasMaximum a,
    coMaximumPacketSize     = coMaximumPacketSize b     <|> coMaximumPacketSize a,
    coReceiveMaximum        = coReceiveMaximum b        <|> coReceiveMaximum a,
    coProtocol              = coProtocol b              <|> coProtocol a,
    coStatPrefix            = coStatPrefix b            <|> coStatPrefix a,
    coOrdering              = coOrdering b              <|> coOrdering a
    }

instance Monoid ConnOptions where
  mempty = ConnOptions Nothing Nothing Nothing Nothing Nothing Nothing Nothing

ordering :: ConnOptions -> OrderedType
ordering = fromMaybe Ordered . coOrdering

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

qstr :: Parser String
qstr = between "\"" "\"" (some $ noneOf ['"'])
       <|> between "'" "'" (some $ noneOf ['\''])

parseConn :: Map Text Conn -> Parser (Map Text Conn)
parseConn m = do
  ((n, url), rawOpts) <- itemList src stuff
  let opts = mconcat rawOpts
  -- A session can only be resumed under the same client ID, so a
  -- persistent one needs it set (as the URI fragment).
  when (maybe False (> 0) (coSessionExpiryInterval opts) && null (drop 1 (uriFragment url))) $
    fail ("conn " <> show n <> " has a session expiry but no client ID (e.g. " <> show (show url <> "#" <> unpack n) <> ")")
  pure $ Map.insert n (Conn n url opts) m

  where
    src = do
      w <- lexeme "conn" *> lexeme word
      when (w `Map.member` m) $ fail ("duplicate conn: " <> show w)
      ustr <- some (noneOf ['\n', ' '])
      u <- maybe (fail ("invalid URI: " <> show ustr)) pure (parseURI ustr)
      pure (w, u)

    stuff = choice [
      numericOption "session-expiry-interval" (between' 0 4294967295) (\v -> mempty{coSessionExpiryInterval=Just v}),
      numericOption "topic-alias-maximum" (between' 0 65535) (\v -> mempty{coTopicAliasMaximum=Just v}),
      numericOption "maximum-packet-size" (between' 1 4294967295) (\v -> mempty{coMaximumPacketSize=Just v}),
      numericOption "receive-maximum" (between' 1 65535) (\v -> mempty{coReceiveMaximum=Just v}),
      numericOption "protocol" (`elem` [3, 5]) (\v -> mempty{coProtocol=Just v}),
      statPrefixOption,
      orderTypeOption
      ]

numericOption :: Text -> (Int -> Bool) -> (Int -> ConnOptions) -> Parser ConnOptions
numericOption n ok f = do
  _ <- lexeme (string n) <* lexeme "="
  v <- lexeme L.decimal
  if ok v then pure (f v) else fail ("invalid value for " <> show n <> ": " <> show v)

statPrefixOption :: Parser ConnOptions
statPrefixOption = do
  _ <- lexeme "stat-prefix" <* lexeme "="
  pstr <- lexeme qstr
  p <- maybe (fail ("invalid stat-prefix: " <> show pstr)) pure (mkTopic (pack pstr))
  pure mempty{coStatPrefix=Just p}

orderTypeOption :: Parser ConnOptions
orderTypeOption = do
  _ <- lexeme "ordering" <* lexeme "="
  ot <- try (lexeme "ordered" $> Ordered) <|> try (lexeme "unordered" $> Unordered)
  pure mempty{coOrdering=Just ot}

between' :: Int -> Int -> Int -> Bool
between' lo hi v = v >= lo && v <= hi

-- | Things worth warning about in a config.
confWarnings :: BridgeConf -> [Text]
confWarnings (BridgeConf conns sinks) = concatMap connWarnings (Map.elems conns) <> echoes <> unretained
  where
    is311 n = maybe False (\(Conn _ _ o) -> coProtocol o == Just 3) (Map.lookup n conns)
    filtersAt n = [f | Sink s ds <- sinks, s == n, Dest f _ _ <- ds]
    -- 3.1.1 has no No Local, so a 3.1.1 conn sends the bridge back what
    -- it forwards there, if it's also syncing an overlapping filter
    -- (forever, if the other end is 3.1.1 too).
    echoes = [ "messages forwarded from " <> s <> " to " <> d <> " on " <> unFilter f
               <> " come back, since 3.1.1 has no No Local"
             | Sink s ds <- sinks, Dest f0 d tf <- ds, is311 d,
               let f = forwarded f0 tf, any (filtersOverlap f) (filtersAt d) ]
    -- ...and no Retain As Published, so live messages arrive unretained.
    unretained = [ "messages from " <> s <> " lose their retain flag, since 3.1.1 has no Retain As Published"
                 | Sink s _ <- sinks, is311 s ]
    forwarded f (TransFun "rewrite" fn) = maybe f toFilter (fn "")
    forwarded f _                       = f

-- | Whether some topic matches both filters.
filtersOverlap :: Filter -> Filter -> Bool
filtersOverlap a b = case (levels a, levels b) of
  -- A wildcard at the start doesn't match a $ topic [MQTT-4.7.2-1].
  (x:_, y:_) | dollarVsWild x y || dollarVsWild y x -> False
  (xs, ys)                                          -> go xs ys
  where
    levels = T.splitOn "/" . unFilter
    dollarVsWild x y = "$" `T.isPrefixOf` x && y `elem` ["+", "#"]
    go ("#":_) _          = True
    go _ ("#":_)          = True
    go [] []              = True
    go (x:xs) (y:ys)
      | x == "+" || y == "+" || x == y = go xs ys
    go _ _                = False

-- | Whether every topic the second filter matches, the first matches too.
filterCovers :: Filter -> Filter -> Bool
filterCovers a b = case (levels a, levels b) of
  -- A wildcard at the start doesn't match a $ topic [MQTT-4.7.2-1].
  (x:_, y:_) | x `elem` ["+", "#"] && "$" `T.isPrefixOf` y -> False
  (xs, ys)                                                 -> go xs ys
  where
    levels = T.splitOn "/" . unFilter
    go ["#"] _            = True
    go [] []              = True
    go (x:xs) (y:ys)
      | x == y || (x == "+" && y /= "#") = go xs ys
    go _ _                = False

-- | Things worth warning about in a conn's settings.
connWarnings :: Conn -> [Text]
connWarnings (Conn n _ opts) =
  [ "conn " <> n <> " uses the default maximum-packet-size of 65536; the broker drops bigger messages for it without saying so"
  | coMaximumPacketSize opts == Nothing ]

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
