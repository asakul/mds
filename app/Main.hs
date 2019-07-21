
module Main where

import           Data.Aeson
import qualified Data.ByteString.Lazy      as BL
import qualified Data.Text                 as T

import           ATrade.MDS.Database
import           ATrade.MDS.HistoryServer

import           Control.Concurrent
import           Control.Monad

import           System.IO
import           System.Log.Formatter
import           System.Log.Handler        (setFormatter)
import           System.Log.Handler.Simple
import           System.Log.Logger
import           System.ZMQ4


data MdsConfig = MdsConfig {
  cfgDbPath      :: T.Text,
  cfgDbName      :: T.Text,
  cfgDbAccount   :: T.Text,
  cfgDbPassword  :: T.Text,
  cfgQHPEndpoint :: T.Text,
  cfgHAPEndpoint :: T.Text
} deriving (Show, Eq)

instance FromJSON MdsConfig where
  parseJSON = withObject "Cfg" $ \v ->
    MdsConfig <$>
      v .: "path" <*>
      v .: "name" <*>
      v .: "account" <*>
      v .: "password" <*>
      v .: "qhp_endpoint" <*>
      v .: "hap_endpoint"

initLogging :: IO ()
initLogging = do
  handler <- fileHandler "mds.log" DEBUG >>=
    (\x -> return $
      setFormatter x (simpleLogFormatter "$utcTime\t {$loggername} <$prio> -> $msg"))

  hSetBuffering stderr LineBuffering
  updateGlobalLogger rootLoggerName (setLevel DEBUG)
  updateGlobalLogger rootLoggerName (setHandlers [handler])

getConfig :: IO MdsConfig
getConfig = do
  rawCfg <- BL.readFile "mds.conf"
  case eitherDecode' rawCfg of
    Left err  -> error err
    Right cfg -> return cfg

main :: IO ()
main = do
  initLogging
  debugM "main" "Initializing MDS"
  cfg <- getConfig
  debugM "main" "Config OK"
  let dbConfig = DatabaseConfig { dbPath = cfgDbPath cfg,
    dbDatabase = cfgDbName cfg,
    dbUser = cfgDbAccount cfg,
    dbPassword = cfgDbPassword cfg }

  db <- initDatabase dbConfig
  debugM "main" "DB initialized"

  let hsConfig = HistoryServerConfig {
    hspQHPEndpoint = cfgQHPEndpoint cfg,
    hspHAPEndpoint = cfgHAPEndpoint cfg }

  debugM "main" "Starting history server"
  withContext $ \ctx -> do
    _ <- startHistoryServer hsConfig db ctx
    forever $ threadDelay 1000000

