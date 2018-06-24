
module Main where

import Data.Aeson
import qualified Data.Text as T
import qualified Data.ByteString.Lazy as BL

import ATrade.MDS.Database
import ATrade.MDS.HistoryServer

import Control.Concurrent
import Control.Monad

import System.ZMQ4

data MdsConfig = MdsConfig {
  cfgDbPath :: T.Text,
  cfgDbName :: T.Text,
  cfgDbAccount :: T.Text,
  cfgDbPassword :: T.Text,
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

getConfig :: IO MdsConfig
getConfig = do
  rawCfg <- BL.readFile "mds.conf"
  case eitherDecode' rawCfg of
    Left err -> error err
    Right cfg -> return cfg
    
main :: IO ()
main = do
  cfg <- getConfig
  let dbConfig = DatabaseConfig { dbPath = cfgDbPath cfg,
    dbDatabase = cfgDbName cfg,
    dbUser = cfgDbAccount cfg,
    dbPassword = cfgDbPassword cfg }

  db <- initDatabase dbConfig

  let hsConfig = HistoryServerConfig {
    hspQHPEndpoint = cfgQHPEndpoint cfg,
    hspHAPEndpoint = cfgHAPEndpoint cfg }

  withContext $ \ctx -> do
    _ <- startHistoryServer hsConfig db ctx
    forever $ threadDelay 1000000

