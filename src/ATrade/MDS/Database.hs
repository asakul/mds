{-# LANGUAGE OverloadedStrings #-}

module ATrade.MDS.Database (
) where

import qualified Data.Configurator as C
import qualified Data.Text as T
import qualified Data.Vector as V
import ATrade.Types
import Data.Time.Clock
import Control.Concurrent.MVar
import Control.Concurrent
import System.Log.Logger
import Database.HDBC
import Database.HDBC.PostgreSQL

data TimeInterval = TimeInterval UTCTime UTCTime

data Timeframe = Timeframe Int

timeframeDaily = Timeframe 86400
timeframeHour = Timeframe 3600
timeframeMinute = Timeframe 60

data DatabaseCommand = DBGet TickerId TimeInterval Timeframe | DBPut TickerId TimeInterval Timeframe (V.Vector Bar)
data DatabaseResponse = DBData [(TimeInterval, V.Vector Bar)] | DBError T.Text

data DatabaseConfig = DatabaseConfig {
  dbHost :: T.Text,
  dbDatabase :: T.Text,
  dbUser :: T.Text,
  dbPassword :: T.Text
} deriving (Show, Eq)

data DatabaseInterface = DatabaseInterface {
  tid :: ThreadId,
  getData :: TickerId -> TimeInterval -> Timeframe -> IO [(TimeInterval, V.Vector Bar)],
  putData :: TickerId -> TimeInterval -> Timeframe -> V.Vector Bar -> IO ()
}

startDatabase :: DatabaseConfig -> IO DatabaseInterface
startDatabase config = do
  conn <- connectPostgreSQL (mkConnectionString config)
  cmdVar <- newEmptyMVar
  respVar <- newEmptyMVar
  tid <- forkFinally (dbThread conn cmdVar respVar) (cleanup conn cmdVar respVar)
  return DatabaseInterface {
    tid = tid,
    getData = doGetData cmdVar respVar,
    putData = doPutData cmdVar respVar }
  where
    mkConnectionString = undefined
    dbThread = undefined
    cleanup = undefined

stopDatabase :: DatabaseInterface -> IO ()
stopDatabase db = undefined


doGetData :: MVar DatabaseCommand -> MVar DatabaseResponse -> TickerId -> TimeInterval -> Timeframe -> IO [(TimeInterval, V.Vector Bar)]
doGetData cmdVar respVar tickerId timeInterval timeframe = do
  putMVar cmdVar (DBGet tickerId timeInterval timeframe)
  resp <- takeMVar respVar
  case resp of
    DBData x -> return x
    DBError err -> do
      warningM "DB.Client" $ "Error while calling getData: " ++ show err
      return []

doPutData :: MVar DatabaseCommand -> MVar DatabaseResponse -> TickerId -> TimeInterval -> Timeframe -> V.Vector Bar -> IO ()
doPutData cmdVar respVar tickerId timeInterval timeframe bars = do
  putMVar cmdVar (DBPut tickerId timeInterval timeframe bars)
  resp <- takeMVar respVar
  case resp of
    DBData x -> return ()
    DBError err -> do
      warningM "DB.Client" $ "Error while calling putData: " ++ show err
      return ()
