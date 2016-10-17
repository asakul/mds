{-# LANGUAGE OverloadedStrings #-}

module ATrade.MDS.Database (
) where

import qualified Data.Configurator as C
import qualified Data.Text as T
import qualified Data.Vector as V
import ATrade.Types
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Maybe
import Control.Concurrent.MVar
import Control.Concurrent
import System.Log.Logger
import Database.HDBC
import Database.HDBC.PostgreSQL
import Control.Monad
import Control.Monad.Loops

data TimeInterval = TimeInterval UTCTime UTCTime

data Timeframe = Timeframe Int

timeframeDaily = Timeframe 86400
timeframeHour = Timeframe 3600
timeframeMinute = Timeframe 60

data DatabaseCommand = DBGet TickerId TimeInterval Timeframe | DBPut TickerId TimeInterval Timeframe (V.Vector Bar)
data DatabaseResponse = DBOk | DBData [(TimeInterval, V.Vector Bar)] | DBError T.Text
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
  compVar <- newEmptyMVar
  tid <- forkFinally (dbThread conn cmdVar respVar) (cleanup conn cmdVar respVar compVar)
  return DatabaseInterface {
    tid = tid,
    getData = doGetData cmdVar respVar,
    putData = doPutData cmdVar respVar }
  where
    mkConnectionString = undefined
    dbThread conn cmdVar respVar = forever $ do
      cmd <- readMVar cmdVar
      handleCmd conn cmd >>= putMVar respVar
      whileM_ (isJust <$> tryReadMVar respVar) yield
      takeMVar cmdVar
    cleanup conn cmdVar respVar compVar _ = disconnect conn >> putMVar compVar ()
    handleCmd conn cmd = case cmd of
      DBPut tickerId (TimeInterval start end) (Timeframe timeframeSecs) bars -> do
        delStmt <- prepare conn "DELETE FROM bars WHERE timestamp > ? AND timestamp < ? AND ticker == ? AND timeframe == ?;"
        execute delStmt [utcTimeToPosixSeconds start, utcTimeToPosixSeconds end, tickerId, timeframeSecs]
        stmt <- prepare conn "INSERT INTO bars (ticker, timeframe, timestamp, open, high, low, close, volume)" ++
          " values (?, ?, ?, ?, ?, ?, ?, ?); "
        executeMany stmt (map barToSql $ V.toList bars)
        return DBOk
      DBGet tickerId interval@(TimeInterval start end) (Timeframe timeframeSecs) -> do
        rows <- quickQuery' conn "SELECT timestamp, open, high, low, close, volume FROM bars WHERE ticker == ? AND timeframe == ? AND timestamp > ? AND timestamp < ?;" [tickerId, timeframeSecs, utcTimeToPosixSeconds start, utcTimeToPosixSeconds end]
        return $ DBData [(interval, V.fromList $ map barFromResult rows)]
     barFromResult = undefined

stopDatabase :: MVar () -> DatabaseInterface -> IO ()
stopDatabase compVar db = killThread (tid db) >> readMVar compVar

doGetData :: MVar DatabaseCommand -> MVar DatabaseResponse -> TickerId -> TimeInterval -> Timeframe -> IO [(TimeInterval, V.Vector Bar)]
doGetData cmdVar respVar tickerId timeInterval timeframe = do
  putMVar cmdVar (DBGet tickerId timeInterval timeframe)
  resp <- takeMVar respVar
  case resp of
    DBData x -> return x
    DBError err -> do
      warningM "DB.Client" $ "Error while calling getData: " ++ show err
      return []
    _ -> do
      warningM "DB.Client" "Unexpected response"
      return []

doPutData :: MVar DatabaseCommand -> MVar DatabaseResponse -> TickerId -> TimeInterval -> Timeframe -> V.Vector Bar -> IO ()
doPutData cmdVar respVar tickerId timeInterval timeframe bars = do
  putMVar cmdVar (DBPut tickerId timeInterval timeframe bars)
  resp <- takeMVar respVar
  case resp of
    DBOk -> return ()
    DBError err -> do
      warningM "DB.Client" $ "Error while calling putData: " ++ show err
      return ()
    _ -> do
      warningM "DB.Client" "Unexpected response"
      return []

