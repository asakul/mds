{-# LANGUAGE OverloadedStrings #-}

module ATrade.MDS.Database (
  DatabaseConfig(..),
  MdsHandle,
  initDatabase,
  closeDatabase,
  getData,
  putData,
  TimeInterval(..),
  Timeframe(..),
  timeframeDaily,
  timeframeHour,
  timeframeMinute
) where

import qualified Data.Text as T
import qualified Data.Vector as V
import ATrade.Types
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Data.Maybe
import Database.HDBC
import Database.HDBC.Sqlite3
import Control.Monad


data TimeInterval = TimeInterval UTCTime UTCTime

data Timeframe = Timeframe Int

timeframeDaily :: Int -> Timeframe
timeframeDaily days = Timeframe (days * 86400)

timeframeHour :: Int -> Timeframe
timeframeHour hours = Timeframe (hours * 3600)

timeframeMinute :: Int -> Timeframe
timeframeMinute mins = Timeframe (mins * 60)

data DatabaseConfig = DatabaseConfig {
  dbPath :: T.Text,
  dbDatabase :: T.Text,
  dbUser :: T.Text,
  dbPassword :: T.Text
} deriving (Show, Eq)

type MdsHandle = Connection

initDatabase :: DatabaseConfig -> IO MdsHandle
initDatabase config = do
  conn <- connectSqlite3 (T.unpack $ dbPath config)
  makeSchema conn
  return conn
  where
    makeSchema conn = runRaw conn "CREATE TABLE IF NOT EXISTS bars (id SERIAL PRIMARY KEY, ticker TEXT, timestamp BIGINT, timeframe INTEGER, open NUMERIC(20, 10), high NUMERIC(20, 10), low NUMERIC(20, 10), close NUMERIC(20,10), volume BIGINT);"

closeDatabase :: MdsHandle -> IO ()
closeDatabase = disconnect

getData :: MdsHandle -> TickerId -> TimeInterval -> Timeframe -> IO [(TimeInterval, V.Vector Bar)]
getData db tickerId interval@(TimeInterval start end) (Timeframe tfSec) = do
  rows <- quickQuery' db "SELECT timestamp, timeframe, open, high, low, close, volume FROM bars WHERE ticker == ? AND timeframe == ? AND timestamp >= ? AND timestamp <= ? ORDER BY timestamp ASC;" [(toSql. T.unpack) tickerId, toSql tfSec, (toSql . utcTimeToPOSIXSeconds) start, (toSql . utcTimeToPOSIXSeconds) end]
  return [(interval, V.fromList $ mapMaybe (barFromResult tickerId) rows)]
  where
    barFromResult ticker [ts, _, open, high, low, close, vol] = Just Bar {
      barSecurity = ticker,
      barTimestamp = fromSql ts,
      barOpen = fromDouble $ fromSql open,
      barHigh = fromDouble $ fromSql high,
      barLow = fromDouble $ fromSql low,
      barClose = fromDouble $ fromSql close,
      barVolume = fromSql vol
    }
    barFromResult _ _ = Nothing

putData :: MdsHandle -> TickerId -> TimeInterval -> Timeframe -> V.Vector Bar -> IO ()
putData db tickerId (TimeInterval start end) tf@(Timeframe tfSec) bars = do
  delStmt <- prepare db "DELETE FROM bars WHERE timestamp >= ? AND timestamp <= ? AND ticker == ? AND timeframe == ?;"
  void $ execute delStmt [(SqlPOSIXTime . utcTimeToPOSIXSeconds) start, (SqlPOSIXTime . utcTimeToPOSIXSeconds) end, (SqlString . T.unpack) tickerId, (SqlInteger . toInteger) tfSec]
  stmt <- prepare db $ "INSERT INTO bars (ticker, timeframe, timestamp, open, high, low, close, volume)" ++
    " values (?, ?, ?, ?, ?, ?, ?, ?); "
  executeMany stmt (map (barToSql tf) $ V.toList bars)
  runRaw db "COMMIT;"
  where
    barToSql :: Timeframe -> Bar -> [SqlValue] 
    barToSql (Timeframe timeframeSecs) bar = [(SqlString . T.unpack . barSecurity) bar,
      (SqlInteger . toInteger) timeframeSecs,
      (SqlPOSIXTime . utcTimeToPOSIXSeconds . barTimestamp) bar,
      (SqlDouble . toDouble . barOpen) bar,
      (SqlDouble . toDouble . barHigh) bar,
      (SqlDouble . toDouble . barLow) bar,
      (SqlDouble . toDouble . barClose) bar,
      (SqlInteger . barVolume) bar ]

