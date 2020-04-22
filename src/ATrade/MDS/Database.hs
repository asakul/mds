{-# LANGUAGE OverloadedStrings #-}

module ATrade.MDS.Database (
  DatabaseConfig(..),
  MdsHandle,
  initDatabase,
  closeDatabase,
  getData,
  getDataConduit,
  putData,
  TimeInterval(..),
  Timeframe(..),
  timeframeDaily,
  timeframeHour,
  timeframeMinute
) where

import           ATrade.Types
import           Control.Monad
import           Control.Monad.IO.Class
import           Control.Monad.Loops
import           Data.Conduit
import           Data.Maybe
import qualified Data.Text              as T
import           Data.Time.Clock
import           Data.Time.Clock.POSIX
import qualified Data.Vector            as V
import           Database.HDBC
import           Database.HDBC.Sqlite3
import           System.Log.Logger


data TimeInterval = TimeInterval UTCTime UTCTime
data Timeframe = Timeframe Int

timeframeDaily :: Int -> Timeframe
timeframeDaily days = Timeframe (days * 86400)

timeframeHour :: Int -> Timeframe
timeframeHour hours = Timeframe (hours * 3600)

timeframeMinute :: Int -> Timeframe
timeframeMinute mins = Timeframe (mins * 60)

data DatabaseConfig = DatabaseConfig {
  dbPath     :: T.Text,
  dbDatabase :: T.Text,
  dbUser     :: T.Text,
  dbPassword :: T.Text
} deriving (Show, Eq)

type MdsHandle = Connection

initDatabase :: DatabaseConfig -> IO MdsHandle
initDatabase config = do
  infoM "DB" $ "Initializing DB"
  conn <- connectSqlite3 (T.unpack $ dbPath config)
  makeSchema conn
  makeIndex conn
  infoM "DB" $ "Schema updated"
  return conn
  where
    makeSchema conn = runRaw conn "CREATE TABLE IF NOT EXISTS bars (id SERIAL PRIMARY KEY, ticker TEXT, timestamp BIGINT, timeframe INTEGER, open NUMERIC(20, 10), high NUMERIC(20, 10), low NUMERIC(20, 10), close NUMERIC(20,10), volume BIGINT);"
    makeIndex conn = runRaw conn "CREATE INDEX IF NOT EXISTS idx_bars ON bars (ticker, timeframe);"

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

getDataConduit :: (MonadIO m) => MdsHandle -> TickerId -> TimeInterval -> Timeframe -> ConduitT () Bar m ()
getDataConduit db tickerId (TimeInterval start end) (Timeframe tfSec) = do
  stmt <- liftIO $ prepare db "SELECT timestamp, timeframe, open, high, low, close, volume FROM bars WHERE ticker == ? AND timeframe == ? AND timestamp >= ? AND timestamp <= ? ORDER BY timestamp ASC;"
  _ <- liftIO $ execute stmt [(toSql. T.unpack) tickerId, toSql tfSec, (toSql . utcTimeToPOSIXSeconds) start, (toSql . utcTimeToPOSIXSeconds) end]
  whileJust_ (liftIO $ fetchRow stmt) $ \row -> case barFromResult tickerId row of
    Just bar -> yield bar
    Nothing  -> return ()
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
  withTransaction db $ \db' -> do
    delStmt <- prepare db' "DELETE FROM bars WHERE timestamp >= ? AND timestamp <= ? AND ticker == ? AND timeframe == ?;"
    void $ execute delStmt [(SqlPOSIXTime . utcTimeToPOSIXSeconds) start, (SqlPOSIXTime . utcTimeToPOSIXSeconds) end, (SqlString . T.unpack) tickerId, (SqlInteger . toInteger) tfSec]
    stmt <- prepare db' $ "INSERT INTO bars (ticker, timeframe, timestamp, open, high, low, close, volume)" ++
        " values (?, ?, ?, ?, ?, ?, ?, ?); "
    executeMany stmt (map (barToSql tf) $ V.toList bars)
    commit db'
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

