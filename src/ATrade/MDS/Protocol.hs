{-# LANGUAGE MultiWayIf #-}

module ATrade.MDS.Protocol (
  QHPRequest(..),
  HAPRequest(..),
  Period(..),
  periodSeconds
) where

-- import ATrade.Types

import Data.Aeson
import Data.Aeson.Types
import Data.Time.Clock
import qualified Data.Text as T

data Period =
  Period1Min  |
  Period5Min  |
  Period15Min |
  Period30Min |
  PeriodHour  |
  PeriodDay   |
  PeriodWeek  |
  PeriodMonth
  deriving (Eq)

instance Show Period where
  show Period1Min = "M1"
  show Period5Min = "M5"
  show Period15Min = "M15"
  show Period30Min = "M30"
  show PeriodHour = "H1"
  show PeriodDay = "D"
  show PeriodWeek = "W"
  show PeriodMonth = "MN"

periodSeconds :: Period -> Int
periodSeconds Period1Min = 60
periodSeconds Period5Min = 60 * 5
periodSeconds Period15Min = 60 * 15
periodSeconds Period30Min = 60 * 30
periodSeconds PeriodHour = 3600
periodSeconds PeriodDay = 86400
periodSeconds PeriodWeek = 86400 * 7
periodSeconds PeriodMonth = 86400 * 7 * 4

data QHPRequest =
  QHPRequest {
    rqTicker :: T.Text,
    rqStartTime :: UTCTime,
    rqEndTime :: UTCTime,
    rqPeriod :: Period
  } deriving (Show, Eq)
   
instance FromJSON QHPRequest where
  parseJSON = withObject "Request" $ \v -> QHPRequest <$>
    v .: "ticker" <*>
    v .: "from" <*>
    v .: "to" <*>
    (v .: "timeframe" >>= parseTf)
    where
      parseTf :: T.Text -> Parser Period
      parseTf t = if
        | t == "M1" -> return Period1Min
        | t == "M5" -> return Period5Min
        | t == "M15" -> return Period15Min
        | t == "M30" -> return Period30Min
        | t == "H1" -> return PeriodHour
        | t == "D" -> return PeriodDay
        | t == "W" -> return PeriodWeek
        | t == "MN" -> return PeriodMonth
        | otherwise -> fail "Invalid period specified"

data HAPRequest =
  HAPRequest {
    hapTicker :: T.Text,
    hapStartTime :: UTCTime,
    hapEndTime :: UTCTime,
    hapTimeframeSec :: Int
  } deriving (Show, Eq)

instance FromJSON HAPRequest where
  parseJSON = withObject "Request" $ \v -> HAPRequest <$>
    v .: "ticker" <*>
    v .: "start_time" <*>
    v .: "end_time" <*>
    v .: "timeframe_sec"
