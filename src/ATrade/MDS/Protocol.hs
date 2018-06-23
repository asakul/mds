{-# LANGUAGE DeriveGeneric #-}

module ATrade.MDS.Protocol (
  MDSRequest(..)
) where

import GHC.Generics

import ATrade.Types

import Data.Aeson
import Data.Time.Clock

data MDSRequest = RequestData {
  rqTicker :: TickerId,
  rqFrom :: UTCTime,
  rqTo :: UTCTime,
  rqTimeframe :: Int
} deriving (Generic, Show, Eq)

instance ToJSON MDSRequest
instance FromJSON MDSRequest

