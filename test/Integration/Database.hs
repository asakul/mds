
module Integration.Database (
  testDatabase
) where

import           Test.Tasty
import           Test.Tasty.HUnit

import           ATrade.MDS.Database

import           ATrade.Types
import           Control.Exception
import           Data.DateTime
import qualified Data.Text           as T
import           Data.Time.Clock
import qualified Data.Vector         as V
import           System.IO.Temp

testDatabase :: TestTree
testDatabase = testGroup "Database tests" [
    testOpenClose
  , testOpenCloseTwice
  , testPutGet
  , testGetReturnsSorted ]

testOpenClose :: TestTree
testOpenClose = testCase "Open/Close" $
  withSystemTempDirectory "test" $ \fp -> do
    let dbConfig = DatabaseConfig (T.pack $ fp ++ "/test.db") T.empty T.empty T.empty
    db <- initDatabase dbConfig
    closeDatabase db

testOpenCloseTwice :: TestTree
testOpenCloseTwice = testCase "Open/Close twice" $
  withSystemTempDirectory "test" $ \fp -> do
    let dbConfig = DatabaseConfig (T.pack $ fp ++ "/test.db") T.empty T.empty T.empty
    db <- initDatabase dbConfig
    closeDatabase db
    db2 <- initDatabase dbConfig
    closeDatabase db2


bar :: UTCTime -> Price -> Price -> Price -> Price -> Integer -> Bar
bar dt o h l c v = Bar { barSecurity = "FOO",
  barTimestamp = dt,
  barOpen = o,
  barHigh = h,
  barLow = l,
  barClose = c,
  barVolume = v }

testPutGet :: TestTree
testPutGet = testCase "Put/Get" $
  withSystemTempDirectory "test" $ \fp -> do
    let dbConfig = DatabaseConfig (T.pack $ fp ++ "/test.db") T.empty T.empty T.empty
    bracket (initDatabase dbConfig) closeDatabase $ \db -> do
      putData db "FOO" interval (timeframeMinute 1) bars
      retrievedBars <- (snd . head) <$> getData db "FOO" interval (timeframeMinute 1)
      assertEqual "Retreived bars are different from saved" bars retrievedBars

  where
    interval = TimeInterval (fromGregorian 2010 1 1 12 0 0) (fromGregorian 2010 1 1 12 5 0)
    bars = V.fromList $ [
      bar (fromGregorian 2010 1 1 12 0 0) 10 11 9 10 1,
      bar (fromGregorian 2010 1 1 12 1 0) 12 15 9 10 1,
      bar (fromGregorian 2010 1 1 12 2 0) 13 15 9 12 1
      ]

testGetReturnsSorted :: TestTree
testGetReturnsSorted = testCase "Get returns sorted vector" $
  withSystemTempDirectory "test" $ \fp -> do
    let dbConfig = DatabaseConfig (T.pack $ fp ++ "/test.db") T.empty T.empty T.empty
    bracket (initDatabase dbConfig) closeDatabase $ \db -> do
      putData db "FOO" interval (timeframeMinute 1) bars
      retrievedBars <- (snd . head) <$> getData db "FOO" interval (timeframeMinute 1)
      assertEqual "Retreived bars are not sorted" sortedBars retrievedBars
  where
    interval = TimeInterval (fromGregorian 2010 1 1 12 0 0) (fromGregorian 2010 1 1 12 5 0)
    bars = V.fromList $ [
      bar (fromGregorian 2010 1 1 12 0 0) 10 11 9 10 1,
      bar (fromGregorian 2010 1 1 12 2 0) 13 15 9 12 1,
      bar (fromGregorian 2010 1 1 12 1 0) 12 15 9 10 1
      ]
    sortedBars = V.fromList $ [
      bar (fromGregorian 2010 1 1 12 0 0) 10 11 9 10 1,
      bar (fromGregorian 2010 1 1 12 1 0) 12 15 9 10 1,
      bar (fromGregorian 2010 1 1 12 2 0) 13 15 9 12 1
      ]
