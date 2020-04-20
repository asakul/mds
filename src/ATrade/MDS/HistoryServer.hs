{-# LANGUAGE OverloadedStrings #-}

module ATrade.MDS.HistoryServer (
  HistoryServer,
  HistoryServerConfig(..),
  startHistoryServer
) where

import           ATrade.MDS.Database
import           ATrade.MDS.Protocol
import           ATrade.Types
import           Control.Concurrent
import           Control.Monad
import           Data.Aeson
import           Data.Binary.Get
import           Data.Binary.Put
import qualified Data.ByteString       as B
import qualified Data.ByteString.Lazy  as BL
import           Data.List.NonEmpty
import qualified Data.Text             as T
import           Data.Time.Clock.POSIX
import qualified Data.Vector           as V
import           Safe

import           System.Log.Logger
import           System.ZMQ4

data HistoryServer = HistoryServer ThreadId ThreadId

data HistoryServerConfig = HistoryServerConfig {
  hspQHPEndpoint :: T.Text,
  hspHAPEndpoint :: T.Text
} deriving (Show, Eq)

startHistoryServer :: HistoryServerConfig -> MdsHandle -> Context -> IO HistoryServer
startHistoryServer cfg db ctx = do
  qhp <- socket ctx Router
  bind qhp $ T.unpack $ hspQHPEndpoint cfg
  qhpTid <- forkIO $ serveQHP db qhp

  hap <- socket ctx Router
  bind hap $ T.unpack $ hspHAPEndpoint cfg
  hapTid <- forkIO $ serveHAP db hap

  return $ HistoryServer qhpTid hapTid

serveQHP :: (Sender a, Receiver a) => MdsHandle -> Socket a -> IO ()
serveQHP db sock = forever $ do
  rq <- receiveMulti sock
  let maybeCmd = (BL.fromStrict <$> rq `atMay` 2) >>= decode
  case (headMay rq, maybeCmd) of
    (Just peerId, Just cmd) -> handleCmd peerId cmd
    _                       -> return ()
  where
    handleCmd :: B.ByteString -> QHPRequest -> IO ()
    handleCmd peerId cmd = case cmd of
      rq -> do
        debugM "QHP" $ "Incoming command: " ++ show cmd
        qdata <- getData db (rqTicker rq) (TimeInterval (rqStartTime rq) (rqEndTime rq)) (Timeframe (periodSeconds $ rqPeriod rq))
        let bytes = serializeBars $ V.concat $ fmap snd qdata
        sendMulti sock $ peerId :| [B.empty, "OK", BL.toStrict bytes]
    serializeBars :: V.Vector Bar -> BL.ByteString
    serializeBars bars = runPut $ V.forM_ bars serializeBar'
    serializeBar' bar = do
      putWord64le (truncate . utcTimeToPOSIXSeconds . barTimestamp $ bar)
      putDoublele (toDouble . barOpen $ bar)
      putDoublele (toDouble . barHigh $ bar)
      putDoublele (toDouble . barLow $ bar)
      putDoublele (toDouble . barClose $ bar)
      putWord64le (fromInteger . barVolume $ bar)

serveHAP :: (Sender a, Receiver a) => MdsHandle -> Socket a -> IO ()
serveHAP db sock = forever $ do
  rq <- receiveMulti sock
  let maybeCmd = (BL.fromStrict <$> rq `atMay` 2) >>= decode
  let mbRawData = rq `atMay` 3
  case (headMay rq, maybeCmd, mbRawData) of
    (Just peerId, Just cmd, Just rawData) -> do
      let parsedData = deserializeBars (hapTicker cmd) $ BL.fromStrict rawData
      handleCmd peerId cmd parsedData
    _ -> return ()
  where
    handleCmd :: B.ByteString -> HAPRequest -> [Bar] -> IO ()
    handleCmd peerId rq bars = do
      debugM "HAP" $ "Incoming command: " ++ show rq
      putData db (hapTicker rq) (TimeInterval (hapStartTime rq) (hapEndTime rq)) (Timeframe $ hapTimeframeSec rq) (V.fromList bars)
      debugM "HAP" $ "Data updated"
      sendMulti sock $ peerId :| B.empty : ["OK"]

    deserializeBars tickerId input =
      case runGetOrFail parseBar input of
        Left _               -> []
        Right (rest, _, bar) -> bar : deserializeBars tickerId rest
      where
        parseBar = do
          rawTimestamp <- realToFrac <$> getWord64le
          baropen <- getDoublele
          barhigh <- getDoublele
          barlow <- getDoublele
          barclose <- getDoublele
          barvolume <- getWord64le
          return Bar
            {
              barSecurity = tickerId,
              barTimestamp = posixSecondsToUTCTime rawTimestamp,
              barOpen = fromDouble baropen,
              barHigh = fromDouble barhigh,
              barLow = fromDouble barlow,
              barClose = fromDouble barclose,
              barVolume = toInteger barvolume
            }
