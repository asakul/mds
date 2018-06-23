
module ATrade.MDS.HistoryServer (
  HistoryServer,
  startHistoryServer
) where

import System.ZMQ4
import ATrade.MDS.Database
import ATrade.MDS.Protocol
import Control.Concurrent
import Control.Monad
import Data.Aeson
import Data.List.NonEmpty
import qualified Data.Vector as V
import Safe
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL

data HistoryServer = HistoryServer ThreadId

startHistoryServer :: MdsHandle -> Context -> IO HistoryServer
startHistoryServer db ctx = do
  sock <- socket ctx Router
  tid <- forkIO $ serve db sock
  return $ HistoryServer tid

serve :: (Sender a, Receiver a) => MdsHandle -> Socket a -> IO ()
serve db sock = forever $ do
  rq <- receiveMulti sock
  let maybeCmd = (BL.fromStrict <$> rq `atMay` 2) >>= decode
  case (headMay rq, maybeCmd) of
    (Just peerId, Just cmd) -> handleCmd peerId cmd
    _ -> return ()
  where
    handleCmd :: B.ByteString -> MDSRequest -> IO ()
    handleCmd peerId cmd = case cmd of
      rq -> do
        qdata <- getData db (rqTicker rq) (TimeInterval (rqFrom rq) (rqTo rq)) (Timeframe (rqTimeframe rq))
        bytes <- serializeBars $ V.concat $ fmap snd qdata
        sendMulti sock $ peerId :| B.empty : bytes
    serializeBars = undefined

