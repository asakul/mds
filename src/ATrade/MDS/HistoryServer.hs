
module ATrade.MDS.HistoryServer (
  startHistoryServer,
  stopHistoryServer
) where

import System.ZMQ4
import ATrade.MDS.Database
import Control.Concurrent

data HistoryServer = HistoryServer (MVar ()) (MVar ()) ThreadId

startHistoryServer :: DatabaseInterface -> Context -> IO HistoryServer
startHistoryServer db ctx = do
  sock <- socket ctx Router
  killMv <- newEmptyMVar
  compMv <- newEmptyMVar
  tid <- forkFinally (serverThread sock killMv compMv) (cleanup sock killMv compMv)
  return $ HistoryServer killMv compMv tid
  where
    serverThread sock killMv compMv = undefined
    cleanup sock killMv compMv = undefined

stopHistoryServer :: HistoryServer -> IO ()
stopHistoryServer (HistoryServer killMv compMv tid) = putMVar killMv () >> killThread tid >> readMVar compMv
  
