
module ATrade.MDS.HistoryServer (
) where

import System.ZMQ4
import ATrade.MDS.Database
import Control.Concurrent

data HistoryServer = HistoryServer ThreadId
}

startHistoryServer :: DatabaseInterface -> Context -> IO HistoryServer
startHistoryServer db ctx = do
  
