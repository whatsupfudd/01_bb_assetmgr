module DB.Operations where

import Hasql.Pool (Pool, use, UsageError)
import qualified Hasql.Transaction as Tx
import qualified Hasql.Transaction.Sessions as Txs


useTx :: Pool -> Tx.Transaction tr -> IO (Either UsageError tr)
useTx pool stmts = use pool (Txs.transaction Txs.Serializable Txs.Write stmts)
