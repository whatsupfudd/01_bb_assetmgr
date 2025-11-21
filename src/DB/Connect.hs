module DB.Connect where

import Control.Exception (bracket)
import Control.Monad.Cont (ContT (..))    -- liftIO

import Data.ByteString (ByteString)
import Data.Time.Clock (DiffTime)

import GHC.Word (Word16)

import qualified Hasql.Connection as DbConn
import           Hasql.Pool (Pool, acquire, release)

data DbConfig = DbConfig {
  port :: Word16
  , host :: ByteString
  , user :: ByteString
  , passwd :: ByteString
  , dbase :: ByteString
  , poolSize :: Int
  , acqTimeout :: DiffTime
  , poolTimeOut :: DiffTime
  , poolIdleTime :: DiffTime
}
  deriving (Show)


defaultDbConf = DbConfig {
  port = 5432
  , host = "test"
  , user = "test"
  , passwd = "test"
  , dbase = "test"
  , poolSize = 5
  , acqTimeout = 5
  , poolTimeOut = 60
  , poolIdleTime = 300
}


start :: DbConfig -> ContT r IO Pool
start dbC =
  let
    dbSettings = DbConn.settings dbC.host dbC.port dbC.user dbC.passwd dbC.dbase
    -- poolSettings = (dbC.poolSize, dbC.poolTimeOut, settings)
  in do
  -- liftIO . putStrLn $ "@[start] user: " <> show dbC.user <> " pwd: " <> show dbC.passwd <> "."
  ContT $ bracket (
      acquire dbC.poolSize dbC.acqTimeout dbC.poolTimeOut dbSettings
    ) release

