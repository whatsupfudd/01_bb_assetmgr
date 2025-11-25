module Options.Runtime (defaultRun, RunOptions (..), PgDbConfig (..), defaultPgDbConf) where
-- import Data.Int (Int)

import Data.Text (Text)

import DB.Connect (PgDbConfig (..), defaultPgDbConf)
import Storage.Types (S3Config (..), defaultS3Conf)

data RunOptions = RunOptions {
    debug :: Int
    , pgDbConf :: PgDbConfig
    , root :: Maybe FilePath
    , owner :: Text
    , s3store :: Maybe S3Config
  }
  deriving (Show)

defaultRun :: RunOptions
defaultRun =
  RunOptions {
    debug = 0
    , pgDbConf = defaultPgDbConf
    , root = Nothing
    , owner = "user"
    , s3store = Nothing
  }