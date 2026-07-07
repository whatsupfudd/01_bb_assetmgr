module Options.Runtime (defaultRun, RunOptions (..), PgDbConfig (..), defaultPgDbConf, BunnyConfig (..), defaultBunnyConf) where
-- import Data.Int (Int)

import Data.Text (Text)

import DB.Connect (PgDbConfig (..), defaultPgDbConf)
import Storage.Types (S3Config (..), defaultS3Conf)
import BunnySync.Types (BunnyConfig (..), defaultBunnyConf)

data RunOptions = RunOptions {
    debug :: Int
    , pgDbConf :: PgDbConfig
    , root :: Maybe FilePath
    , owner :: Text
    , s3store :: Maybe S3Config
    , bunnyStore :: Maybe BunnyConfig
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
    , bunnyStore = Nothing
  }