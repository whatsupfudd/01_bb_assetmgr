module Options.Runtime (defaultRun, RunOptions (..), DbConfig (..)) where
-- import Data.Int (Int)

import Data.Text (Text)

import DB.Connect (DbConfig (..), defaultDbConf)
import Filing.S3 (S3Config (..), defaultS3Conf)

data RunOptions = RunOptions {
    debug :: Int
    , db :: DbConfig
    , root :: Text
    , owner :: Text
    , s3opts :: S3Config
  }
  deriving (Show)

defaultRun :: RunOptions
defaultRun =
  RunOptions {
    debug = 0
    , db = defaultDbConf
    , root = "/tmp"
    , owner = "user"
    , s3opts = defaultS3Conf
  }