module Storage.Types where

import Data.Int (Int32, Int64)
import Data.Text (Text)
import qualified Data.Text as T
import Data.UUID (UUID)

import qualified Network.Minio as Mn


data S3Config = S3Config {
    user :: Text
    , passwd :: Text
    , host :: Text
    , region :: Text
    , bucket :: Text
  }
  deriving (Show)

defaultS3Conf :: S3Config
defaultS3Conf =
  S3Config {
    user = "test"
    , passwd = "test"
    , host = "http://localhost:3900"
    , region = "garage"
    , bucket = "test.1"
  }


data S3Conn = S3Conn {
    bucketCn :: Text
    , credentialsCn :: Mn.CredentialValue
    , connInfoCn :: Mn.ConnectInfo
  }


data Asset = Asset {
    name :: Maybe Text
    , uid :: Maybe Int32
    , eid :: UUID
    , description :: Maybe Text
    , contentType :: Text
    , size :: Int64
    , version :: Int32
    , notes :: Maybe Text
  }
  deriving (Show)

