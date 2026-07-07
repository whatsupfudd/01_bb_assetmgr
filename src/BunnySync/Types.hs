{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DeriveGeneric #-}
module BunnySync.Types where

import Data.Int (Int64)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Time.Clock (UTCTime)

import qualified Data.Aeson as Ae

import qualified Network.HTTP.Client as NC
import GHC.Generics (Generic)


data BunnyConfig = BunnyConfig
  { zoneName       :: Text
  , accessKey      :: Text
  , endpoint       :: Text
  , pullZoneUrl    :: Maybe Text
  , prefix         :: Maybe Text
  }
  deriving (Show)

defaultBunnyConf :: BunnyConfig
defaultBunnyConf =
  BunnyConfig {
    zoneName = "test"
    , accessKey = "test"
    , endpoint = "test"
    , pullZoneUrl = Nothing
    , prefix = Nothing
  }


data SyncConfig = SyncConfig
  { limit             :: Maybe Int
  , sourcePrefix      :: Maybe Text
  , destinationPrefix :: Maybe Text
  , dryRun            :: Bool
  , overwrite         :: Bool
  , verify            :: SyncVerifyMode
  , concurrency       :: Int
  }
  deriving (Show)


data SyncVerifyMode =
  NoneVsm
  | ContentHashVsm
  | HashAndTSVsm
  deriving stock (Show, Read, Eq, Ord)

parseSyncVerifyMode :: String -> Maybe SyncVerifyMode
parseSyncVerifyMode "none" = Just NoneVsm
parseSyncVerifyMode "content-hash" = Just ContentHashVsm
parseSyncVerifyMode "hash-and-ts" = Just HashAndTSVsm
parseSyncVerifyMode _ = Nothing


data BunnyConn = BunnyConn {
    configBc :: BunnyConfig
  , connBc :: NC.Manager
  }


data BunnyObjectInfo = BunnyObjectInfo {
  name :: Text
  , size :: Int64
  , lastModified :: UTCTime
  , contentType :: Text
  , isDirectory :: Bool
  }
  deriving (Show, Generic)

instance Ae.FromJSON BunnyObjectInfo where
  parseJSON = Ae.withObject "BunnyObjectInfo" $ \o -> do
    name <- o Ae..: "ObjectName"
    size <- o Ae..: "Length"
    lastModified <- o Ae..: "LastChanged"
    contentType <- o Ae..:? "ContentType"
    isDirectory <- o Ae..: "IsDirectory"
    pure $ BunnyObjectInfo name size lastModified (fromMaybe "application/octet-stream" contentType) isDirectory


data BunnyUploadResult
data BunnyError


data SyncMode
  = AdditiveOnly
  | OverwriteChanged
  | MirrorDeleteExtra

data SyncDecision
  = SkipAlreadyPresent
  | UploadMissing
  | UploadSizeMismatch
  | UploadForced
  | FailedPrecheck Text

data SyncItem = SyncItem
  { sourceKey      :: Text
  , destinationKey :: Text
  , sourceSize     :: Maybe Int64
  , contentType    :: Text
  , decision       :: SyncDecision
  }

data SyncSummary = SyncSummary
  { scanned  :: Int
  , planned  :: Int
  , uploaded :: Int
  , skipped  :: Int
  , failed   :: Int
  , bytes    :: Int64
  }
