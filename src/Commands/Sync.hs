{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Commands.Sync where

import Control.Exception (SomeException, catch)
import Control.Monad (when)
import Control.Monad.Cont (runContT)
import qualified Data.Aeson as Aes
import qualified Data.ByteString.Lazy as BL
import Data.Int (Int32, Int64)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as Tio
import qualified Data.Vector as V
import GHC.Generics (Generic)
import System.Directory
  ( doesFileExist
  , getFileSize
  , getTemporaryDirectory
  , removeFile
  )
import System.FilePath (takeExtension)
import System.IO (hClose, openTempFile)

import qualified DB.Connect as Db
import qualified DB.Operations as Dbo
import qualified DB.Statements as Dbs

import qualified Storage.S3 as S3
import qualified Storage.Types as S3t

import qualified Options.Runtime as Rto

import qualified BunnySync.Types as Bnt
import qualified BunnySync.Http as Bho
import qualified BunnySync.Mime as Bmm
import Options.Cli (SyncOpts(..))


data SyncStatus
  = UploadPlanned
  | Uploaded
  | SkippedPresent
  | SkippedDirectory
  | Failed
  deriving (Show, Generic)

instance Aes.ToJSON SyncStatus


data SyncItemResult = SyncItemResult
  { sourceKind     :: Text
  , sourceKey      :: Text
  , destinationKey :: Text
  , sourceSize     :: Maybe Int64
  , assetID        :: Maybe Int32
  , md5Hash        :: Maybe Text
  , contentType    :: Maybe Text
  , status         :: SyncStatus
  , message        :: Maybe Text
  , cdnUrl         :: Maybe Text
  } deriving (Show, Generic)


instance Aes.ToJSON SyncItemResult


data SyncSummary = SyncSummary
  { scanned  :: Int
  , planned  :: Int
  , uploaded :: Int
  , skipped  :: Int
  , failed   :: Int
  } deriving (Show, Generic)

instance Aes.ToJSON SyncSummary


data SyncManifest = SyncManifest
  { summary :: SyncSummary
  , results :: [SyncItemResult]
  } deriving (Show, Generic)

instance Aes.ToJSON SyncManifest


-- | Normal command entry point.
--
-- The sync command should now be DB-driven by default.  The DB asset catalogue
-- already knows which assets exist, where they are stored, and how large they
-- should be, so there is no reason to list the whole S3 bucket as the primary
-- planning step.
syncCmd :: SyncOpts -> Rto.RunOptions -> IO ()
syncCmd = dbSync


-- | DB-driven S3 -> Bunny synchronisation.
--
-- Selection path:
--
--   data.assets
--     -> locator, md5, size
--     -> S3.getFile locator
--     -> Bunny PUT destinationKey
--
-- This avoids object-store listing and gives a better manifest because every
-- item can be tied back to an asset id and md5.
dbSync :: SyncOpts -> Rto.RunOptions -> IO ()
dbSync params rtOpts = do
  when (rtOpts.debug > 0) $ do
    putStrLn $ "@[dbSync] starting, opts: " <> show params
    putStrLn $ "@[dbSync] runtime: " <> show rtOpts

  case (rtOpts.s3store, rtOpts.bunnyStore) of
    (Nothing, _) ->
      putStrLn "@[dbSync] no s3store configured."

    (_, Nothing) ->
      putStrLn "@[dbSync] no bunnyStore configured."

    (Just s3Conf, Just bunnyConf) -> do
      let
        s3Conn = S3.makeS3Conn s3Conf
      bunnyConn <- Bho.makeBunnyConn bunnyConf

      runContT (Db.startPg rtOpts.pgDbConf) $ \dbPool -> do
        let
          mbLocatorPrefix = emptyToNothing params.sourcePrefix
          maxItems = syncLimit params.limit

        eiAssets <- Dbo.fetchAssetsForSync dbPool mbLocatorPrefix maxItems

        case eiAssets of
          Left err ->
            putStrLn $ "@[dbSync] fetchAssetsForSync err: " <> err

          Right assetRows -> do
            let
              sourceAssets = map dbAssetToSourceAsset (V.toList assetRows)

            putStrLn $ "@[dbSync] syncing " <> show (length sourceAssets) <> " DB-selected asset(s) from S3 to Bunny."

            results <- mapM (syncOneSource params rtOpts s3Conn bunnyConn) sourceAssets

            let
              manifest = SyncManifest { summary = buildSummary results, results = results }

            printSummary manifest.summary
            writeManifest params manifest


-- | Legacy fallback: S3-list-driven sync.
--
-- Keep this around while Bunny/CDN testing is active, but do not use it as the
-- normal mode.  It exists for comparing DB catalogue state with bucket state or
-- for emergency repairs when the DB catalogue is known to be incomplete.
s3ListSyncCmd :: SyncOpts -> Rto.RunOptions -> IO ()
s3ListSyncCmd params rtOpts = do
  when (rtOpts.debug > 0) $ do
    putStrLn $ "@[s3ListSyncCmd] starting, opts: " <> show params
    putStrLn $ "@[s3ListSyncCmd] runtime: " <> show rtOpts

  case (rtOpts.s3store, rtOpts.bunnyStore) of
    (Nothing, _) ->
      putStrLn "@[s3ListSyncCmd] no s3store configured."

    (_, Nothing) ->
      putStrLn "@[s3ListSyncCmd] no bunnyStore configured."

    (Just s3Conf, Just bunnyConf) -> do
      let s3Conn = S3.makeS3Conn s3Conf
      bunnyConn <- Bho.makeBunnyConn bunnyConf

      eiObjects <- S3.listObjectsMeta s3Conn (emptyToNothing params.sourcePrefix)

      case eiObjects of
        Left err ->
          putStrLn $ "@[s3ListSyncCmd] S3 listObjectsMeta err: " <> err

        Right objects -> do
          let
            limitedObjects =
              case params.limit of
                Nothing -> objects
                Just limitCount -> take (fromIntegral limitCount) objects
            sourceAssets = map s3ObjectToSourceAsset limitedObjects

          putStrLn $ "@[s3ListSyncCmd] syncing " <> show (length sourceAssets)
            <> " S3-listed object(s) from S3 to Bunny."

          results <- mapM (syncOneSource params rtOpts s3Conn bunnyConn) sourceAssets

          let
            manifest = SyncManifest { summary = buildSummary results, results = results }

          printSummary manifest.summary
          writeManifest params manifest


data SourceAsset = SourceAsset
  { srcKind  :: Text
  , srcKey   :: Text
  , srcSize  :: Maybe Int64
  , srcID    :: Maybe Int32
  , srcMD5   :: Maybe Text
  , srcNameHint :: Maybe Text
  }
  deriving (Show)


dbAssetToSourceAsset :: Dbs.AssetSyncOut -> SourceAsset
dbAssetToSourceAsset (assetId, md5, size, locator, _lastMod, nameHint) =
  SourceAsset
    { srcKind = "db-asset"
    , srcKey = locator
    , srcSize = Just size
    , srcID = Just assetId
    , srcMD5 = Just md5
    , srcNameHint = nameHint
    }


s3ObjectToSourceAsset :: S3t.S3ObjectMeta -> SourceAsset
s3ObjectToSourceAsset sourceObj =
  SourceAsset
    { srcKind = "s3-object"
    , srcKey = sourceObj.key
    , srcSize = sourceObj.size
    , srcID = Nothing
    , srcMD5 = Nothing
    , srcNameHint = Nothing
    }

{-
syncOneSource :: SyncOpts -> Rto.RunOptions -> S3t.S3Conn -> Bnt.BunnyConn
        -> SourceAsset -> IO SyncItemResult
syncOneSource params rtOpts s3Conn bunnyConn source = do
  when (rtOpts.debug > 0) $ putStrLn $ "@[syncOneSource] source: " <> show source
  let
    dstPrefix = effectiveDestinationPrefix bunnyConn.configBc params.destinationPrefix
    dstKey = mkDestinationKey params.sourcePrefix dstPrefix source.srcKey
    mbCdnUrl = Bho.bunnyCdnUrl bunnyConn.configBc dstKey
    mkResult st msg = SyncItemResult {
          sourceKind = source.srcKind
        , sourceKey = source.srcKey
        , destinationKey = dstKey
        , sourceSize = source.srcSize
        , assetID = source.srcID
        , md5Hash = source.srcMD5
        , status = st
        , message = msg
        , cdnUrl = mbCdnUrl
        }

  case source.srcSize of
    Nothing -> pure $ mkResult SkippedDirectory (Just "source item has no size; treating as directory/prefix")
    Just expectedSize -> do
      eiPresent <- if params.overwrite then
          pure $ Right False
        else
          Bho.objectExistsWithSize bunnyConn dstKey (Just expectedSize)

      case eiPresent of
        Left err -> pure $ mkResult Failed (Just . T.pack $ "Bunny precheck failed: " <> err)
        Right True -> do
          when (rtOpts.debug > 0) $ Tio.putStrLn $ "@[syncCmd] skip present: " <> dstKey
          pure $ mkResult SkippedPresent Nothing

        Right False
          | params.dryRun -> do
            Tio.putStrLn $ "@[syncCmd] dry-run upload: " <> source.srcKey <> " -> " <> dstKey
            pure $ mkResult UploadPlanned Nothing
          | otherwise -> do
              eiUpload <- withSyncTempFile params.tmpDir $ \tmpPath -> do
                eiDownload <- S3.getFile s3Conn source.srcKey tmpPath

                case eiDownload of
                  Left err -> pure . Left $ "S3 download failed: " <> err
                  Right _ -> do
                    localSizeInteger <- getFileSize tmpPath

                    let
                      localSize = fromIntegral localSizeInteger :: Int64
                    if localSize /= expectedSize then
                        pure . Left $
                          "downloaded size mismatch: expected "
                            <> show expectedSize
                            <> ", got "
                            <> show localSize
                      else do
                        let
                          contentType = inferContentType source.srcKey
                        eiPut <- Bho.putFile bunnyConn dstKey (Just contentType) tmpPath

                        case eiPut of
                          Left err -> pure . Left $ "Bunny upload failed: " <> err
                          Right _ -> verifyUploadedObject params bunnyConn dstKey localSize

              case eiUpload of
                Left err -> do
                  putStrLn $ "@[syncCmd] failed " <> T.unpack source.srcKey <> ": " <> err
                  pure $ mkResult Failed (Just $ T.pack err)

                Right _ -> do
                  Tio.putStrLn $ "@[syncCmd] uploaded: "
                    <> source.srcKey <> " -> " <> dstKey
                  pure $ mkResult Uploaded Nothing
-}

syncOneSource
  :: SyncOpts
  -> Rto.RunOptions
  -> S3t.S3Conn
  -> Bnt.BunnyConn
  -> SourceAsset
  -> IO SyncItemResult
syncOneSource params rtOpts s3Conn bunnyConn source = do
  let
    dstPrefix =
      effectiveDestinationPrefix bunnyConn.configBc params.destinationPrefix

    baseDstKey =
      mkDestinationKey params.sourcePrefix dstPrefix source.srcKey

    mbGuessedMime =
      Bmm.guessAssetMime source.srcNameHint source.srcKey

    plannedDstKey =
      case mbGuessedMime of
        Nothing ->
          baseDstKey
        Just guessed ->
          Bmm.ensureExtension guessed baseDstKey

    plannedContentType =
      fmap Bmm.mimeType mbGuessedMime

    plannedCdnUrl =
      Bho.bunnyCdnUrl bunnyConn.configBc plannedDstKey

    mkResult dstKey mbCt st msg =
      SyncItemResult
        { sourceKind = source.srcKind
        , sourceKey = source.srcKey
        , destinationKey = dstKey
        , sourceSize = source.srcSize
        , assetID = source.srcID
        , md5Hash = source.srcMD5
        , contentType = mbCt
        , status = st
        , message = msg
        , cdnUrl = Bho.bunnyCdnUrl bunnyConn.configBc dstKey
        }

  case source.srcSize of
    Nothing ->
      pure $ mkResult plannedDstKey plannedContentType SkippedDirectory
        (Just "source item has no size; treating as directory/prefix")

    Just expectedSize -> do
      -- Fast path: if we already know the MIME from the DB filename hint, the
      -- final Bunny key is known before download, so we can skip present files.
      case mbGuessedMime of
        Just guessedMime -> do
          let
            finalDstKey =
              plannedDstKey

            finalCt =
              Bmm.mimeType guessedMime

          eiPresent <-
            if params.overwrite
              then pure $ Right False
              else Bho.objectExistsWithSize bunnyConn finalDstKey (Just expectedSize)

          case eiPresent of
            Left err -> pure $ mkResult finalDstKey (Just finalCt) Failed (Just . T.pack $ "Bunny precheck failed: " <> err)

            Right True -> do
              when (rtOpts.debug > 0) $
                Tio.putStrLn $ "@[syncCmd] skip present: " <> finalDstKey
              pure $ mkResult finalDstKey (Just finalCt) SkippedPresent Nothing

            Right False
              | params.dryRun -> do
                  Tio.putStrLn $ "@[syncCmd] dry-run upload: "
                    <> source.srcKey <> " -> " <> finalDstKey
                    <> " [" <> finalCt <> "]"
                  pure $ mkResult finalDstKey (Just finalCt) UploadPlanned Nothing

              | otherwise ->
                  uploadAfterDownload params rtOpts s3Conn bunnyConn source expectedSize baseDstKey (Just guessedMime)

        Nothing -> do
          -- Slow path: UUID locator has no extension and DB gave no filename.
          -- Download first, sniff file bytes, derive final CDN key and MIME,
          -- then precheck/upload.
          uploadAfterDownload params rtOpts s3Conn bunnyConn source expectedSize baseDstKey Nothing


uploadAfterDownload :: SyncOpts -> Rto.RunOptions -> S3t.S3Conn -> Bnt.BunnyConn -> SourceAsset
      -> Int64 -> Text -> Maybe Bmm.AssetMime -> IO SyncItemResult
uploadAfterDownload params rtOpts s3Conn bunnyConn source expectedSize baseDstKey mbKnownMime = do
  let
    mkResult dstKey mbCt st msg =
      SyncItemResult
        { sourceKind = source.srcKind
        , sourceKey = source.srcKey
        , destinationKey = dstKey
        , sourceSize = source.srcSize
        , assetID = source.srcID
        , md5Hash = source.srcMD5
        , contentType = mbCt
        , status = st
        , message = msg
        , cdnUrl = Bho.bunnyCdnUrl bunnyConn.configBc dstKey
        }

  eiUpload <- withSyncTempFile params.tmpDir $ \tmpPath -> do
    eiDownload <- S3.getFile s3Conn source.srcKey tmpPath

    case eiDownload of
      Left err ->
        pure . Left $ "S3 download failed: " <> err

      Right _ -> do
        localSizeInteger <- getFileSize tmpPath

        let
          localSize =
            fromIntegral localSizeInteger :: Int64

        if localSize /= expectedSize
          then
            pure . Left $
              "downloaded size mismatch: expected "
                <> show expectedSize
                <> ", got "
                <> show localSize

          else do
            detectedMime <-
              case mbKnownMime of
                Just known ->
                  pure known
                Nothing ->
                  Bmm.detectAssetMime source.srcNameHint source.srcKey tmpPath

            let
              finalContentType = Bmm.mimeType detectedMime
              finalDstKey = Bmm.ensureExtension detectedMime baseDstKey

            eiPresent <-
              if params.overwrite
                then pure $ Right False
                else Bho.objectExistsWithSize bunnyConn finalDstKey (Just localSize)

            case eiPresent of
              Left err -> pure . Left $ "Bunny precheck failed: " <> err
              Right True -> pure $ Right (finalDstKey, finalContentType, False)
              Right False
                | params.dryRun ->
                    pure $ Right (finalDstKey, finalContentType, False)

                | otherwise -> do
                    eiPut <- Bho.putFile bunnyConn finalDstKey (Just finalContentType) tmpPath

                    case eiPut of
                      Left err ->
                        pure . Left $ "Bunny upload failed: " <> err

                      Right _ -> do
                        eiVerify <- verifyUploadedObject params bunnyConn finalDstKey localSize
                        case eiVerify of
                          Left err ->
                            pure $ Left err
                          Right _ ->
                            pure $ Right (finalDstKey, finalContentType, True)

  case eiUpload of
    Left err -> do
      putStrLn $ "@[syncCmd] failed " <> T.unpack source.srcKey <> ": " <> err
      pure $ mkResult baseDstKey Nothing Failed (Just $ T.pack err)

    Right (finalDstKey, finalContentType, didUpload)
      | params.dryRun -> do
          Tio.putStrLn $ "@[syncCmd] dry-run upload: "
            <> source.srcKey <> " -> " <> finalDstKey
            <> " [" <> finalContentType <> "]"
          pure $ mkResult finalDstKey (Just finalContentType) UploadPlanned Nothing

      | not didUpload -> do
          when (rtOpts.debug > 0) $
            Tio.putStrLn $ "@[syncCmd] skip present: " <> finalDstKey
          pure $ mkResult finalDstKey (Just finalContentType) SkippedPresent Nothing

      | otherwise -> do
          Tio.putStrLn $ "@[syncCmd] uploaded: "
            <> source.srcKey <> " -> " <> finalDstKey
            <> " [" <> finalContentType <> "]"
          pure $ mkResult finalDstKey (Just finalContentType) Uploaded Nothing


verifyUploadedObject
  :: SyncOpts
  -> Bnt.BunnyConn
  -> Text
  -> Int64
  -> IO (Either String ())
verifyUploadedObject params bunnyConn dstKey expectedSize =
  if params.verify == Bnt.NoneVsm
    then pure $ Right ()
    else do
      eiVerified <- Bho.objectExistsWithSize bunnyConn dstKey (Just expectedSize)
      case eiVerified of
        Left err ->
          pure . Left $ "Bunny verify failed: " <> err

        Right False ->
          pure $ Left "Bunny verify failed: object missing or size mismatch"

        Right True ->
          pure $ Right ()


withSyncTempFile :: Maybe FilePath -> (FilePath -> IO (Either String a)) -> IO (Either String a)
withSyncTempFile mbTmpDir action = do
  tmpDir <- maybe getTemporaryDirectory pure mbTmpDir
  (tmpPath, handle) <- openTempFile tmpDir "assetmgr-sync.bin"
  hClose handle
  action tmpPath `finallyRemove` tmpPath


finallyRemove :: IO (Either String a) -> FilePath -> IO (Either String a)
finallyRemove action tmpPath = do
  result <- action `catch` (\ex -> pure . Left $ "exception: " <> show (ex :: SomeException))
  exists <- doesFileExist tmpPath
  when exists $
    removeFile tmpPath `catch` (\(_ :: SomeException) -> pure ())
  pure result


buildSummary :: [SyncItemResult] -> SyncSummary
buildSummary = foldl step (SyncSummary 0 0 0 0 0)
  where
    step :: SyncSummary -> SyncItemResult -> SyncSummary
    step acc item =
      case item.status of
        UploadPlanned ->
          acc { scanned = acc.scanned + 1, planned = acc.planned + 1 }

        Uploaded ->
          acc { scanned = acc.scanned + 1, uploaded = acc.uploaded + 1 }

        SkippedPresent ->
          acc { scanned = acc.scanned + 1, skipped = acc.skipped + 1 }

        SkippedDirectory ->
          acc { scanned = acc.scanned + 1, skipped = acc.skipped + 1 }

        Failed ->
          acc { scanned = acc.scanned + 1, failed = acc.failed + 1 }


printSummary :: SyncSummary -> IO ()
printSummary s = do
  putStrLn "@[syncCmd] summary:"
  putStrLn $ "  scanned:  " <> show s.scanned
  putStrLn $ "  planned:  " <> show s.planned
  putStrLn $ "  uploaded: " <> show s.uploaded
  putStrLn $ "  skipped:  " <> show s.skipped
  putStrLn $ "  failed:   " <> show s.failed


writeManifest :: SyncOpts -> SyncManifest -> IO ()
writeManifest params manifest =
  case params.manifest of
    Nothing ->
      pure ()

    Just manifestPath -> do
      BL.writeFile manifestPath (Aes.encode manifest)
      putStrLn $ "@[syncCmd] wrote manifest: " <> manifestPath


syncLimit :: Maybe Int32 -> Int32
syncLimit Nothing = 1000

syncLimit (Just n)
  | n <= 0 = 1000
  | n > maxBound = maxBound
  | otherwise = n


emptyToNothing :: Text -> Maybe Text
emptyToNothing txt
  | T.null txt = Nothing
  | otherwise = Just txt


effectiveDestinationPrefix :: Bnt.BunnyConfig -> Text -> Text
effectiveDestinationPrefix bunnyConf cliPrefix =
  joinPathText [fromMaybe "" bunnyConf.prefix, cliPrefix]


mkDestinationKey :: Text -> Text -> Text -> Text
mkDestinationKey sourcePrefix destPrefix sourceKey =
  joinPathText [destPrefix, relativeSourceKey sourcePrefix sourceKey]


relativeSourceKey :: Text -> Text -> Text
relativeSourceKey sourcePrefix sourceKey
  | T.null sourcePrefix =
      trimLeftSlash sourceKey

  | otherwise =
      let
        prefix =
          ensureTrailingSlash $ trimSlashes sourcePrefix

        key =
          trimLeftSlash sourceKey
      in
        fromMaybe key (T.stripPrefix prefix key)


joinPathText :: [Text] -> Text
joinPathText =
  T.intercalate "/" . filter (not . T.null) . map trimSlashes


trimSlashes :: Text -> Text
trimSlashes =
  T.dropAround (== '/')


trimLeftSlash :: Text -> Text
trimLeftSlash =
  T.dropWhile (== '/')


ensureTrailingSlash :: Text -> Text
ensureTrailingSlash txt
  | T.null txt =
      txt

  | T.isSuffixOf "/" txt =
      txt

  | otherwise =
      txt <> "/"

{- Deprecated: 
inferContentType :: Text -> Text
inferContentType key =
  case T.toLower . T.pack . takeExtension . T.unpack $ key of
    ".avif" -> "image/avif"
    ".bmp"  -> "image/bmp"
    ".css"  -> "text/css"
    ".gif"  -> "image/gif"
    ".glb"  -> "model/gltf-binary"
    ".gltf" -> "model/gltf+json"
    ".htm"  -> "text/html"
    ".html" -> "text/html"
    ".jpeg" -> "image/jpeg"
    ".jpg"  -> "image/jpeg"
    ".js"   -> "application/javascript"
    ".json" -> "application/json"
    ".m4v"  -> "video/x-m4v"
    ".mp4"  -> "video/mp4"
    ".pdf"  -> "application/pdf"
    ".png"  -> "image/png"
    ".svg"  -> "image/svg+xml"
    ".txt"  -> "text/plain"
    ".wasm" -> "application/wasm"
    ".webm" -> "video/webm"
    ".webp" -> "image/webp"
    ".xml"  -> "application/xml"
    _       -> "application/octet-stream"
-}