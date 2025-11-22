module Storage.S3 (S3Config (..)
  , makeS3Conn, defaultS3Conf, listFiles, listFilesWith
  , putFile, getFile, getFileB
) where

import Data.Text (Text, unpack, pack)
import Data.String (fromString)
import Data.Vector (Vector)
import qualified Data.Vector as Vc
import Conduit (runConduit, liftIO, (.|), sinkList, sinkFile, foldlC)
import qualified Network.Minio as Mn
import UnliftIO (throwIO, try)
import Network.HTTP.Client as NC

import Storage.Types (S3Config (..), S3Conn (..), defaultS3Conf)

makeS3Conn :: S3Config -> S3Conn
makeS3Conn conf =
  let
    creds = Mn.CredentialValue (Mn.AccessKey conf.user) (fromString $ unpack conf.passwd) Nothing
    ciHost :: Mn.ConnectInfo
    ciHost = fromString $ unpack conf.host
    connInfo = Mn.setRegion conf.region $ Mn.setCreds creds ciHost
  in
  S3Conn {
    bucketCn = conf.bucket
    , credentialsCn = creds
    , connInfoCn = connInfo
  }


-- TODO: send back upload time.
putFile :: S3Conn -> FilePath -> Text -> IO (Either String ())
putFile s3Conf filePath locator = do
  res <- Mn.runMinio s3Conf.connInfoCn $ do
      -- Make a bucket; catch bucket already exists exception if thrown.
      bErr <- try $ Mn.makeBucket s3Conf.bucketCn Nothing
      case bErr of
        Left Mn.BucketAlreadyOwnedByYou -> pure ()
        Left e -> throwIO e
        Right _ -> pure ()

      -- Upload filepath to bucket; object is derived from filepath.
      Mn.fPutObject s3Conf.bucketCn locator filePath Mn.defaultPutObjectOptions
  case res of
    Left e -> pure . Left $ "file upload failed due to " ++ show e
    Right () -> pure $ Right ()


getFile :: S3Conn -> Text -> FilePath -> IO (Either String ())
getFile s3Conf locator filePath = do
  res <- Mn.runMinio s3Conf.connInfoCn $ do
      Mn.fGetObject s3Conf.bucketCn locator filePath Mn.defaultGetObjectOptions
  case res of
    Left e -> pure . Left $ "file download failed due to " ++ show e
    Right () -> pure $ Right ()

getFileB :: S3Conn -> Text -> FilePath -> IO (Either String ())
getFileB s3Conf locator filePath = do
  res <- Mn.runMinio s3Conf.connInfoCn $ do
      rezA <- Mn.getObject s3Conf.bucketCn locator Mn.defaultGetObjectOptions
      let
        objInfo = Mn.gorObjectInfo rezA
      liftIO $ putStrLn $ "@[getFileB] size: " <> show (Mn.oiSize objInfo) <> ", modTime: " <> show (Mn.oiModTime objInfo)
      runConduit $ Mn.gorObjectStream rezA .| sinkFile filePath

  case res of
    Left e -> pure . Left $ "file download failed due to " ++ show e
    Right () -> pure $ Right ()



listFiles :: S3Conn -> Maybe Text -> IO (Either String [FilePath])
listFiles s3Conf path = do
  {- Testing:
  rezT1 <- Mn.runMinio s3Conf.connInfoCn $ do
    Mn.listBuckets
  putStrLn $ "@[listFiles] rezT1: " <> show rezT1
  --}
  -- putStrLn $ "@[listFiles] starting, path: " <> show path <> ", bucket: " <> show s3Conf.bucketCn
  rezA <- Mn.runMinio s3Conf.connInfoCn $ do
    runConduit $ Mn.listObjects s3Conf.bucketCn path True .| sinkList
  -- liftIO $ putStrLn $ show (take 5 rezA)
  -- putStrLn "@[listFiles] done..."
  case rezA of
    Left err -> pure . Left $ show err
    Right listItems -> do
      -- mapM_ print listItems
      -- putStrLn $ "@[listFiles] length: " <> show (length listItems)
      pure . Right $ map (unpack . itemToText) listItems

itemToText anItem =
  case anItem of
    Mn.ListItemPrefix p -> p
    Mn.ListItemObject o -> Mn.oiObject o


listFilesWith :: S3Conn -> Vc.Vector Text  -> IO (Either String [FilePath])
listFilesWith s3Conf paths = do
  mgr <- NC.newManager NC.defaultManagerSettings
  conn <- Mn.mkMinioConn s3Conf.connInfoCn mgr
  rezA <- mapM (\aPath -> Mn.runMinioWith conn $ do
      -- foldlC (\accum v -> accum <> v) 0
      liftIO . putStrLn $ "@[listFilesWith] will do path: " <> unpack aPath
      rezB <- runConduit $ Mn.listObjects s3Conf.bucketCn (Just aPath) True .| foldlC (\accum item -> accum <> [unpack $ itemToText item]) []
      liftIO . putStrLn $ "@[listFilesWith] done path: " <> unpack aPath
      pure rezB
    ) paths
  -- pure . Right $ foldl (\accum rez -> accum + (case rez of Left err -> 0; Right aList -> Vc.length (Vc.fromList aList))) 0 rezA
  let
    (allFound, errors) = foldl (\(items, errs) rez ->
      case rez of
        Left anErr ->
          let
            errMsg = "e: " <> show anErr
          in
          (items, if errs == "" then errMsg else errs <> ", " <> errMsg)
        Right aList -> (items <> aList, errs)
      ) ([], "") rezA
  case errors of
    "" -> pure $ Right allFound
    aVal -> pure $ Left aVal
