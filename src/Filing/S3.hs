module Filing.S3 (S3Config (..)
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


data S3Config = S3Config {
    user :: Text
    , passwd :: String
    , host :: String
    , bucket :: Text
  }
  deriving (Show)

defaultS3Conf :: S3Config
defaultS3Conf =
  S3Config {
    user = "test"
    , passwd = "test"
    , host = "http://localhost:9000"
    , bucket = "test.1"
  }


data S3Conn = S3Conn {
    bucket :: Text
    , credentials :: Mn.CredentialValue
    , connInfo :: Mn.ConnectInfo
  }


makeS3Conn :: S3Config -> S3Conn
makeS3Conn conf =
  let
    creds = Mn.CredentialValue (Mn.AccessKey conf.user) (fromString conf.passwd) Nothing
    ciHost :: Mn.ConnectInfo
    ciHost = fromString conf.host
  in
  S3Conn {
    bucket = conf.bucket
    , credentials = creds
    , connInfo = Mn.setCreds creds ciHost
  }


-- TODO: send back upload time.
putFile :: S3Conn -> FilePath -> Text -> IO (Either String ())
putFile s3Conf filePath locator = do
  res <- Mn.runMinio s3Conf.connInfo $ do
      -- Make a bucket; catch bucket already exists exception if thrown.
      bErr <- try $ Mn.makeBucket s3Conf.bucket Nothing
      case bErr of
        Left Mn.BucketAlreadyOwnedByYou -> pure ()
        Left e -> throwIO e
        Right _ -> pure ()

      -- Upload filepath to bucket; object is derived from filepath.
      Mn.fPutObject s3Conf.bucket locator filePath Mn.defaultPutObjectOptions
  case res of
    Left e -> pure . Left $ "file upload failed due to " ++ show e
    Right () -> pure $ Right ()


getFile :: S3Conn -> Text -> FilePath -> IO (Either String ())
getFile s3Conf locator filePath = do
  res <- Mn.runMinio s3Conf.connInfo $ do
      Mn.fGetObject s3Conf.bucket locator filePath Mn.defaultGetObjectOptions
  case res of
    Left e -> pure . Left $ "file download failed due to " ++ show e
    Right () -> pure $ Right ()

getFileB :: S3Conn -> Text -> FilePath -> IO (Either String ())
getFileB s3Conf locator filePath = do
  res <- Mn.runMinio s3Conf.connInfo $ do
      rezA <- Mn.getObject s3Conf.bucket locator Mn.defaultGetObjectOptions
      let
        objInfo = Mn.gorObjectInfo rezA
      liftIO $ putStrLn $ "@[getFileB] size: " <> show (Mn.oiSize objInfo) <> ", modTime: " <> show (Mn.oiModTime objInfo)
      runConduit $ Mn.gorObjectStream rezA .| sinkFile filePath

  case res of
    Left e -> pure . Left $ "file download failed due to " ++ show e
    Right () -> pure $ Right ()



listFiles :: S3Conn -> Maybe Text -> IO (Either String [FilePath])
listFiles s3Conf path = do
  -- putStrLn "@[listFiles] starting..."
  rezA <- Mn.runMinio s3Conf.connInfo $ do
    runConduit $ Mn.listObjects s3Conf.bucket path True .| sinkList
    -- liftIO $ putStrLn $ show (take 5 rezB)
  -- putStrLn "@[listFiles] done..."
  case rezA of
    Left err -> pure . Left $ show err
    Right listItems -> do
      -- mapM_ (putStrLn . show) listItems
      -- putStrLn $ "@[listFiles] length: " <> (show $ length listItems)
      pure . Right $ map (unpack . itemToText) listItems

itemToText anItem =
  case anItem of
    Mn.ListItemPrefix p -> p
    Mn.ListItemObject o -> Mn.oiObject o


listFilesWith :: S3Conn -> Vc.Vector Text  -> IO (Either String [FilePath])
listFilesWith s3Conf paths = do
  mgr <- NC.newManager NC.defaultManagerSettings
  conn <- Mn.mkMinioConn s3Conf.connInfo mgr
  rezA <- mapM (\aPath -> Mn.runMinioWith conn $ do
      -- foldlC (\accum v -> accum <> v) 0
      runConduit $ Mn.listObjects s3Conf.bucket (Just aPath) True .| foldlC (\accum item -> accum <> [unpack $ itemToText item]) []
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
