module Commands.Fetch where

import Control.Monad.Cont (runContT)

import Data.Text (Text)

import Hasql.Pool (Pool, use)

import qualified Storage.S3 as S3
import qualified DB.Connect as Db
import qualified DB.Statements as Do
import qualified Options.Runtime as Rto

import Options.Cli (FetchOpts (..))

fetchCmd :: FetchOpts -> Rto.RunOptions -> IO ()
fetchCmd params rtOpts = do
  case rtOpts.s3store of
    Nothing -> putStrLn $ "@[fetchCmd] no s3store configured."
    Just s3store ->
      let
        s3Conn = S3.makeS3Conn s3store
      in 
      runContT (Db.startPg rtOpts.pgDbConf) (mainAction s3Conn)
  where
  mainAction s3Conn dbPool = do
    rezC <- use dbPool $ Do.fetchNodeInfo params.nodeID
    case rezC of
      Left err -> putStrLn $ "@[fetchCmd] fetchNodeInfo err: " <> show err
      -- (Text, Maybe Int32, Maybe UTCTime, Maybe Text, Maybe Int64, Maybe Text, Maybe UTCTime)
      Right mbItem ->
        case mbItem of
          Nothing -> putStrLn $ "@[fetchCmd] node not found."
          Just item@(label, parentID, lastMod, md5, size, mbLocator, lastModAsset) -> do
            putStrLn $ "@[fetchCmd] item: " <> show item
            case mbLocator of
              Nothing -> putStrLn $ "@[fetchCmd] locator not found."
              Just locator -> do
                rezA <- S3.getFile s3Conn locator params.outputFile
                case rezA of
                  Left err -> putStrLn $ "@[fetchCmd] getFile err: " <> show err
                  Right _ -> putStrLn "@[fetchCmd] done."
                pure ()

