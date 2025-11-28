{-# LANGUAGE BangPatterns #-}
module Commands.List where

import Control.Monad.Cont (runContT)

import Data.Int (Int32)
import Data.Time (UTCTime, getCurrentTime, diffUTCTime)
import qualified Data.Map.Strict as Mp
import Data.Maybe (fromMaybe)
import Data.Text (Text, unpack, pack)
import qualified Data.Vector as Vc
import Numeric (showHex)

import Hasql.Pool (Pool, use)

import qualified DB.Connect as Db
import qualified DB.Statements as Do
import qualified Options.Runtime as Rto

import Options.Cli (ListOpts (..))
import qualified Logic.Listing as Lst
import qualified Tree.Logic as Tl

listCmd :: ListOpts -> Rto.RunOptions -> IO ()
listCmd params rtOpts = do
  runContT (Db.startPg rtOpts.pgDbConf) mainAction
  where
  mainAction dbPool = do
    eiRezA <- case params.owner of
      "" -> do
        rezC <- use dbPool Do.fetchTaxos
        case rezC of
          Left err -> pure . Left $ "@[listCmd] fetchTaxos err: " <> show err
          Right items -> pure . Right . Lst.Triplet $ items
      _ ->
        case params.taxonomy of
          "" -> Lst.listTaxosForOwner dbPool params.owner
          _ -> Lst.listTaxoPath dbPool params.owner params.taxonomy params.maxDepth
    case eiRezA of
      Left err -> putStrLn $ "@[listCmd] fetchTaxos err: " <> show err
      Right items -> Lst.showListResults items
          -- mapM_ (\(id, label, parentID, assetID, lastMod, depth) -> putStrLn $ show id <> "\t" <> show label <> "\t" <> show parentID <> "\t" <> show assetID <> "\t" <> show lastMod <> "\t" <> show depth) items
    pure ()


