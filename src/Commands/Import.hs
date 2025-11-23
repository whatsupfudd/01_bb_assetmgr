{-# LANGUAGE BangPatterns #-}

module Commands.Import where

import Control.Monad.Cont (runContT)
import Control.Monad (when)

import qualified Data.Map as Mp
import Data.Bits (testBit)
import Data.Int (Int32)
import qualified Data.List as DL
import Data.Text (Text, unpack, isPrefixOf)
import Data.Vector as Vc

import Hasql.Pool (Pool, use)
import qualified Hasql.Transaction as Tx
import qualified Hasql.Transaction.Sessions as Txs

import DB.Connect (startPg)
import qualified Options.Runtime as Rto
import qualified Storage.Explore as Xpl
import qualified Tree.Types as Tr
import qualified Tree.Logic as Tl
import qualified DB.Statements as Op

type ImportParams = (Text, Text)

importCmd :: ImportParams -> Rto.RunOptions -> IO ()
importCmd (taxonomy, path) rtOpts = do
  when (testBit rtOpts.debug 0) $
    putStrLn $ "@[importCmd] starting, opts: " <> show rtOpts
  runContT (startPg rtOpts.pgDbConf) mainAction
  where
  mainAction dbPool = do
    let shutdownHandler = putStrLn "@[importCmd] Terminating..."
        destDir = unpack $ if isPrefixOf "/" path then
                    path
                  else
                    rtOpts.root <> "/" <> path
    -- TODO:
    tree <- Xpl.loadFolderTree $ destDir
    -- DBG:
    parsing <- parseTree tree
    when (testBit rtOpts.debug 1) $ do
      putStrLn $ "@[importCmd] tree-def: " <> show parsing <> " folder/files."
      putStrLn $ "@[importCmd] root: " <> destDir <> ", tree: " <> show tree
    rezA <- getTaxonomy dbPool rtOpts.owner taxonomy
    case rezA of
      Left errMsg ->
        putStrLn $ "@[importCmd] " <> errMsg
      Right taxoID -> do
        rezB <- use dbPool $ Op.fetchNodesForTaxo taxoID
        case rezB of
          Left err -> putStrLn $ "@[importCmd] fetchNodesForTaxoID err: " <> show err <> "."
          Right items ->
            if testBit rtOpts.debug 2 then
              let
                (treeMap, dbgInfo) = Tl.treeFromNodes items
              -- DBG:
              in
              Tl.showTree items treeMap dbgInfo
            else
              putStrLn $ "@[showTree] # items: " <> show (Vc.length items)
    pure ()


parseTree :: Xpl.RType -> IO (Int, Int)
parseTree tree =
  -- TODO: get the md5 of the files, date of last mod
  pure $ Prelude.foldl (\(folderCount, fileCount) (fPath, files) -> (folderCount + 1, fileCount + Prelude.length files)) (0, 0) tree


getTaxonomy :: Pool -> Text -> Text -> IO (Either String Int32)
getTaxonomy dbPool owner label = do
  rezA <- use dbPool $ Op.fetchTaxoByLabel (owner, label)
  -- DBG: putStrLn $ "@[getTaxonomy] o: " <> show owner <> ", l: " <> show label <> " => " <> show rezA
  case rezA of
    Left err -> pure . Left $ "@[getTaxonomy] fetchTaxoByLabel err: " <> show err <> "."
    Right mbTaxoID ->
      case mbTaxoID of
        Just (taxoID, ownerID) -> pure $ Right taxoID
        Nothing -> do
          rezC <- use dbPool $ Op.fetchOwnerByName owner
          case rezC of
            Left err -> pure . Left $ "@[getTaxonomy] fetchOwnerByName err: " <> show err <> "."
            Right mbOwnerID ->
              case mbOwnerID of
                Nothing -> pure . Left $ "@[getTaxonomy] can't find owner : " <> unpack owner <> "."
                Just ownerID -> do
                  rezB <- use dbPool $ Op.addTaxonomy (ownerID, label)
                  case rezB of
                    Left err -> pure . Left $ "@[getTaxonomy] addTaxonomy err: " <> show err <> "."
                    Right (taxoID) -> pure $ Right taxoID
