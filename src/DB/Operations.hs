{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use second" #-}
module DB.Operations where

import Data.Bits ((.&.))
import qualified Data.Foldable as Fld
import Data.Int (Int32, Int64)
import qualified Data.Map.Strict as Mp
import qualified Data.Sequence as Seq
import Data.Text (Text, unpack, pack)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import Data.Word (Word16)

import qualified System.Posix.Files as Pf
import qualified System.Posix.Types as Pt

import Hasql.Pool (Pool, use, UsageError)
import qualified Hasql.Transaction as Tx
import qualified Hasql.Transaction.Sessions as Txs

import qualified DB.Statements as St
import qualified Storage.Explore as Xpl
import Data.Time (UTCTime)
import Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds)

useTx :: Pool -> Tx.Transaction tr -> IO (Either UsageError tr)
useTx pool stmts = use pool (Txs.transaction Txs.Serializable Txs.Write stmts)


getTaxonomy :: Pool -> Text -> Text -> IO (Either String (Int32, Bool))
getTaxonomy dbPool owner label = do
  rezA <- use dbPool $ St.fetchTaxoByLabel (owner, label)
  -- DBG: putStrLn $ "@[getTaxonomy] o: " <> show owner <> ", l: " <> show label <> " => " <> show rezA
  case rezA of
    Left err -> pure . Left $ "@[getTaxonomy] fetchTaxoByLabel err: " <> show err <> "."
    Right mbTaxoID ->
      case mbTaxoID of
        Just (taxoID, ownerID) -> pure $ Right (taxoID, False)
        Nothing -> do
          rezC <- use dbPool $ St.fetchOwnerByName owner
          case rezC of
            Left err -> pure . Left $ "@[getTaxonomy] fetchOwnerByName err: " <> show err <> "."
            Right mbOwnerID ->
              case mbOwnerID of
                Nothing -> pure . Left $ "@[getTaxonomy] can't find owner : " <> unpack owner <> "."
                Just ownerID -> do
                  rezB <- use dbPool $ St.addTaxonomy (ownerID, label)
                  case rezB of
                    Left err -> pure . Left $ "@[getTaxonomy] addTaxonomy err: " <> show err <> "."
                    Right taxoID -> pure $ Right (taxoID, True)


fetchAssetsByMD5 :: Pool -> V.Vector Text -> IO (Either String (Mp.Map Text Int32))
fetchAssetsByMD5 dbPool md5Hashes = do
  rezB <- use dbPool $ St.fetchAssetsByMD5 md5Hashes
  case rezB of
    Left err -> pure . Left $ "@[fetchAssetsByMD5] err: " <> show err <> "."
    Right items ->
      -- For each md5 in the input, lookup its id in the items returned from the DB
      -- First, shortcut if all md5s are found in the DB:
      let
        idMap = foldl (\accum (md5, id) -> Mp.insert md5 id accum) Mp.empty items
      in
      pure $ Right idMap


fetchNodesForTaxo :: Pool -> Int32 -> IO (Either String (V.Vector St.NodeOut))
fetchNodesForTaxo dbPool taxoID = do
  rezB <- use dbPool $ St.fetchNodesForTaxo taxoID
  case rezB of
    Left err -> pure . Left $ "@[fetchNodesForTaxo] err: " <> show err <> "."
    Right nodes ->
      pure $ Right nodes


addPathToTaxo :: Pool -> Int32 -> Maybe Int32 -> Xpl.DirInfo -> FilePath -> IO (Either String (St.NewNodeIn, St.NewNodeOut))
addPathToTaxo dbPool taxoID mbParentID dirInfo lastPath =
  let
    rights = pack $ modeToOctalPerms dirInfo.permsDI
    newNodeIn = (pack lastPath, mbParentID, Nothing, dirInfo.modifTimeDI, rights, taxoID)
  in do
  rezB <- use dbPool $ St.insertNode newNodeIn
  case rezB of
    Left err -> pure . Left $ "@[addPathsToTaxo] err: " <> show err <> "."
    Right aNodeOut -> pure $ Right (newNodeIn, aNodeOut)


addVirtualDirToTaxo :: Pool -> Int32 -> Maybe Int32 -> (Text, UTCTime, Text) -> IO (Either String (St.NewNodeIn, St.NewNodeOut))
addVirtualDirToTaxo dbPool taxoID mbParentID (label, lastmod, rights) =
  let
    newNodeIn = (label, mbParentID, Nothing, floor $ utcTimeToPOSIXSeconds lastmod, rights, taxoID)
  in do
  rezB <- use dbPool $ St.insertNode newNodeIn
  case rezB of
    Left err -> pure . Left $ "@[addPathsToTaxo] err: " <> show err <> "."
    Right aNodeOut -> pure $ Right (newNodeIn, aNodeOut)


-- NewNodeIn = label, parentid, assetid, lastmod, rights, arboid => (Text, Maybe Int32, Maybe Int32, Int64, Text, Int32)
addFileToTaxo :: Pool -> Int32 -> Int32 -> Int32 ->Xpl.FileInfo -> IO (Either String Int32)
addFileToTaxo dbPool taxoID dirNodeID assetID fileInfo =
  let
    rights = pack $ modeToOctalPerms fileInfo.permsFI
    shortPath = T.drop fileInfo.rootLengthFI $ pack fileInfo.lpathFI
    newNode = (shortPath, Just dirNodeID, Just assetID, fileInfo.modifTimeFI, rights, taxoID)
  in do
  -- putStrLn $ "@[addFileToTaxo] newNode: " <> show newNode
  rezA <- use dbPool $ St.insertNode newNode
  case rezA of
    Left err -> pure . Left $ "@[addFileToTaxo] err: " <> show err <> "."
    Right (nodeID, _) -> pure $ Right nodeID


addAsset :: Pool -> Xpl.FileInfo -> (Text, Int64) -> IO (Either String Int32)
addAsset dbPool fileInfo (locator, size) = do
  rezA <- use dbPool $ St.insertAsset (pack fileInfo.md5hFI, size, locator)
  case rezA of
    Left err -> pure . Left $ "@[addAsset] err: " <> show err <> "."
    Right (assetID, _) -> pure $ Right assetID


-- | Convert a FileStatus (from getFileStatus) into an "rwx" owner permission string
ownerPermsFromStatus :: Pt.CMode -> String
ownerPermsFromStatus fileMode = [rChar, wChar, xChar]
  where
    rChar = if fileMode .&. Pf.ownerReadMode /= 0 then 'r' else '-'
    wChar = if fileMode .&. Pf.ownerWriteMode /= 0 then 'w' else '-'
    xChar = if fileMode .&. Pf.ownerExecuteMode /= 0 then 'x' else '-'

modeToOctalPerms :: Pt.FileMode -> String
modeToOctalPerms fileMode =
  map digit [sys, own, grp, oth]
  where
    -- system bits: setuid, setgid, sticky
    sys =
      bit Pf.setUserIDMode 4
      + bit Pf.setGroupIDMode 2
      -- + bit Pf.stickyMode 1

    -- owner bits
    own =
      bit Pf.ownerReadMode 4
      + bit Pf.ownerWriteMode 2
      + bit Pf.ownerExecuteMode 1

    -- group bits
    grp =
      bit Pf.groupReadMode 4
      + bit Pf.groupWriteMode 2
      + bit Pf.groupExecuteMode  1

    -- other bits
    oth =
      bit Pf.otherReadMode 4
      + bit Pf.otherWriteMode 2
      + bit Pf.otherExecuteMode 1

    bit :: Pt.FileMode -> Int -> Int
    bit flag val = if fileMode .&. flag /= 0 then val else 0

    digit :: Int -> Char
    digit n = toEnum (fromEnum '0' + n)
