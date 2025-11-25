{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}

module Storage.Explore where

import Control.Monad (forM_)
import qualified Control.Exception as Cexc

import qualified Data.ByteString.Lazy as LB
import qualified Data.Char as DC
import Data.Either (rights)
import Data.Int (Int64)
import Data.List (isSuffixOf)
import qualified Data.Sequence as Seq
import Data.Word (Word16)
import Data.Digest.Pure.MD5

import qualified System.Directory.PathWalk as Wlk
import System.FilePath (joinPath)
import qualified System.IO.Error as Serr
import System.Directory (doesPathExist)
import System.Posix.Files (getFileStatus, fileSize, fileMode, modificationTime)
import System.Posix.Types (COff (..), CMode (..))

import Foreign.C.Types (CTime (..))


data FileInfo = FileInfo {
    lpathFI :: !FilePath
    , rootLengthFI :: !Int
    , md5hFI :: !String
    , sizeFI :: {-# UNPACK #-} !Int64
    , modifTimeFI :: {-# UNPACK #-} !Int64
    , permsFI :: !CMode
  }
  deriving Show


data DirInfo = DirInfo {
    lpathDI :: !FilePath
    , rootLengthDI :: !Int
    , insetLengthDI :: Maybe Int
    , modifTimeDI :: {-# UNPACK #-} !Int64
    , permsDI :: !CMode
  }
  deriving Show


type RType = Seq.Seq (DirInfo, [Either String FileInfo])

loadFolderTree :: FilePath -> IO RType
loadFolderTree rootPath = do
  -- putStrLn "@[loadFolderTree] starting."
  -- TODO: run in a try to catch non-existent rootPath
  let
    prefixLength = length rootPath + 1
  mbRez <- Cexc.try (Wlk.pathWalkAccumulate rootPath (filesAnalyser prefixLength)) :: IO (Either Serr.IOError RType)
  case mbRez of
    Left exception -> do
      if Serr.isDoesNotExistErrorType . Serr.ioeGetErrorType $ exception then do
        pure ()
      else
        putStrLn $ "@[loadFolderTree] err: " <> show exception
      pure Seq.empty
    Right rez -> pure rez


filesAnalyser :: Int -> FilePath -> [ FilePath ] -> [[Char]] -> IO RType
filesAnalyser pLen root dirs files = do
  dirStatus <- getFileStatus root
  let
    CTime mTime = modificationTime dirStatus
    fStatus = fileMode dirStatus
    dirInfo = DirInfo {
      lpathDI = root
      , rootLengthDI = pLen
      , insetLengthDI = Nothing
      , modifTimeDI = mTime
      , permsDI = fStatus
    }
  md5Info <- mapM (hashCalc root) files
  pure $ Seq.singleton (dirInfo, md5Info)


hashCalc :: FilePath -> FilePath -> IO (Either String FileInfo)
hashCalc prefix filePath =
  let
    (fullFilePath, rootLength) = case prefix of
        "" -> (filePath, 0)
        _ -> (prefix <> "/" <> filePath, length prefix + 1)
  in do
  pathExist <- doesPathExist fullFilePath
  if pathExist then do
    fileContent <- LB.readFile fullFilePath
    fileStatus <- getFileStatus fullFilePath
    let
      !md5Val = show . md5 $ fileContent
      fStatus = fileMode fileStatus
      COff fSize = fileSize fileStatus
      CTime mTime = modificationTime fileStatus
    pure . Right $ FileInfo {
        lpathFI = fullFilePath, rootLengthFI = rootLength, md5hFI = md5Val, sizeFI = fSize
        , modifTimeFI = mTime, permsFI = fStatus
      }
  else
    pure . Left $ "@[hashCalc] no file: " <> filePath


parseTree :: RType -> IO (Int, Int)
parseTree tree =
  -- TODO: get the md5 of the files, date of last mod
  pure $ Prelude.foldl (\(folderCount, fileCount) (fPath, files) -> (folderCount + 1, fileCount + Prelude.length files)) (0, 0) tree


showTree :: RType -> IO ()
showTree tree = do
  putStrLn $ "@[showTree] tree:\n"
  Prelude.mapM_ showTreeItem tree


showTreeItem :: (DirInfo, [Either String FileInfo]) -> IO ()
showTreeItem (fPath, files) = do
  let
    shortPath = drop fPath.rootLengthDI fPath.lpathDI
  putStrLn $ "--- fPath: " <> shortPath
  Prelude.mapM_ showFileInfo files


showFileInfo :: Either String FileInfo -> IO ()
showFileInfo = \case
  Left err -> putStrLn $ "@[showFileInfo] err: " <> err
  Right fileInfo -> putStrLn $ "    " <> show fileInfo


treeSize :: RType -> Int
treeSize = Prelude.foldl (\acc (_, files) -> acc + Prelude.length (rights files)) 0