{-# LANGUAGE BangPatterns #-}

module Storage.Explore where

import Control.Monad (forM_)
import qualified Control.Exception as Cexc
import qualified Data.ByteString.Lazy as LB
import qualified Data.Char as DC
import Data.Int (Int64)
import Data.List (isSuffixOf)
import Data.Word (Word16)
import Data.Digest.Pure.MD5

-- import Data.List.Split (splitOn, splitOneOf)
import qualified Data.Vector as Vc
import qualified Data.Sequence as Seq
import qualified System.Directory.PathWalk as Wlk
import System.FilePath (joinPath)
import qualified System.IO.Error as Serr
import System.Directory (doesPathExist)
import System.Posix.Files (getFileStatus, fileSize, fileMode, modificationTime)
import System.Posix.Types (COff (..), CMode (..))
import Foreign.C.Types (CTime (..))

data FileInfo = FileInfo {
    path :: !FilePath
    , md5h :: !String
    , size :: {-# UNPACK #-} !Int64
    , modifTime :: {-# UNPACK #-} !Int64
    , perms :: !Word16
  }
  deriving Show

type RType = Seq.Seq (FilePath, [Either String FileInfo])

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
  md5Info <- mapM (hashCalc root) files
  pure $ Seq.singleton (drop pLen root, md5Info)


hashCalc :: FilePath -> FilePath -> IO (Either String FileInfo)
hashCalc prefix filePath =
  let
    fullFilePath = case prefix of
        "" -> filePath
        _ -> prefix <> "/" <> filePath
  in do
  pathExist <- doesPathExist fullFilePath
  if pathExist then do
    fileContent <- LB.readFile fullFilePath
    fileStatus <- getFileStatus fullFilePath
    let
      !md5Val = show . md5 $ fileContent
      CMode fStatus = fileMode fileStatus
      COff fSize = fileSize fileStatus
      CTime mTime = modificationTime fileStatus
    pure . Right $ FileInfo {
        path = filePath, md5h = md5Val, size = fSize
        , modifTime = mTime, perms = fStatus
      }
  else
    pure . Left $ "@[hashCalc] no file: " <> filePath
