{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}

module Storage.Explore where

import Control.Monad (forM_, unless, replicateM_, replicateM)
import qualified Control.Exception as Cexc
import qualified Control.Concurrent as C
import qualified Control.Concurrent.Chan as Cc

import qualified Data.ByteString.Lazy as LB
import qualified Data.ByteString as BS
import qualified Data.Char as DC
import qualified Data.IORef as Ior
import Data.Either (rights)
import Data.Int (Int64)
import Data.List (isSuffixOf, sortOn)
import qualified Data.Map.Strict as Mp
import qualified Data.Sequence as Seq
import Data.Word (Word16)
import Data.Digest.Pure.MD5 (md5)
import qualified Crypto.Hash as Ch

import qualified System.Directory.PathWalk as Wlk
import System.FilePath (joinPath, (</>))
import qualified System.IO as Sio
import qualified System.IO.Error as Serr
import System.Directory (doesPathExist)
import System.Posix.Files (getFileStatus, FileStatus (..), fileSize, fileMode, modificationTime)
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
  deriving (Show, Eq, Ord)


type RType = Seq.Seq (DirInfo, [Either String FileInfo])


loadFolderTree :: FilePath -> IO (Seq.Seq (DirInfo, [FilePath]))  -- RType
loadFolderTree rootPath = do
  -- putStrLn $ "@[loadFolderTree] starting, rootPath: " <> rootPath
  -- TODO: run in a try to catch non-existent rootPath
  let
    prefixLength = length rootPath + 1

  ref <- Ior.newIORef Seq.empty
  Wlk.pathWalk rootPath $ \dirPath _subdirs files -> do
    -- we *prepend* to keep this O(1) per directory;
    -- order is not critical for correctness.
    dirStatus <- getFileStatus dirPath
    let
      CTime mTime = modificationTime dirStatus
      fStatus = fileMode dirStatus
      dirInfo = DirInfo {
        lpathDI = dirPath
        , rootLengthDI = prefixLength
        , insetLengthDI = Nothing
        , modifTimeDI = mTime
        , permsDI = fStatus
      }
    Ior.modifyIORef' ref $ \accum -> accum Seq.|> (dirInfo, files)
  Ior.readIORef ref

  {-
  mbRez <- Cexc.try (Wlk.pathWalkAccumulate rootPath (filesAnalyser prefixLength)) :: IO (Either Serr.IOError (Seq.Seq (DirInfo, [FilePath])))  -- RType
  case mbRez of
    Left exception -> do
      if Serr.isDoesNotExistErrorType . Serr.ioeGetErrorType $ exception then do
        putStrLn $ "@[loadFolderTree] no such file or directory: " <> show exception
        pure ()
      else
        putStrLn $ "@[loadFolderTree] err: " <> show exception
      pure Seq.empty
    Right rez -> do
      -- putStrLn $ "@[loadFolderTree] rez: " <> show rez
      pure rez
  -}


resolveFsContent :: Int -> FilePath -> Seq.Seq (DirInfo, [FilePath]) -> IO (Either [String] RType)
resolveFsContent workerCount rootPath fsContent =
  let
    splitContent = foldl (\accum (dirInfo, files) -> accum <> Seq.fromList (map (dirInfo,) files)) Seq.empty fsContent
    rootLength = length rootPath + 1
  in do
  rezA <- pooledMapConcurrentlyN workerCount makeFileInfo splitContent
  let
    rezB = foldl (\accum aResult ->
      case aResult of
        Left err ->
          case accum of
            Left errs -> Left (errs <> [err])
            Right acc -> Left [err]
        Right (dirInfo, fileInfo) ->
          case accum of
            Left errs -> Left errs
            Right dirMap ->
              case Mp.lookup dirInfo dirMap of
                Nothing -> Right $ Mp.insert dirInfo [Right fileInfo] dirMap
                Just existingFiles -> Right $ Mp.insert dirInfo (Right fileInfo : existingFiles) dirMap
      ) (Right Mp.empty) rezA
  case rezB of
    Left errs -> pure $ Left errs
    Right dirMap -> do
      -- putStrLn $ "@[resolveFsContent] dirMap: " <> show dirMap
      pure . Right $ Seq.fromList (Mp.toList dirMap)


filesAnalyser :: Int -> FilePath -> [ FilePath ] -> [[Char]] -> IO (Seq.Seq (DirInfo, [FilePath]))-- RType
filesAnalyser pLen curPath dirs files = do
  -- putStrLn $ "@[filesAnalyser] starting, root: " <> root <> ", dirs: " <> show dirs <> ", files: " <> show files
  dirStatus <- getFileStatus curPath
  let
    CTime mTime = modificationTime dirStatus
    fStatus = fileMode dirStatus
    dirInfo = DirInfo {
      lpathDI = curPath
      , rootLengthDI = pLen
      , insetLengthDI = Nothing
      , modifTimeDI = mTime
      , permsDI = fStatus
    }
  -- md5Info <- mapM (hashCalc root) files
  -- putStrLn $ "@[filesAnalyser] md5Info: " <> show md5Info
  pure $ Seq.singleton (dirInfo, files)


makeFileInfo :: (DirInfo, FilePath) -> IO (Either String (DirInfo, FileInfo))
makeFileInfo (dirInfo, filePath) =
  let
    fullFilePath = dirInfo.lpathDI </> filePath
  in do
  eStatus <- Cexc.try (getFileStatus fullFilePath) :: IO (Either Cexc.SomeException FileStatus)
  case eStatus of
    Left _ -> pure . Left $ "@[makeFileInfo] no file: " <> fullFilePath
    Right fileStatus -> do
      eMd5 <- md5FileStreaming fullFilePath
      case eMd5 of
        Left err -> pure . Left $ "@[makeFileInfo] hash error: " <> err
        Right md5Val ->
          let
            fStatus = fileMode fileStatus
            COff  fSize = fileSize fileStatus
            CTime mTime = modificationTime fileStatus
            fileInfo = FileInfo { 
                lpathFI = fullFilePath
              , rootLengthFI = length dirInfo.lpathDI + 1
              , md5hFI = md5Val
              , sizeFI = fSize
              , modifTimeFI = mTime
              , permsFI = fStatus
              }
          in
          pure . Right $ (dirInfo, fileInfo)
  {-
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
  -}

-- | Streaming MD5 for a file path, returning hex digest as String.
md5FileStreaming :: FilePath -> IO (Either String String)
md5FileStreaming fp = do
  eDigest <- Cexc.try (Sio.withBinaryFile fp Sio.ReadMode hashHandleMD5)
              :: IO (Either Cexc.SomeException (Ch.Digest Ch.MD5))
  case eDigest of
    Left ex     -> pure . Left  $ "@[md5FileStreaming] " <> fp <> ": " <> show ex
    Right digest-> pure . Right $ show digest


-- | Stream MD5 over a handle using cryptonite, without loading the whole file.
hashHandleMD5 :: Sio.Handle -> IO (Ch.Digest Ch.MD5)
hashHandleMD5 h = iterHashing Ch.hashInit
  where
    chunkSize :: Int
    chunkSize = 64 * 1024  -- 64 KiB; tune as desired

    iterHashing :: Ch.Context Ch.MD5 -> IO (Ch.Digest Ch.MD5)
    iterHashing !ctx = do
      bs <- BS.hGetSome h chunkSize
      if BS.null bs
        then pure (Ch.hashFinalize ctx)
        else iterHashing (Ch.hashUpdate ctx bs)


parseTree :: RType -> (Int, Int)
parseTree =
  -- TODO: get the md5 of the files, date of last mod
  Prelude.foldl (\(folderCount, fileCount) (fPath, files) -> (folderCount + 1, fileCount + Prelude.length files)) (0, 0)


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


pooledMapConcurrentlyN :: Int -> (a -> IO b) -> Seq.Seq a -> IO (Seq.Seq b)
pooledMapConcurrentlyN workerCount action items = do
  let
    indexedJobs = Seq.fromFunction (Seq.length items) (\i -> (i, Seq.index items i))
    jobCount = length indexedJobs

  jobChan <- Cc.newChan          :: IO (Cc.Chan (Maybe (Int, a)))
  resultChan<- Cc.newChan          :: IO (Cc.Chan (Int, b))

  -- Define a worker helper injecting local variables into the worker closure.
  let
    worker = do
      mJob <- Cc.readChan jobChan
      case mJob of
        Nothing -> return ()  -- no more work, exit this worker
        Just (i, x) -> do
          r <- action x
          Cc.writeChan resultChan (i, r)
          worker

  -- Spawn workers, feed jobs, send termination signals
  replicateM_ workerCount (C.forkIO worker)
  forM_ indexedJobs (Cc.writeChan jobChan . Just)
  replicateM_ workerCount (Cc.writeChan jobChan Nothing)

  -- Collect results, return them sorted by job index.
  results <- Seq.replicateM jobCount (Cc.readChan resultChan)
  let
    sorted = Seq.sortOn fst results
  pure (fmap snd sorted)
