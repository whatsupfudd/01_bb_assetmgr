{-# LANGUAGE LambdaCase #-}

module Logic.Ingestion where

import Control.Monad.Cont (runContT)
import Control.Monad (when, foldM, foldM_)
import Control.Monad.IO.Class (liftIO)
import qualified Control.Monad.Trans.Except as Ex
import qualified Control.Concurrent as C

import Data.Bifunctor (first)
import Data.Bits (testBit)
import Data.Either (lefts, rights, partitionEithers)
import qualified Data.Foldable as Fld
import Data.Int (Int32, Int64)
import qualified Data.IntMap as Mi
import qualified Data.Map.Strict as Mp
import qualified Data.List as L
import qualified Data.Sequence as Seq
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time (getCurrentTime)
import qualified Data.Vector as V

import System.Directory (getCurrentDirectory)
import System.FilePath ((</>), splitDirectories)

import Hasql.Pool (Pool, use)
import qualified Hasql.Transaction as Tx
import qualified Hasql.Transaction.Sessions as Txs

import DB.Connect (startPg)
import qualified Options.Runtime as Rto
import Options.Runtime (RunOptions (..))
import qualified Storage.Explore as Xpl
import qualified Tree.Types as Tr
import qualified Tree.Logic as Tl
import qualified DB.Statements as St
import qualified DB.Operations as Do
import qualified Storage.Types as S3
import qualified Storage.S3 as Ss
import Storage.S3 (makeS3Conn)


loadFileTree :: Pool -> S3.S3Conn -> Rto.RunOptions -> Text -> Maybe FilePath -> Xpl.RType -> IO ()
loadFileTree dbPool s3Conn rtOpts taxonomy mbAnchorPath fsTree = do
    rezA <- Do.getTaxonomy dbPool rtOpts.owner taxonomy
    case rezA of
      Left errMsg ->
        putStrLn $ "@[importCmd] " <> errMsg
      Right (taxoID, newFlag) ->
        let
          assetBlocks = Seq.chunksOf 256 fsTree
        in do
        eiSplitAssets <- mapM (md5Existence dbPool) assetBlocks
        case partitionEithers (Fld.toList eiSplitAssets) of
          ([], idMaps) ->
            let
              assetMap = Mp.unions idMaps
            in do
            putStrLn $ "@[importCmd] found " <> show (Mp.size assetMap) <> " existing assets."
            if newFlag then
              addFilesToTaxo dbPool s3Conn rtOpts taxoID assetMap fsTree
            else
              appendFilesToTaxo dbPool s3Conn rtOpts taxoID assetMap fsTree
          (errs, _) -> do
            putStrLn $ "@[importCmd] md5Existence errs: " <> show errs <> "."
    pure ()


addFilesToTaxo :: Pool -> S3.S3Conn -> Rto.RunOptions -> Int32 -> Mp.Map Text Int32 -> Xpl.RType -> IO ()
addFilesToTaxo dbPool s3Conn rtOpts taxoID assetMap fsTree =
  putStrLn "@[addFilesToTaxo] starting..."


md5Existence :: Pool -> Seq.Seq (Xpl.DirInfo, [Either String Xpl.FileInfo]) -> IO (Either String (Mp.Map Text Int32))
md5Existence dbPool assetBlock = 
  let
    eiAssetList :: [Either String Text]
    eiAssetList = concatMap (\(_, eiFiles) ->
        map (\case
            Left err -> Left err
            Right fileInfo -> Right $ T.pack fileInfo.md5hFI
          ) eiFiles
      ) (Fld.toList assetBlock)
  in
  case partitionEithers eiAssetList of
    ([], assetList) -> do
      rezB <- Do.fetchAssetsByMD5 dbPool (V.fromList assetList)
      case rezB of
        Left err ->
          pure . Left $ "@[md5Existence] fetchAssetsByMD5 err: " <> show err <> "."
        Right idMap -> do
          pure $ Right idMap
    (errs, _) -> do
      pure . Left $ "@[md5Existence] file errs: " <> show errs <> "."


appendFilesToTaxo :: Pool -> S3.S3Conn -> Rto.RunOptions -> Int32 -> Mp.Map Text Int32 -> Xpl.RType -> IO ()
appendFilesToTaxo dbPool s3Conn rtOpts taxoID assetMap fsTree = do
  putStrLn "@[appendFilesToTaxo] starting..."
  eiNodes <- Do.fetchNodesForTaxo dbPool taxoID
  case eiNodes of
    Left err -> putStrLn $ "@[importCmd] fetchNodesForTaxo err: " <> show err <> "."
    Right nodes ->
      let
        treeMap = Tl.buildForestMapV nodes
        namedDirMap = convFolderMapToNamedDirMap treeMap
      in do
      -- putStrLn $ "@[appendFilesToTaxo] namedDirMap: " <> show namedDirMap
      putStrLn $ "@[appendFilesToTaxo] fsTree size: " <> show (Xpl.treeSize fsTree) <> "."
      storeRez <- storeToTaxo dbPool s3Conn rtOpts taxoID assetMap namedDirMap fsTree
      case storeRez of
        Left err -> putStrLn $ "@[appendFilesToTaxo] storeToTaxo err: " <> show err <> "."
        Right namedDirMap -> do
          putStrLn "@[appendFilesToTaxo] storeToTaxo success."


storeToTaxo :: Pool -> S3.S3Conn -> Rto.RunOptions -> Int32 -> Mp.Map Text Int32 -> NamedDirMap St.NodeOut -> Xpl.RType -> IO (Either String (NamedDirMap St.NodeOut, Mp.Map Text Int32))
storeToTaxo dbPool s3Conn rtOpts taxoID assetMap namedDirMap fsTree = do
  -- putStrLn $ "@[storeFilesToTaxo] fsTree: " <> show fsTree
  foldM (\accum aNode ->
    case accum of
      Left err -> pure $ Left err
      Right (dirMap, iterAssetMap) -> do
        rezA <- storeDirToTaxo dbPool rtOpts taxoID iterAssetMap dirMap aNode
        case rezA of
          Left err -> pure $ Left err
          Right (newNodeID, updDirMap) -> do
            rezB <- storeFilesToTaxo dbPool s3Conn rtOpts iterAssetMap taxoID newNodeID (snd aNode)
            case rezB of
              Left err -> pure $ Left err
              Right newValues ->
                let
                  newAssetMap = Mp.fromList [fst aValue | aValue <- newValues]
                in do
                putStrLn $ "@[storeToTaxo] newAssetMap: " <> show newAssetMap <> "."
                pure $ Right (updDirMap, iterAssetMap <> newAssetMap)
    ) (Right (namedDirMap, assetMap)) fsTree


storeDirToTaxo :: Pool -> Rto.RunOptions -> Int32 -> Mp.Map Text Int32 -> NamedDirMap St.NodeOut -> (Xpl.DirInfo, [Either String Xpl.FileInfo])
    -> IO (Either String (Int32, NamedDirMap St.NodeOut))
storeDirToTaxo dbPool rtOpts taxoID assetMap namedDirMap fsTree@(dirInfo, eiFileInfo) =
  let
    filePathList = splitDirectories (drop dirInfo.rootLengthDI dirInfo.lpathDI)
    derefPos = derefPath ([], []) namedDirMap filePathList
  in do
  putStrLn $ "@[storeDirToTaxo] derefPos: " <> show derefPos
  case derefPos of
    (foundPath, []) ->
      if null foundPath then
        pure . Left $ "@[storeDirToTaxo] empty foundPath and empty path for dirInfo: " <> show dirInfo <> "."
      else
        let
          lastNode = last foundPath
        in
        pure $ Right (snd lastNode, namedDirMap)
    (foundPath, pathsToAdd) ->
      let
        mbParentID = case foundPath of
          [] -> Nothing
          _ -> Just . snd . last $ foundPath
      in do
      case pathsToAdd of
        [aPath] -> do
          eiNodeOut <- Do.addPathToTaxo dbPool taxoID mbParentID dirInfo aPath
          case eiNodeOut of
            Left err -> pure $ Left $ "@[storeDirToTaxo] addPathToTaxo err: " <> show err <> "."
            Right (newNodeIn, newNodeOut) ->
              let
                updDerefPos = (foundPath, [(aPath, newNodeIn, newNodeOut)])
                updDirMap = updateDirMap updDerefPos namedDirMap
                newNodeID = fst newNodeOut
              in do
              -- putStrLn $ "@[storeDirToTaxo] updDerefPos: " <> show updDerefPos
              -- putStrLn $ "@[storeDirToTaxo] updDirMap: " <> show updDirMap
              pure $ Right (newNodeID, updDirMap)
            
        _ -> -- This should only happens when connecting to a new taxo path (not FS); need to create nodes for the prefix, and then
             -- continue with dirInfo content for the last component of the path.
          let
            prefix = reverse . drop 1 . reverse $ pathsToAdd
            lastPath = last pathsToAdd
            mbAnchorID = case foundPath of
              [] -> Nothing
              _ -> Just . snd . last $ foundPath
          in do
          putStrLn $ "@[storeDirToTaxo] premature deep path: " <> show pathsToAdd
          rezA <- Ex.runExceptT $ foldM (addVirtualPath dbPool taxoID) (mbAnchorID, []) prefix
          case rezA of
            Left err -> pure $ Left err
            Right (mbLastID, nodeList) -> do
              eiNodeOut <- Do.addPathToTaxo dbPool taxoID mbLastID dirInfo lastPath
              case eiNodeOut of
                Left err -> pure $ Left $ "@[storeDirToTaxo] addPathToTaxo err: " <> show err <> "."
                Right (newNodeIn, newNodeOut) ->
                  let
                    updNodeList = map (\(nodeIn@(label, _, _, _, _, _), nodeOut) -> (T.unpack label, nodeIn, nodeOut)) nodeList
                    updDerefPos = (foundPath, updNodeList <> [(lastPath, newNodeIn, newNodeOut)])
                    updDirMap = updateDirMap updDerefPos namedDirMap
                    newNodeID = fst newNodeOut
                  in do
                  -- putStrLn $ "@[storeDirToTaxo] updDerefPos: " <> show updDerefPos
                  -- putStrLn $ "@[storeDirToTaxo] updDirMap: " <> show updDirMap
                  pure $ Right (newNodeID, updDirMap)
  where
  addVirtualPath :: Pool -> Int32 -> (Maybe Int32, [(St.NewNodeIn, St.NewNodeOut)]) -> FilePath -> Ex.ExceptT String IO (Maybe Int32, [(St.NewNodeIn, St.NewNodeOut)])
  addVirtualPath dbPool taxoID (mbParentID, accum) aPath = do
    now <- liftIO getCurrentTime
    let
      tPath = T.pack aPath
      newNodeIn = (tPath, now, "0755")
    eiNewDir <- liftIO $ Do.addVirtualDirToTaxo dbPool taxoID mbParentID newNodeIn
    case eiNewDir of
      Left err -> Ex.ExceptT $ pure $ Left err
      Right aResult@(_, (newNodeID, _)) ->
        Ex.ExceptT . pure . Right $ (Just newNodeID, accum <> [aResult])


updateDirMap :: ([(FilePath, Int32)], [(FilePath, St.NewNodeIn, St.NewNodeOut)]) -> NamedDirMap St.NodeOut -> NamedDirMap St.NodeOut
updateDirMap (foundPath, newNodes) namedDirMap =
  case foundPath of
    [] -> -- No prefix path found, add entirely to root
      insertPathChain newNodes Nothing namedDirMap
    pathChain ->
      updateAtPath pathChain newNodes namedDirMap
  where
  getNodeAtPath :: [(FilePath, St.NewNodeIn, St.NewNodeOut)] -> NamedDirMap St.NodeOut -> Maybe (NamedDirNode St.NodeOut)
  getNodeAtPath pathList dirMap =
    case pathList of
      [] -> Nothing
      [(lastName, _, _)] -> Mp.lookup lastName dirMap
      ((pName, _, _) : rest) ->
        Mp.lookup pName dirMap >>= \ndNode -> getNodeAtPath rest ndNode.children

  updateAtPath :: [(FilePath, Int32)] -> [(FilePath, St.NewNodeIn, St.NewNodeOut)] -> NamedDirMap St.NodeOut -> NamedDirMap St.NodeOut
  updateAtPath pathChain newNodes dirMap =
    case pathChain of
      [] -> dirMap
      [(name, _)] ->
        case Mp.lookup name dirMap of
          Nothing -> dirMap
          Just ndNode ->
            Mp.insert name (ndNode { children = insertPathChain newNodes (Just ndNode.node) ndNode.children }) dirMap
      ((name, _) : rest) ->
        case Mp.lookup name dirMap of
          Nothing -> dirMap
          Just ndNode ->
            let
              updNode = ndNode { children = updateAtPath rest newNodes ndNode.children }
            in
              Mp.insert name updNode dirMap

  -- Helper: Add a chain of nodes to NamedDirMap, optionally with parent nodeOut for each new node (for now, mock nodeOut with default values)
  insertPathChain :: [(FilePath, St.NewNodeIn, St.NewNodeOut)] -> Maybe St.NodeOut -> NamedDirMap St.NodeOut -> NamedDirMap St.NodeOut
  insertPathChain newNodes mbParentNode dirMap =
    case newNodes of
      [] -> dirMap
      (filePath, newNodeIn, (nodeID, enteredOn)) : rest ->
        case Mp.lookup filePath dirMap of
          Just ndNode ->
            Mp.insert filePath (ndNode { children = insertPathChain rest (Just ndNode.node) ndNode.children }) dirMap
          Nothing ->
            let
              -- NewNodeIn: label, parentid, assetid, lastmod, rights, arboid (Text, Maybe Int32, Maybe Int32, Int64, Text, Int32)
              -- NodeOut: id, label, parentID, assetID, lastMod, depth (Int32, Text, Maybe Int32, Maybe Int32, Maybe UTCTime, Int32)
              (label, parentID, _, _, _, _) = newNodeIn
              updDepth = case mbParentNode of
                Just (_, _, _, _, _, depth) -> depth + 1
                Nothing -> 0
              newNodeOut = (nodeID, label, parentID, Nothing, Just enteredOn, updDepth)
              updChildren = insertPathChain rest (Just newNodeOut) Mp.empty
              newDirNode = NamedDirNode { node = newNodeOut, children = updChildren }
            in
              Mp.insert filePath newDirNode dirMap


type NamedDirMap element = Mp.Map FilePath (NamedDirNode element)
data NamedDirNode element = NamedDirNode {
    node :: !element
  , children :: !(NamedDirMap element)
  }
  deriving Show

convFolderMapToNamedDirMap :: Tr.FolderMap St.NodeOut -> NamedDirMap St.NodeOut
convFolderMapToNamedDirMap folderMap =
  foldl (\accum node ->
    case node of
      Tr.FolderFT (Tr.FDetails nodeOut@(id, label, parentID, assetID, lastMod, depth) children) ->
        let
          updChildren = convFolderMapToNamedDirMap children
        in
        Mp.insert (T.unpack label) (NamedDirNode nodeOut updChildren) accum
      _ -> accum
    ) Mp.empty (Mi.elems folderMap)


derefPath :: ([(FilePath, Int32)], [FilePath]) -> NamedDirMap St.NodeOut -> [FilePath] -> ([(FilePath, Int32)], [FilePath])
derefPath (accum, leftOver) treeMap subPaths =
  case subPaths of
    [] -> (accum, leftOver <> subPaths)
    (aName : rest) -> case Mp.lookup aName treeMap of
      Nothing -> (accum, leftOver <> subPaths)
      Just ndNode ->
        let
          (nodeID, _, _, _, _, _) = ndNode.node
        in
        derefPath (accum <> [(aName, nodeID)], leftOver) ndNode.children rest


storeFilesToTaxo :: Pool -> S3.S3Conn -> Rto.RunOptions -> Mp.Map Text Int32 -> Int32 -> Int32 -> [Either String Xpl.FileInfo] -> IO (Either String [((Text, Int32), Int32)])
storeFilesToTaxo dbPool s3Conn rtOpts assetMap taxoID dirNodeID files = do
  -- putStrLn $ "@[storeFilesToTaxo] files: " <> show files
  fullRez <- Ex.runExceptT $ foldM (saveLoop dbPool s3Conn rtOpts taxoID) (assetMap, []) files
  case fullRez of
    Right (_, resultList) -> pure . Right $ resultList
    Left err -> pure . Left $ "@[storeFilesToTaxo] err: " <> err <> "."
  where
  saveLoop :: Pool -> S3.S3Conn -> Rto.RunOptions -> Int32 -> (Mp.Map Text Int32, [((Text, Int32), Int32)]) -> Either String Xpl.FileInfo -> Ex.ExceptT String IO (Mp.Map Text Int32, [((Text, Int32), Int32)])
  saveLoop dbPool s3Conn rtOpts taxoID (assetMap, iterFiles) = \case
    Left err -> Ex.ExceptT $ pure . Left $ "@[storeFilesToTaxo] file err: " <> show err <> "."
    Right fileInfo ->
      let
        tMd5h = T.pack fileInfo.md5hFI
      in do
      eiAssetID <- liftIO $ case Mp.lookup tMd5h assetMap of
          Just assetID -> pure $ Right (assetID, False)
          Nothing -> do
            -- putStrLn $ "@[storeFilesToTaxo] uploading asset: " <> show fileInfo.lpathFI <> " (md5: " <> show fileInfo.md5hFI <> ")."
            eiS3Info <- liftIO $ uploadAsset dbPool s3Conn rtOpts taxoID fileInfo
            case eiS3Info of
              Left err -> pure $ Left err
              Right s3Info -> do
                -- putStrLn $ "@[storeFilesToTaxo] adding asset: " <> show s3Info <> "."
                assetAddRez <- liftIO $ Do.addAsset dbPool fileInfo s3Info
                case assetAddRez of
                  Left err -> pure $ Left err
                  Right assetID -> pure $ Right (assetID, True)
      case eiAssetID of
        Left err -> do
          -- putStrLn $ "@[storeFilesToTaxo] adding asset for file: " <> show fileInfo.lpathFI <> " failed: " <> show err <> "."
          Ex.ExceptT $ pure $ Left err
        Right (assetID, newFlag) -> do
          -- putStrLn $ "@[storeFilesToTaxo] adding to dir: " <> show dirNodeID <> ", assetID: " <> show assetID <> "."
          eiFileRez <- liftIO $ Do.addFileToTaxo dbPool taxoID dirNodeID assetID fileInfo
          case eiFileRez of
            Left err -> do
              liftIO $ putStrLn $ "@[storeFilesToTaxo] adding file to taxo for file: "
                <> show fileInfo.lpathFI <> ", assetID: " <> show assetID <> " failed: " <> show err <> "."
              Ex.ExceptT $ pure $ Left err
            Right fileID ->
              let
                updAssetMap = if newFlag then
                   Mp.insert tMd5h assetID assetMap
                  else
                    assetMap
              in
              Ex.ExceptT . pure . Right $ (updAssetMap, iterFiles <> [((tMd5h, assetID), fileID)])


uploadAsset :: Pool -> S3.S3Conn -> Rto.RunOptions -> Int32 -> Xpl.FileInfo -> IO (Either String (Text, Int64))
uploadAsset dbPool s3Conn rtOpts taxoID fileInfo =
  let
    tMd5 = T.pack fileInfo.md5hFI
    locator = T.take 2 tMd5 <> "/" <> T.take 2 (T.drop 2 tMd5) <> "/" <> tMd5
  in do
  rezA <- Ss.putFile s3Conn fileInfo.lpathFI locator
  case rezA of
    Left err -> pure $ Left err
    -- TODO: get the real size that has been uploaded to S3.
    Right () -> pure $ Right (locator, fileInfo.sizeFI)
