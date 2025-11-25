
module Commands.Import where

import Control.Monad.Cont (runContT)
import Control.Monad (when, foldM, foldM_)

import Data.Bifunctor (first)
import Data.Bits (testBit)
import Data.Either (lefts, partitionEithers)
import qualified Data.Foldable as Fld
import Data.Int (Int32)
import qualified Data.IntMap as Mi
import qualified Data.Map.Strict as Mp
import qualified Data.List as DL
import qualified Data.Sequence as Seq
import Data.Text (Text, unpack, isPrefixOf, pack)
import qualified Data.Vector as V

import System.Directory (getCurrentDirectory)
import System.FilePath ((</>), splitDirectories)

import Hasql.Pool (Pool, use)
import qualified Hasql.Transaction as Tx
import qualified Hasql.Transaction.Sessions as Txs

import DB.Connect (startPg)
import qualified Options.Runtime as Rto
import qualified Storage.Explore as Xpl
import qualified Tree.Types as Tr
import qualified Tree.Logic as Tl
import qualified DB.Statements as St
import qualified DB.Operations as Do

import Options.Cli (ImportOpts (..))


importCmd :: ImportOpts -> Rto.RunOptions -> IO ()
importCmd importOpts rtOpts = do
  when (testBit rtOpts.debug 0) $
    putStrLn $ "@[importCmd] starting, opts: " <> show rtOpts
  runContT (startPg rtOpts.pgDbConf) mainAction
  where
  mainAction dbPool = do
    let
      shutdownHandler = putStrLn "@[importCmd] Terminating..."
      mbAnchorPath = case importOpts.anchor of
        "" -> Nothing
        aValue -> Just aValue
    destDir <- if isPrefixOf "/" (pack importOpts.path) then
        pure importOpts.path
      else
        case rtOpts.root of
          Nothing -> getCurrentDirectory
          Just root -> pure $ root <> "/" <> importOpts.path
    tree <- Seq.drop 1 <$> Xpl.loadFolderTree destDir
    let
      rebasedTree = if importOpts.anchor /= "" then
        first (\dirInfo -> (dirInfo { Xpl.pathDI = importOpts.anchor </> dirInfo.pathDI })) <$> tree
      else
        tree
    -- DBG:
    when (testBit rtOpts.debug 1) $ do
      parsing <- Xpl.parseTree tree
      putStrLn $ "@[importCmd] tree-def: " <> show parsing <> " folder/files."
      putStrLn $ "@[importCmd] root: " <> destDir
      -- putStrLn $ "@[importCmd] tree: " <> show rebasedTree
      Xpl.showTree rebasedTree
    loadFileTree dbPool rtOpts importOpts.taxonomy mbAnchorPath rebasedTree


loadFileTree :: Pool -> Rto.RunOptions -> Text -> Maybe FilePath -> Xpl.RType -> IO ()
loadFileTree dbPool rtOpts taxonomy mbAnchorPath fsTree = do
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
            -- putStrLn $ "@[importCmd] assets: " <> show assets <> "."
            if newFlag then
              addFilesToTaxo dbPool rtOpts taxoID assetMap fsTree
            else
              appendFilesToTaxo dbPool rtOpts taxoID assetMap fsTree
          (errs, _) -> do
            putStrLn $ "@[importCmd] md5Existence errs: " <> show errs <> "."
    pure ()


addFilesToTaxo :: Pool -> Rto.RunOptions -> Int32 -> Mp.Map Text Int32 -> Xpl.RType -> IO ()
addFilesToTaxo dbPool rtOpts taxoID assetMap fsTree =
  putStrLn "@[addFilesToTaxo] starting..."



md5Existence :: Pool -> Seq.Seq (Xpl.DirInfo, [Either String Xpl.FileInfo]) -> IO (Either String (Mp.Map Text Int32))
md5Existence dbPool assetBlock = do
  rezB <- Do.fetchAssetsByMD5 dbPool Seq.empty   -- assetBlock
  case rezB of
    Left err ->
      pure . Left $ "@[md5Existence] err: " <> show err <> "."
    Right idMap -> do
      pure $ Right idMap


appendFilesToTaxo :: Pool -> Rto.RunOptions -> Int32 -> Mp.Map Text Int32 -> Xpl.RType -> IO ()
appendFilesToTaxo dbPool rtOpts taxoID assetMap fsTree = do
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
      storeRez <- storeFilesToTaxo dbPool rtOpts taxoID assetMap namedDirMap fsTree
      -- putStrLn $ "@[appendFilesToTaxo] storeRez: " <> show storeRez
      pure ()
  pure ()


{-
data FileInfo = FileInfo {
    path :: !FilePath
    , md5h :: !String
    , size :: {-# UNPACK #-} !Int64
    , modifTime :: {-# UNPACK #-} !Int64
    , perms :: !Word16
  }
  deriving Show

type RType = Seq.Seq (FilePath, [Either String FileInfo])
-}

storeFilesToTaxo :: Pool -> Rto.RunOptions -> Int32 -> Mp.Map Text Int32 -> NamedDirMap St.NodeOut -> Xpl.RType -> IO (Either String (NamedDirMap St.NodeOut))
storeFilesToTaxo dbPool rtOpts taxoID assets namedDirMap fsTree = do
  putStrLn $ "@[storeFilesToTaxo] fsTree: " <> show fsTree
  foldM (\accum aNode ->
    case accum of
      Left err -> pure $ Left err
      Right aMap -> storeDirToTaxo dbPool rtOpts taxoID assets aMap aNode
    ) (Right namedDirMap) fsTree


storeDirToTaxo :: Pool -> Rto.RunOptions -> Int32 -> Mp.Map Text Int32 -> NamedDirMap St.NodeOut -> (Xpl.DirInfo, [Either String Xpl.FileInfo])
    -> IO (Either String (NamedDirMap St.NodeOut))
storeDirToTaxo dbPool rtOpts taxoID assetMap namedDirMap fsTree@(dirInfo, eiFileInfo) =
  let
    filePathList = splitDirectories dirInfo.pathDI
    derefPos = derefPath ([], []) namedDirMap filePathList
  in do
  putStrLn $ "@[storeDirToTaxo] derefPos: " <> show derefPos
  case derefPos of
    (foundPath, []) -> pure $ Right namedDirMap
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
              in do
              -- putStrLn $ "@[storeDirToTaxo] updDerefPos: " <> show updDerefPos
              -- putStrLn $ "@[storeDirToTaxo] updDirMap: " <> show updDirMap
              pure $ Right updDirMap
            
        _ -> -- This should only happens when connecting to a new taxo path (not FS); need to create nodes for the prefix, and then
             -- continue with dirInfo content for the last component of the path.
             -- TODO:
          pure $ Left $ "@[storeDirToTaxo] premature deep path: " <> dirInfo.pathDI


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
        Mp.insert (unpack label) (NamedDirNode nodeOut updChildren) accum
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
