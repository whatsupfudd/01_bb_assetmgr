{-# LANGUAGE BangPatterns #-}

module Commands.Import where

import Control.Monad.Cont (runContT)
import qualified Data.Map as Mp
import Data.Bits (testBit)
import Data.Int (Int32)
import qualified Data.List as DL
import Data.Text (Text, unpack, isPrefixOf)
import Data.Vector as Vc

import Hasql.Pool (Pool, use)

import DB.Connect (startPg)
import qualified Options.Runtime as Rto
import qualified Storage.Explore as Xpl
import qualified DB.Opers as Op

type ImportParams = (Text, Text)

importCmd :: ImportParams -> Rto.RunOptions -> IO ()
importCmd (taxonomy, path) rtOpts = do
  if testBit rtOpts.debug 0 then
    putStrLn $ "@[importCmd] starting, opts: " <> show rtOpts
  else
    pure ()
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
    if testBit rtOpts.debug 1 then do
      putStrLn $ "@[importCmd] tree-def: " <> show parsing <> " folder/files."
      putStrLn $ "@[importCmd] root: " <> destDir <> ", tree: " <> show tree
    else
      pure ()
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
                (treeMap, dbgInfo) = treeFromNodes items
              -- DBG:
              in
              showTree items treeMap dbgInfo
            else
              putStrLn $ "@[showTree] # items: " <> (show $ Vc.length items)
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


type FolderMap = Mp.Map Int32 FolderTree
data FolderTree =
  FolderFT FDetails
  | LeafFT Op.NodeOut
  | EmptyFT
  deriving (Show)

data FDetails = FDetails { node :: !Op.NodeOut, children :: !FolderMap }
  deriving (Show)


data DebugInfo = DebugInfo {
    leftOver :: [ FolderTree ]
    , p1Errs :: [ FolderTree ]
    , p2Errs :: [ FolderTree ]
    , lenLeaves :: Int
    , mp2Size :: Int
  }


showTree :: Vc.Vector Op.NodeOut -> FolderMap -> DebugInfo -> IO ()
showTree items tree di = do
  putStrLn $ "@[showTree] # items: " <> (show $ Vc.length items)
          <> ", leftOvers #: " <> show (Prelude.length di.leftOver)
          <> ", p1-errs #: " <> show (Prelude.length di.p1Errs)
          <> ", p2-errs #: " <> show (Prelude.length di.p2Errs)
          -- <> ", # leaves: " <> show di.lenLeaves <> ", mp2 #: " <> show di.mp2Size
          <> ", tree size: " <> show (sizeTree tree)
          <> "."

  Prelude.mapM_ (showTreeItem 0) (Mp.elems tree)


showTreeItem :: Int -> FolderTree -> IO ()
showTreeItem depth item =
  let
    spacing = Prelude.replicate depth ' '
  in
  case item of
    FolderFT details -> do
      let
        (iID, label, parentID, md5ID, lastMod, d) = details.node
      putStrLn $ spacing <> "_| " <> unpack label <> " (" <> show iID <> ")"
      Prelude.mapM_ (showTreeItem (depth + 2)) (Mp.elems details.children)
    LeafFT node -> do
      let
        (iID, label, parentID, md5ID, lastMod, depth) = node
      putStrLn $ spacing <> " |# " <> unpack label <> " (" <> show iID <> ")"
    EmptyFT -> putStrLn $ spacing <> "<|"


sizeTree :: FolderMap -> Int
sizeTree tree =
  sizeTreeB 0 tree
  where
  sizeTreeB :: Int -> FolderMap -> Int
  sizeTreeB t1 tree =
    DL.foldl' treeCount t1 (Mp.elems tree)

  treeCount :: Int -> FolderTree -> Int
  treeCount curTotal element =
    case element of
      EmptyFT -> curTotal + 1
      LeafFT _ -> curTotal + 1
      FolderFT (FDetails _ chMap) ->
        sizeTreeB (curTotal + 1) chMap


--item@(id, label, mbParentID, md5ID, lastMod, depth) =
treeFromNodes :: Vc.Vector Op.NodeOut -> (FolderMap, DebugInfo)
treeFromNodes !opNodes =
  let
    -- Break into branches & leaves:
    (!map1, !leafList) = Vc.foldl' (\(accum, leafs) opNode ->
          let
            (oID, _, _, md5id, _, _) = opNode
          in
          case md5id of
            Nothing ->
              let
                nTreeNode = FolderFT (FDetails opNode (Mp.empty :: FolderMap))
              in
                (Mp.insert oID nTreeNode accum, leafs)
            Just anID -> (accum, (LeafFT opNode) : leafs)
        ) (Mp.empty :: FolderMap, []) opNodes
    -- DBG: lenLeafs = Prelude.length leafList
    -- Link all leaf nodes to their parents:
    (!map2, !missed1) =
      DL.foldl' (\(accum, leftOver) leaf ->
          case leaf of
            LeafFT item ->
              let
                (lID, _, mbParentID, _, _, _) = item
              in
              case mbParentID of
                Nothing -> (accum, leaf : leftOver)
                Just parID -> case Mp.lookup parID accum of
                  Nothing -> (accum, leaf : leftOver)
                  Just treeNode ->
                    case treeNode of
                      FolderFT (FDetails fn children) ->
                        let
                          updTreeNode = FolderFT $ FDetails fn (Mp.insert lID leaf children)
                        in
                        (Mp.insert parID updTreeNode accum, leftOver)
                      _ -> (accum, leaf : leftOver)
            -- That should never happen:
            _ -> (accum, leaf : leftOver)
        ) (map1, []) leafList
    -- DBG mp2Size = sizeTree map2

    (!map3, !missed2, errs) =
      DL.foldl' (\(root, accOrphans, accErrs) node ->
          let
            (nRoot, nOrphans, nErrs) = addToTree_B root node
          in
            (nRoot, accOrphans <> nOrphans, accErrs <> nErrs)
        ) (Mp.empty :: FolderMap, [], []) map2
  in
  (map3, DebugInfo missed1 missed2 errs 0 0)  -- lenLeafs mp2Size
  -- (map2, missed1, [])
  {-
  where
  addToTree_A :: FolderMap -> Op.NodeOut -> FolderMap
  addToTree_A root node@(id, label, parentID, md5ID, lastMod, depth) =
    let
      entry = case md5ID of
        Nothing -> FolderFT node (Mp.empty::FolderMap)
        Just anID -> LeafFT node
    in
      case parentID of
        Nothing ->
          Mp.insert id entry root
        Just pID ->
          case Mp.lookup pID root of
            Nothing ->
              -- TODO: dig down
              root
            Just parentItem ->
              case parentItem of
                FolderFT node children ->
                  let
                    nChildren = Mp.insert id entry children
                  in
                  Mp.insert pID (FolderFT node nChildren) root
                _ ->
                   -- TODO: signal an error
                   root
          -- ? trouve le parent dans l'arbo, ajoute l'enfant, update toute la chaine parent
  -}

-- out: updated tree, orphans, errors
addToTree_B :: FolderMap -> FolderTree -> (FolderMap, [ FolderTree ], [ FolderTree ])
addToTree_B root e@(EmptyFT) = (root, [ ], [ e ] )
addToTree_B !root !entry =
  let
    item@(iID, label, mbParentID, md5ID, lastMod, depth) =
      case entry of
        LeafFT i -> i
        FolderFT (FDetails i _) -> i
        -- TODO: handle Empty items with error.
    {-
    entry = case md5ID of
      Nothing -> FolderFT item (Mp.empty::FolderMap)
      Just anID -> LeafFT item
    -}
  in
    case mbParentID of
      -- It's a top-level entry:
      Nothing ->
        (Mp.insert iID entry root, [], [])
      Just _ ->
        case Mp.elems root of
          [] -> (root, [ entry ], [])
          eleHead : eleTail ->
            case addInChildren [] eleHead eleTail entry of
              -- Couldn't place the entry in the children, it's orphan:
              Nothing -> (root, [ entry ], [])
              -- one sub-tree has been modified but don't know which, so just reconstruct the map:
              -- Just updList ->
              Just updChild ->
                let
                  -- (newMap, errs) = childrenListToMap updList
                  (newMap, errs) =
                    case getNodeKey updChild of
                      Nothing -> (root, [ updChild ])
                      Just anID -> (Mp.insert anID updChild root, [])
                in
                -- TODO: figure out error handling
                ( newMap, [], errs )


childrenListToMap !cList =
  DL.foldl' (\(!fMap, errs) !element ->
      case element of
        FolderFT (FDetails (vID, _, _, _, _, _) children) -> (Mp.insert vID element fMap, errs)
        LeafFT (vID, _, _, _, _, _) -> (Mp.insert vID element fMap, errs)
        _ -> (fMap, element : errs)
    ) (Mp.empty :: FolderMap, []) cList



-- addInChildren :: [ FolderTree ] -> FolderTree -> [ FolderTree ] -> FolderTree -> Maybe [ FolderTree ]
addInChildren :: [ FolderTree ] -> FolderTree -> [ FolderTree ] -> FolderTree -> Maybe FolderTree
addInChildren _ _ _ EmptyFT = Nothing
addInChildren _ _ _ (LeafFT _) = Nothing
addInChildren !prev !curr !after !item =
  case addToNode curr item of
    -- Sometihng was updated, recompose list with modified entry:
    Just newCurr ->
      -- Just $ prev <> ( newCurr : after )
      Just $ newCurr
    -- No update in current item, keep looking into the list:
    Nothing -> case after of
      -- List eneded, nothing was updated:
      [] -> Nothing
      -- Still some entries to check:
      _ ->
        addInChildren (curr : prev) (Prelude.head after) (Prelude.tail after) item


addToNode :: FolderTree -> FolderTree -> Maybe FolderTree
addToNode _ EmptyFT = Nothing
addToNode (LeafFT _) _ = Nothing
addToNode EmptyFT _ = Nothing
-- (id, label, parentID, md5ID, lastMod, depth)
addToNode (FolderFT (FDetails !srcItem !srcChildren)) !childNode =
  let
    (srcID, _, _, _, _, _) = srcItem
    childItem = case childNode of
      FolderFT (FDetails i _) -> i
      LeafFT i -> i
      -- TODO: handle the empty case.
    (itemID, _, mbParentID, md5ID, _, _) = childItem
  in
  case mbParentID of
    Nothing -> Nothing
    Just parentID ->
      if srcID == parentID then
        let
          oChildren = srcChildren
          newChildren = Mp.insert itemID childNode oChildren
        in
        Just $ FolderFT (FDetails srcItem newChildren)
      else
        case Mp.elems srcChildren of
          [] -> Nothing
          eleHead : eleTail ->
            case addInChildren [] eleHead eleTail childNode of
              Nothing -> Nothing
              -- Just updList ->
              Just updChild ->
                let
                  -- (newChildren, errs) = childrenListToMap updList
                  (newChildren, errs) =
                    case getNodeKey updChild of
                      Nothing -> (srcChildren, [ updChild ])
                      Just anID -> (Mp.insert anID updChild srcChildren, [])
                in
                -- TODO: figure out error handling
                Just $ FolderFT (FDetails srcItem newChildren)


getNodeKey aNode =
  case aNode of
    FolderFT (FDetails (vID, _, _, _, _, _) children) -> Just vID
    LeafFT (vID, _, _, _, _, _) -> Just vID
    EmptyFT -> Nothing
