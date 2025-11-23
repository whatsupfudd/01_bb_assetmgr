{-# LANGUAGE BangPatterns #-}
module Tree.Logic where

import qualified Data.Vector as Vc
import qualified Data.Map.Strict as Mp
import qualified Data.List as DL
import Data.Text (unpack)

import qualified DB.Statements as Op
import qualified Tree.Types as Tr


showTree :: Vc.Vector Op.NodeOut -> Tr.FolderMap Op.NodeOut -> Tr.DebugInfo Op.NodeOut -> IO ()
showTree items tree di = do
  putStrLn $ "@[showTree] # items: " <> show (Vc.length items)
          <> ", leftOvers #: " <> show (Prelude.length di.leftOver)
          <> ", p1-errs #: " <> show (Prelude.length di.p1Errs)
          <> ", p2-errs #: " <> show (Prelude.length di.p2Errs)
          -- <> ", # leaves: " <> show di.lenLeaves <> ", mp2 #: " <> show di.mp2Size
          <> ", tree size: " <> show (sizeTree tree)
          <> "."

  Prelude.mapM_ (showTreeItem 0) (Mp.elems tree)


showTreeItem :: Int -> Tr.FolderTree Op.NodeOut -> IO ()
showTreeItem depth item =
  let
    spacing = Prelude.replicate depth ' '
  in
  case item of
    Tr.FolderFT details -> do
      let
        (iID, label, parentID, md5ID, lastMod, d) = details.node
      putStrLn $ spacing <> "_| " <> unpack label <> " (" <> show iID <> ")"
      Prelude.mapM_ (showTreeItem (depth + 2)) (Mp.elems details.children)
    Tr.LeafFT node -> do
      let
        (iID, label, parentID, md5ID, lastMod, depth) = node
      putStrLn $ spacing <> " |# " <> unpack label <> " (" <> show iID <> ")"
    Tr.EmptyFT -> putStrLn $ spacing <> "<|"


sizeTree :: Tr.FolderMap Op.NodeOut -> Int
sizeTree tree =
  sizeTreeB 0 tree
  where
  sizeTreeB :: Int -> Tr.FolderMap Op.NodeOut -> Int
  sizeTreeB t1 tree =
    DL.foldl' treeCount t1 (Mp.elems tree)

  treeCount :: Int -> Tr.FolderTree Op.NodeOut -> Int
  treeCount curTotal element =
    case element of
      Tr.EmptyFT -> curTotal + 1
      Tr.LeafFT _ -> curTotal + 1
      Tr.FolderFT (Tr.FDetails _ chMap) ->
        sizeTreeB (curTotal + 1) chMap


--item@(id, label, mbParentID, md5ID, lastMod, depth) =
treeFromNodes :: Vc.Vector Op.NodeOut -> (Tr.FolderMap Op.NodeOut, Tr.DebugInfo Op.NodeOut)
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
                nTreeNode = Tr.FolderFT (Tr.FDetails opNode (Mp.empty :: Tr.FolderMap Op.NodeOut))
              in
                (Mp.insert oID nTreeNode accum, leafs)
            Just anID -> (accum, Tr.LeafFT opNode : leafs)
        ) (Mp.empty :: Tr.FolderMap Op.NodeOut, []) opNodes
    -- DBG: lenLeafs = Prelude.length leafList
    -- Link all leaf nodes to their parents:
    (!map2, !missed1) =
      DL.foldl' (\(accum, leftOver) leaf ->
          case leaf of
            Tr.LeafFT item ->
              let
                (lID, _, mbParentID, _, _, _) = item
              in
              case mbParentID of
                Nothing -> (accum, leaf : leftOver)
                Just parID -> case Mp.lookup parID accum of
                  Nothing -> (accum, leaf : leftOver)
                  Just !treeNode ->
                    case treeNode of
                      Tr.FolderFT (Tr.FDetails fn children) ->
                        let
                          !updTreeNode = Tr.FolderFT $ Tr.FDetails fn (Mp.insert lID leaf children)
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
            (!nRoot, !nOrphans, !nErrs) = addToTree root node
          in
            (nRoot, accOrphans <> nOrphans, accErrs <> nErrs)
        ) (Mp.empty :: Tr.FolderMap Op.NodeOut, [], []) map2
  in
  (map3, Tr.DebugInfo missed1 missed2 errs 0 0)  -- lenLeafs mp2Size
  -- (map2, missed1, [])
  
-- out: updated tree, orphans, errors
addToTree :: Tr.FolderMap Op.NodeOut -> Tr.FolderTree Op.NodeOut -> (Tr.FolderMap Op.NodeOut, [ Tr.FolderTree Op.NodeOut ], [ Tr.FolderTree Op.NodeOut ])
addToTree root Tr.EmptyFT = (root, [ ], [ Tr.EmptyFT ] )
addToTree !root !entry =
  let
    item@(iID, label, mbParentID, md5ID, lastMod, depth) =
      case entry of
        Tr.LeafFT i -> i
        Tr.FolderFT (Tr.FDetails i _) -> i
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
        Tr.FolderFT (Tr.FDetails (vID, _, _, _, _, _) children) -> (Mp.insert vID element fMap, errs)
        Tr.LeafFT (vID, _, _, _, _, _) -> (Mp.insert vID element fMap, errs)
        _ -> (fMap, element : errs)
    ) (Mp.empty :: Tr.FolderMap Op.NodeOut, []) cList



-- addInChildren :: [ FolderTree ] -> FolderTree -> [ FolderTree ] -> FolderTree -> Maybe [ FolderTree ]
addInChildren :: [ Tr.FolderTree Op.NodeOut ] -> Tr.FolderTree Op.NodeOut
        -> [ Tr.FolderTree Op.NodeOut ] -> Tr.FolderTree Op.NodeOut
        -> Maybe (Tr.FolderTree Op.NodeOut)
addInChildren _ _ _ Tr.EmptyFT = Nothing
addInChildren _ _ _ (Tr.LeafFT _) = Nothing
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


addToNode :: Tr.FolderTree Op.NodeOut -> Tr.FolderTree Op.NodeOut -> Maybe (Tr.FolderTree Op.NodeOut)
addToNode _ Tr.EmptyFT = Nothing
addToNode (Tr.LeafFT _) _ = Nothing
addToNode Tr.EmptyFT _ = Nothing
-- (id, label, parentID, md5ID, lastMod, depth)
addToNode (Tr.FolderFT (Tr.FDetails !srcItem !srcChildren)) !childNode =
  let
    (srcID, _, _, _, _, _) = srcItem
    childItem = case childNode of
      Tr.FolderFT (Tr.FDetails i _) -> i
      Tr.LeafFT i -> i
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
        Just $ Tr.FolderFT (Tr.FDetails srcItem newChildren)
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
                Just $ Tr.FolderFT (Tr.FDetails srcItem newChildren)


getNodeKey aNode =
  case aNode of
    Tr.FolderFT (Tr.FDetails (vID, _, _, _, _, _) children) -> Just vID
    Tr.LeafFT (vID, _, _, _, _, _) -> Just vID
    Tr.EmptyFT -> Nothing
