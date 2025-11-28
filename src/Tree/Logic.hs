{-# LANGUAGE BangPatterns #-}
module Tree.Logic where

import Data.Int (Int32)
import qualified Data.Vector as Vc
import qualified Data.Map.Strict as Mp
import qualified Data.IntMap.Strict as Mi
import qualified Data.List as DL
import Data.Text (unpack)

import qualified DB.Statements as Op
import qualified Tree.Types as Tr
import Data.Maybe (fromMaybe)


defaultDebugInfo :: Tr.DebugInfo Op.NodeOut
defaultDebugInfo = Tr.DebugInfo {
    leftOver = []
    , p1Errs = []
    , p2Errs = []
    , lenLeaves = 0
    , mp2Size = 0
  }


showTree :: Vc.Vector Op.NodeOut -> Tr.FolderMap Op.NodeOut -> Tr.DebugInfo Op.NodeOut -> IO ()
showTree items tree di = do
  putStrLn $ "@[showTree] # items: " <> show (Vc.length items)
          <> ", leftOvers #: " <> show (Prelude.length di.leftOver)
          <> ", p1-errs #: " <> show (Prelude.length di.p1Errs)
          <> ", p2-errs #: " <> show (Prelude.length di.p2Errs)
          -- <> ", # leaves: " <> show di.lenLeaves <> ", mp2 #: " <> show di.mp2Size
          <> ", tree size: " <> show (sizeTree tree)
          <> "."
  Prelude.mapM_ (showTreeItem 0) (Mi.elems tree)


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
      Prelude.mapM_ (showTreeItem (depth + 2)) (Mi.elems details.children)
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
    DL.foldl' treeCount t1 (Mi.elems tree)

  treeCount :: Int -> Tr.FolderTree Op.NodeOut -> Int
  treeCount curTotal element =
    case element of
      Tr.EmptyFT -> curTotal + 1
      Tr.LeafFT _ -> curTotal + 1
      Tr.FolderFT (Tr.FDetails _ chMap) ->
        sizeTreeB (curTotal + 1) chMap

{- version Hugo:
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
                nTreeNode = Tr.FolderFT (Tr.FDetails opNode (Mi.empty :: Tr.FolderMap Op.NodeOut))
              in
                (Mi.insert (fromIntegral oID) nTreeNode accum, leafs)
            Just anID -> (accum, Tr.LeafFT opNode : leafs)
        ) (Mi.empty :: Tr.FolderMap Op.NodeOut, []) opNodes
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
                Just parID -> case Mi.lookup (fromIntegral parID) accum of
                  Nothing -> (accum, leaf : leftOver)
                  Just !treeNode ->
                    case treeNode of
                      Tr.FolderFT (Tr.FDetails fn children) ->
                        let
                          !updTreeNode = Tr.FolderFT $ Tr.FDetails fn (Mi.insert (fromIntegral lID) leaf children)
                        in
                        (Mi.insert (fromIntegral parID) updTreeNode accum, leftOver)
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
        ) (Mi.empty :: Tr.FolderMap Op.NodeOut, [], []) map2
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
        (Mi.insert (fromIntegral iID) entry root, [], [])
      Just _ ->
        case Mi.elems root of
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
                      Just anID -> (Mi.insert (fromIntegral anID) updChild root, [])
                in
                -- TODO: figure out error handling
                ( newMap, [], errs )


childrenListToMap !cList =
  DL.foldl' (\(!fMap, errs) !element ->
      case element of
        Tr.FolderFT (Tr.FDetails (vID, _, _, _, _, _) children) -> (Mi.insert (fromIntegral vID) element fMap, errs)
        Tr.LeafFT (vID, _, _, _, _, _) -> (Mi.insert (fromIntegral vID) element fMap, errs)
        _ -> (fMap, element : errs)
    ) (Mi.empty :: Tr.FolderMap Op.NodeOut, []) cList



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
          newChildren = Mi.insert (fromIntegral itemID) childNode oChildren
        in
        Just $ Tr.FolderFT (Tr.FDetails srcItem newChildren)
      else
        case Mi.elems srcChildren of
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
                      Just anID -> (Mi.insert (fromIntegral anID) updChild srcChildren, [])
                in
                -- TODO: figure out error handling
                Just $ Tr.FolderFT (Tr.FDetails srcItem newChildren)


getNodeKey :: Tr.FolderTree Op.NodeOut -> Maybe Int32
getNodeKey aNode =
  case aNode of
    Tr.FolderFT (Tr.FDetails (vID, _, _, _, _, _) children) -> Just vID
    Tr.LeafFT (vID, _, _, _, _, _) -> Just vID
    Tr.EmptyFT -> Nothing
-}

-- Gpt5.1 version:
-- | Build a ForestMap (top-level map of roots, keyed by id) from a Vector of NodeOut.
nodeId :: Op.NodeOut -> Int32
nodeId (i,_,_,_,_,_) = i

nodeParentId :: Op.NodeOut -> Maybe Int32
nodeParentId (_,_,mp,_,_,_) = mp

nodeAssetId :: Op.NodeOut -> Maybe Int32
nodeAssetId (_,_,_,ma,_,_) = ma

-- Helper: Int32 -> Int key for IntMap
int32Key :: Int32 -> Int
int32Key = fromIntegral

{-v1:
buildForestMapV :: Vc.Vector Op.NodeOut -> Tr.FolderMap Op.NodeOut
buildForestMapV vec =
  let
    (rootRev, childrenByParent) = Vc.foldl' step ([], Mi.empty) vec

    step (roots, mp) itm =
      case nodeParentId itm of
        Nothing ->
          (itm : roots, mp)
        Just p  ->
          let k   = int32Key p
              mp' = Mi.insertWith (++) k [itm] mp
          in (roots, mp')

    roots = reverse rootRev
  in
    Mi.fromList
      [ (int32Key (nodeId r), buildSubtree childrenByParent r)
      | r <- roots
      ]


-- | Expand a single NodeOut into a FolderTree, recursively.
buildSubtree
  :: Mi.IntMap [Op.NodeOut]  -- ^ parentID -> children
  -> Op.NodeOut
  -> Tr.FolderTree Op.NodeOut
buildSubtree childrenByParent nodeOut =
  case nodeAssetId nodeOut of
    -- Leaf (file)
    Just _assetId ->
      Tr.LeafFT nodeOut

    -- Folder (directory)
    Nothing ->
      let
        myId        = nodeId nodeOut
        childItems  = Mi.findWithDefault [] (int32Key myId) childrenByParent

        -- childItems are in reverse insertion order due to insertWith (++).
        -- Reverse once here to get back to input order.
        orderedChildren = reverse childItems

        childForest :: Tr.FolderMap Op.NodeOut
        childForest =
          Mi.fromList
            [ (int32Key (nodeId c), buildSubtree childrenByParent c)
            | c <- orderedChildren
            ]
      in
        Tr.FolderFT (Tr.FDetails nodeOut childForest)
-}

-- v2:
buildForestMapV :: Vc.Vector Op.NodeOut -> Tr.FolderMap Op.NodeOut
buildForestMapV vec =
  let
    -- Phase 1: group child indices by parent id, and collect root indices.
    (rootIdxsRev, childIndexMap) = Vc.ifoldl' iterForest ([], Mi.empty) vec

    iterForest :: ([Int], Mi.IntMap [Int]) -> Int -> Op.NodeOut -> ([Int], Mi.IntMap [Int])
    iterForest (!roots, !mp) ix n =
      case nodeParentId n of
        Nothing ->
          -- root element
          (ix : roots, mp)
        Just pid ->
          let k   = int32Key pid
              mp' = Mi.insertWith (++) k [ix] mp
              -- We store children lists in reverse order per parent
              -- (O(1) append). We'll fix order later with a single 'reverse'.
          in (roots, mp')

    -- We constructed roots in reverse order.
    rootIdxs = Vc.reverse (Vc.fromList rootIdxsRev)

    -- Phase 2: recursively build FolderTree from roots downwards.
    buildSubtree :: Int -> Tr.FolderTree Op.NodeOut
    buildSubtree ix =
      let n = vec Vc.! ix
      in case nodeAssetId n of
           -- Leaf (file) – we ignore any children in the DB by design.
           Just _aid ->
             Tr.LeafFT n

           -- Folder (directory)
           Nothing   ->
             let myId          = nodeId n
                 -- Child indices were accumulated in reverse order.
                 childIdxsRev  = Mi.findWithDefault [] (int32Key myId) childIndexMap
                 childIdxs     = Vc.reverse (Vc.fromList childIdxsRev)
                 childMap      =
                   Vc.foldl' (\acc cix ->
                             let cn      = buildSubtree cix
                                 childId = nodeId (vec Vc.! cix)
                             in Mi.insert (int32Key childId) cn acc)
                          Mi.empty
                          childIdxs
             in Tr.FolderFT (Tr.FDetails n childMap)
                  

    -- Build the top-level forest, keyed by each root's id.
    forest :: Tr.FolderMap Op.NodeOut
    forest =
      Vc.foldl' (\acc ix ->
                let n  = vec Vc.! ix
                    tn = buildSubtree ix
                in Mi.insert (int32Key (nodeId n)) tn acc)
             Mi.empty
            rootIdxs

  in forest

-- Convenience entry point if your data starts as a list:
buildForestMapFromList :: [Op.NodeOut] -> Tr.FolderMap Op.NodeOut
buildForestMapFromList xs =
  buildForestMapV (Vc.fromList xs)


