{-# LANGUAGE BangPatterns #-}
module Logic.Listing where

import Data.Int (Int32)
import qualified Data.Map.Strict as Mp
import Data.Time (UTCTime, getCurrentTime, diffUTCTime)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Vector as Vc

import qualified System.FilePath as Fs

import Hasql.Pool (Pool, use)

import qualified DB.Statements as St
import qualified Tree.Logic as Tl


listTaxosForOwner :: Pool -> Text -> IO (Either String ListResult)
listTaxosForOwner dbPool ownerName = do
  rezB <- use dbPool $ St.fetchOwnerByName ownerName
  case rezB of
    Left err -> pure . Left $ "@[listCmd] fetchOwnerByName err: " <> show err
    Right mbOwnerID -> case mbOwnerID of
        Nothing -> pure . Left $ "@[listCmd] fetchOwnerByName: owner not found."
        Just ownerID -> do
          rezD <- use dbPool $ St.fetchTaxosForOwner ownerID
          case rezD of
            Left err -> pure . Left $ "@[listCmd] fetchTaxosForOwner err: " <> show err
            Right items -> pure . Right . Triplet $ items


listTaxoPath :: Pool -> Text -> Text -> Int32 -> IO (Either String ListResult)
listTaxoPath dbPool ownerName taxonomyName maxDepth =
  case T.split (== '/') taxonomyName of
    [taxoPath] -> do
      rezE <- use dbPool $ St.fetchTaxosForOwnerLabel (ownerName, taxoPath)
      case rezE of
        Left err -> pure . Left $ "@[listCmd] fetchTaxosForOwnerLabel err: " <> show err
        Right mbTaxoID -> case mbTaxoID of
          Nothing -> pure . Left $ "@[listCmd] fetchTaxosForOwnerLabel: taxonomy not found."
          Just taxoID -> do
            case maxDepth of
              0 -> do
                rezF <- use dbPool $ St.fetchRootsForTaxo taxoID
                case rezF of
                  Left err -> pure . Left $ "@[listCmd] fetchRootsForTaxo err: " <> show err
                  Right items -> pure . Right . Taxo $ items
              _ -> do
                startTime <- getCurrentTime
                rezG <- use dbPool $ St.fetchNodesForTaxoLimited taxoID maxDepth
                endTime <- getCurrentTime
                putStrLn $ "@[listCmd] fetchTaxosForOwnerLabel time: " <> show (diffUTCTime endTime startTime)
                case rezG of
                  Left err -> pure . Left $ "@[listCmd] fetchNodesForTaxoLimited err: " <> show err
                  Right items -> pure . Right . Nodes $ items
    taxoPath : dirPaths -> do
      rezE <- use dbPool $ St.fetchTaxosForOwnerLabel (ownerName, taxoPath)
      case rezE of
        Left err -> pure . Left $ "@[listCmd] fetchTaxosForOwnerLabel err: " <> show err
        Right mbTaxoID -> case mbTaxoID of
          Nothing -> pure . Left $ "@[listCmd] fetchTaxosForOwnerLabel: taxonomy not found."
          Just taxoID -> do
            rezH <- use dbPool $ St.derefPathToNode taxoID (Vc.fromList dirPaths)
            case rezH of
              Left err -> pure . Left $ "@[listCmd] derefPathToNode err: " <> show err
              Right mbNodeID -> case mbNodeID of
                Nothing -> pure . Left $ "@[listCmd] derefPathToNode: node not found."
                Just nodeID -> do
                  rezI <- use dbPool $ St.fetchSubTreeForNode nodeID maxDepth
                  case rezI of
                    Left err -> pure . Left $ "@[listCmd] fetchNodesForTaxoLimited err: " <> show err
                    Right items ->
                      if Vc.null items then
                        pure . Right . Nodes $ items
                      else
                        let
                          -- type NodeOut = (Int32, Text, Maybe Int32, Maybe Int32, Maybe UTCTime, Int32)
                          (id, label, parentID, assetID, lastMod, depth) = Vc.head items
                          newItems = Vc.cons (id, label, Nothing, assetID, lastMod, depth) (Vc.tail items)
                        in
                        pure . Right . Nodes $ newItems

data ListResult =
  Triplet (Vc.Vector St.TaxoLabelOut)
  | Taxo (Vc.Vector St.RootOut)
  | Nodes (Vc.Vector St.NodeOut)
  deriving (Show)


showListResults :: ListResult -> IO ()
showListResults items =
  case items of
  Triplet items -> putStrLn $ "@[listCmd] items: " <> show items
  Taxo items -> mapM_ print items
  Nodes items -> do
    -- id::int4, label::text, parentid::int4?, assetid::int4?, lastmod::timestamptz?, depth::int4
    startTime <- getCurrentTime
    let
      -- (!tree, !dbgInfo) = Tl.treeFromNodes items
      tree = Tl.buildForestMapV items
      !nbrTreeNodes = Tl.sizeTree tree
    midTime <- getCurrentTime
    Tl.showTree items tree Tl.defaultDebugInfo
    endTime <- getCurrentTime
    putStrLn $ "@[showListResults] treeFromNodes time: " <> show (diffUTCTime midTime startTime) <> ", showTree time: " <> show (diffUTCTime endTime midTime)



data SNode =
  RootTN St.NodeOut
  | NodeTN St.NodeOut
  | LeafTN St.NodeOut
  deriving (Show)

data TNode =
  TNode Int32 Text (Mp.Map Int32 TNode)
  -- id::int4, label::text, parentid::int4?, assetid::int4?, lastmod::timestamptz?, depth::int4
  | TLeaf Int32 Text Int32 (Maybe UTCTime)


mapNodesToTNodes :: Vc.Vector St.NodeOut -> Mp.Map Int32 TNode
mapNodesToTNodes = Vc.foldl' (\acc node@(id, label, parentID, assetID, lastMod, depth) -> case parentID of
          Nothing -> Mp.insert id (TNode id label Mp.empty) acc
          Just parentID -> case assetID of
            Nothing -> Mp.insert id (TNode id label Mp.empty) acc
            Just assetID -> Mp.insert id (TLeaf id label parentID lastMod) acc
  ) Mp.empty


mapNodesToSNodes :: Vc.Vector St.NodeOut -> Vc.Vector SNode
mapNodesToSNodes = Vc.map (\node@(id, label, parentID, assetID, lastMod, depth) -> case parentID of
          Nothing -> RootTN node
          Just parentID -> case assetID of
            Nothing -> NodeTN node
            Just assetID -> LeafTN node
  )
