module Commands.List where

import Control.Monad.Cont (runContT)

import Data.Int (Int32)
import Data.Time (UTCTime)
import qualified Data.Map.Strict as Mp
import Data.Maybe (fromMaybe)
import Data.Text (Text, unpack, pack)
import qualified Data.Vector as Vc
import Numeric (showHex)

import Hasql.Pool (Pool, use)

import qualified DB.Connect as Db
import qualified DB.Opers as Do
import qualified Options.Runtime as Rto

import Options.Cli (ListOpts (..))
import DB.Opers (NodeOut)
import qualified Commands.Import as Ci

data ListResult =
  Triplet (Vc.Vector Do.TaxoLabelOut)
  | Taxo (Vc.Vector Do.RootOut)
  | Nodes (Vc.Vector Do.NodeOut)
  deriving (Show)


listCmd :: ListOpts -> Rto.RunOptions -> IO ()
listCmd params rtOpts = do
  runContT (Db.startPg rtOpts.pgDbConf) mainAction
  where
  mainAction dbPool = do
    eiRezA <- case params.owner of
      "" -> do
        rezC <- use dbPool Do.fetchTaxos
        case rezC of
          Left err -> pure . Left $ "@[listCmd] fetchTaxos err: " <> show err
          Right items -> pure . Right . Triplet $ items
      _ ->
        case params.taxonomy of
          "" -> do
            rezB <- use dbPool $ Do.fetchOwnerByName params.owner
            case rezB of
              Left err -> pure . Left $ "@[listCmd] fetchOwnerByName err: " <> show err
              Right mbOwnerID -> case mbOwnerID of
                  Nothing -> pure . Left $ "@[listCmd] fetchOwnerByName: owner not found."
                  Just ownerID -> do
                    rezD <- use dbPool $ Do.fetchTaxosForOwner ownerID
                    case rezD of
                      Left err -> pure . Left $ "@[listCmd] fetchTaxosForOwner err: " <> show err
                      Right items -> pure . Right . Triplet $ items
          _ -> do
            rezE <- use dbPool $ Do.fetchTaxosForOwnerLabel (params.owner, params.taxonomy)
            case rezE of
              Left err -> pure . Left $ "@[listCmd] fetchTaxosForOwnerLabel err: " <> show err
              Right mbTaxoID -> case mbTaxoID of
                Nothing -> pure . Left $ "@[listCmd] fetchTaxosForOwnerLabel: taxonomy not found."
                Just taxoID -> do
                  case params.maxDepth of
                    0 -> do
                      rezF <- use dbPool $ Do.fetchRootsForTaxo taxoID
                      case rezF of
                        Left err -> pure . Left $ "@[listCmd] fetchRootsForTaxo err: " <> show err
                        Right items -> pure . Right . Taxo $ items
                    _ -> do
                      rezG <- use dbPool $ Do.fetchNodesForTaxoLimited taxoID params.maxDepth
                      case rezG of
                        Left err -> pure . Left $ "@[listCmd] fetchNodesForTaxoLimited err: " <> show err
                        Right items -> pure . Right . Nodes $ items

    case eiRezA of
      Left err -> putStrLn $ "@[listCmd] fetchTaxos err: " <> show err
      Right items -> case items of
        Triplet items -> putStrLn $ "@[listCmd] items: " <> show items
        Taxo items -> mapM_ print items
        Nodes items -> do
          -- id::int4, label::text, parentid::int4?, assetid::int4?, lastmod::timestamptz?, depth::int4
          putStrLn $ "@[listCmd] items: id\tlabel\tparentID\tassetID\tlastMod\tdepth"
          let
            (tree, dbgInfo) = Ci.treeFromNodes items
          Ci.showTree items tree dbgInfo
          -- mapM_ (\(id, label, parentID, assetID, lastMod, depth) -> putStrLn $ show id <> "\t" <> show label <> "\t" <> show parentID <> "\t" <> show assetID <> "\t" <> show lastMod <> "\t" <> show depth) items
    pure ()


data SNode =
  RootTN NodeOut
  | NodeTN NodeOut
  | LeafTN NodeOut
  deriving (Show)

data TNode =
  TNode Int32 Text (Mp.Map Int32 TNode)
  -- id::int4, label::text, parentid::int4?, assetid::int4?, lastmod::timestamptz?, depth::int4
  | TLeaf Int32 Text Int32 (Maybe UTCTime)


mapNodesToTNodes :: Vc.Vector Do.NodeOut -> Mp.Map Int32 TNode
mapNodesToTNodes = Vc.foldl' (\acc node@(id, label, parentID, assetID, lastMod, depth) -> case parentID of
          Nothing -> Mp.insert id (TNode id label Mp.empty) acc
          Just parentID -> case assetID of
            Nothing -> Mp.insert id (TNode id label Mp.empty) acc
            Just assetID -> Mp.insert id (TLeaf id label parentID lastMod) acc
  ) Mp.empty


mapNodesToSNodes :: Vc.Vector Do.NodeOut -> Vc.Vector SNode
mapNodesToSNodes = Vc.map (\node@(id, label, parentID, assetID, lastMod, depth) -> case parentID of
          Nothing -> RootTN node
          Just parentID -> case assetID of
            Nothing -> NodeTN node
            Just assetID -> LeafTN node
  )
