module Tree.Types where

import qualified Data.Map.Strict as Mp
import Data.Int (Int32)

import qualified DB.Statements as Op

type FolderMap element = Mp.Map Int32 (FolderTree element)

data FolderTree element =
  FolderFT (FDetails element)
  | LeafFT element
  | EmptyFT
  deriving (Show)


data FDetails element = FDetails { node :: !element, children :: !(FolderMap element) }
  deriving (Show)


data DebugInfo element = DebugInfo {
    leftOver :: [ FolderTree element ]
    , p1Errs :: [ FolderTree element ]
    , p2Errs :: [ FolderTree element ]
    , lenLeaves :: Int
    , mp2Size :: Int
  }
