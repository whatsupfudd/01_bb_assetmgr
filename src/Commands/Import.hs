
module Commands.Import where

import Control.Monad.Cont (runContT)
import Control.Monad (when)
import qualified Control.Concurrent as C 

import Data.Bifunctor (first)
import Data.Bits (testBit)
import qualified Data.Sequence as Seq
import qualified Data.Text as T

import System.Directory (getCurrentDirectory)
import System.FilePath ((</>), splitDirectories)

import DB.Connect (startPg)
import qualified Options.Runtime as Rto
import qualified Storage.Explore as Xpl
import Storage.S3 (makeS3Conn)
import qualified Logic.Ingestion as Ing

import Options.Cli (ImportOpts (..))


importCmd :: ImportOpts -> Rto.RunOptions -> IO ()
importCmd importOpts rtOpts = do
  when (testBit rtOpts.debug 0) $
    putStrLn $ "@[importCmd] starting, opts: " <> show rtOpts
  case rtOpts.s3store of
    Nothing -> pure ()
    Just s3Conf ->
      let
        s3Conn = makeS3Conn s3Conf
      in
      runContT (startPg rtOpts.pgDbConf) (mainAction s3Conn)
  where
  mainAction s3Conn dbPool = do
    let
      mbAnchorPath = case importOpts.anchor of
        "" -> Nothing
        aValue -> Just aValue
    srcDir <- if T.isPrefixOf "/" (T.pack importOpts.path) then
        pure importOpts.path
      else
        case rtOpts.root of
          Nothing -> do
            cwd <- getCurrentDirectory
            pure $ cwd <> "/" <> importOpts.path
          Just root -> pure $ root <> "/" <> importOpts.path
    fsContent <- Xpl.loadFolderTree srcDir
    -- putStrLn $ "@[importCmd] fsContent: " <> show fsContent
    nbrCores <- C.getNumCapabilities
    let workerCount = max 1 nbrCores
    eiTree <- Xpl.resolveFsContent workerCount srcDir fsContent
    case eiTree of
      Left errs -> putStrLn $ "@[importCmd] resolveFsContent errs: " <> show errs <> "."
      Right tree -> do
        -- putStrLn $ "@[importCmd] tree: " <> show tree
        let
          updTree = if importOpts.noRecurse then
            Seq.take 1 tree
          else
            tree
          rebasedTree =
            let
              rebasingAnchor = case mbAnchorPath of
                Nothing -> last $ splitDirectories srcDir
                Just anchorPath -> anchorPath
            in
            first (\dirInfo -> 
                let
                  updPath = take dirInfo.rootLengthDI dirInfo.lpathDI </> rebasingAnchor </> drop dirInfo.rootLengthDI dirInfo.lpathDI
                in
                (dirInfo { Xpl.lpathDI = updPath, Xpl.insetLengthDI = Just (length rebasingAnchor) })
                ) <$> updTree

        -- DBG:
        when (testBit rtOpts.debug 1) $ 
          let
            parsing = Xpl.parseTree updTree
          in do
          putStrLn $ "@[importCmd] tree-def: " <> show parsing <> " folder/files."
          putStrLn $ "@[importCmd] root: " <> srcDir
          -- putStrLn $ "@[importCmd] tree: " <> show rebasedTree
          Xpl.showTree rebasedTree
        when (testBit rtOpts.debug 2) $
          let
            parsing = Xpl.parseTree updTree
          in do
          putStrLn $ "@[importCmd] tree-def: " <> show parsing <> " folder/files."

        Ing.loadFileTree dbPool s3Conn rtOpts importOpts.taxonomy mbAnchorPath rebasedTree


