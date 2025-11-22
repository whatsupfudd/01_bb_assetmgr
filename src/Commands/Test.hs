module Commands.Test where

import Data.Maybe (fromMaybe)
import Data.Text (Text, unpack, pack)
import qualified Data.Vector as Vc
import Numeric (showHex)
import qualified Options.Runtime as Rto
import qualified Storage.S3 as S3
import qualified Storage.Types as S3

type TestParams = (Text)

testCmd :: TestParams -> Rto.RunOptions -> IO ()
testCmd subCmd rtOpts =
  case rtOpts.s3store of
    Nothing -> putStrLn "@[testCmd] no s3store configured."
    Just s3store ->
      let
        s3Conn = S3.makeS3Conn s3store
      in do
      putStrLn $ "@[testCmd] s3store: " <> show s3store
      fetchObject s3Conn subCmd
      -- showPathList s3Conn subCmd
      -- showDirectory s3Conn subCmd Nothing



fetchFile s3conn locator = do
  rezA <- S3.getFile s3conn locator "/tmp/gaga.out"
  case rezA of
    Left err -> putStrLn $ "@[fetchFile] getFile err: " <> show err <> "."
    Right _ -> putStrLn $ "@[fetchFile] done."


fetchObject s3conn locator = do
  putStrLn $ "@[fetchObject] fetching object: " <> unpack locator
  rezA <- S3.getFileB s3conn locator "/tmp/gaga.out"
  case rezA of
    Left err -> putStrLn $ "@[fetchFile] getFile err: " <> show err <> "."
    Right _ -> putStrLn $ "@[fetchFile] done."


showPathList s3conn subCmd =
  let
    maxNbr = read . unpack $ subCmd
    pref1 = map padHex [0..maxNbr]
    pref2 = map padHex [0..15]
    prefixes = concatMap (\tl -> zipWith (\a b -> a <> "/" <> b) (repeat tl) pref2) pref1
  in do
    putStrLn $ "@[testCmd] prefixes: " <> show prefixes
    rezA <- S3.listFilesWith s3conn (Vc.fromList prefixes)
    case rezA of
      Left err -> putStrLn $ "@[testCmd] listFilesWith err: " <> show err <> "."
      Right aSum -> putStrLn $ "@[testCmd] total: " <> show aSum
  where
  padHex n =
    pack $ if n < 16 then "0" <> showHex n "" else showHex n ""



-- mapM_ (showSubDir s3Conn) prefixes
showDirectory :: S3.S3Conn -> Text -> Maybe Text -> IO ()
showDirectory s3conn subCmd mbPrefix=
  let
    prefix = fromMaybe subCmd mbPrefix
  in do
    rezA <- S3.listFiles s3conn (Just prefix)
    case rezA of
      Left err ->
        putStrLn $ "@[testCmd] listFiles err: " <> show err
      Right aRez -> do
        putStrLn $ "@[testCmd] list of " <> unpack prefix <> " length: " <> (show . length $ aRez)
        putStrLn $ "@[testCmd] list: " <> show aRez
