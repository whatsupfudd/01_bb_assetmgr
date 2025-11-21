module Commands.Test where

import Data.Text (Text, unpack, pack)
import qualified Data.Vector as Vc
import Numeric (showHex)
import qualified Options.Runtime as Rto
import Filing.S3 (makeS3Conn, listFilesWith, listFiles, getFile, getFileB)

type TestParams = (Text)

testCmd :: TestParams -> Rto.RunOptions -> IO ()
testCmd (subCmd) rtOpts =
  let
    s3Conn = makeS3Conn rtOpts.s3opts
  in
  fetchObject s3Conn subCmd


fetchFile s3conn locator = do
  rezA <- getFile s3conn locator "/tmp/gaga.out"
  case rezA of
    Left err -> putStrLn $ "@[fetchFile] getFile err: " <> show err <> "."
    Right _ -> putStrLn $ "@[fetchFile] done."


fetchObject s3conn locator = do
  rezA <- getFileB s3conn locator "/tmp/gaga.out"
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
    rezA <- listFilesWith s3conn (Vc.fromList prefixes)
    case rezA of
      Left err -> putStrLn $ "@[testCmd] listFilesWith err: " <> show err <> "."
      Right aSum -> putStrLn $ "@[testCmd] total: " <> show aSum
  where
  padHex n =
    pack $ if n < 16 then "0" <> showHex n "" else showHex n ""


-- mapM_ (showSubDir s3Conn) prefixes
showDirectory s3conn subCmd p =
  let
    prefix = subCmd <> p
  in do
    rezA <- listFiles s3conn (Just prefix)
    case rezA of
      Left err ->
        putStrLn $ "@[testCmd] listFiles err: " <> show err
      Right aRez ->
        putStrLn $ "@[testCmd] list of " <> (unpack prefix) <> " length: " <> (show . length $ aRez)
