module Commands.IngestClass where

import Control.Exception (SomeException (..), try)
import Data.Maybe (isNothing, fromJust)
import Data.Text (Text, unpack, pack)
import Text.Read (readMaybe)
import Data.Text.Encoding (decodeUtf8)
import qualified Data.ByteString as BS
import qualified Options.Runtime as Rto


ingestCmd :: FilePath -> Rto.RunOptions -> IO ()
ingestCmd inFile rtOpts = do
  -- TODO: put in a try block
  rawContent <- BS.readFile inFile
  let
    lines = BS.split 10 rawContent
    (_, result) = processLines lines
  mapM_ showResult result


showResult :: Either String ImageFeatures -> IO ()
showResult eiRez =
  case eiRez of
    Right aValue ->
      case aValue.features of
        [] -> pure ()
        _ -> do
          putStrLn $ "Img: " <> aValue.filePath
          mapM_ showFeature aValue.features
    Left errMsg ->
      putStrLn $ "@[ingestCmd] got an error: " <> errMsg
  where
    showFeature :: Feature -> IO ()
    showFeature aFeature =
      putStrLn $ unpack aFeature.fClass <> " @ " <> show aFeature.confidence


data ImageFeatures = ImageFeatures {
    filePath :: FilePath
    , features :: [ Feature ]
  }
  deriving (Show)


data Feature = Feature {
    fClass :: Text
    , confidence :: Float
    , startCorner :: (Int, Int)
    , size :: (Int, Int)
  }
  deriving (Show)


processLines lines =
  Prelude.foldl processALine (Right Nothing, []) (zip lines [1..])

processALine :: (Either String (Maybe ImageFeatures), [ Either String ImageFeatures]) -> (BS.ByteString, Int) -> (Either String (Maybe ImageFeatures), [ Either String ImageFeatures])
processALine (eiCurr, accum) (line, pos) =
  case eiCurr of
    Left errMsg ->
      if BS.isPrefixOf "<<<" line then
        (Right Nothing, Left errMsg : accum)
      else
        (eiCurr, accum)
    Right curr ->
      if BS.isPrefixOf ">>>" line then
        case curr of
          Nothing -> (Right Nothing, accum)
          -- TODO: raise error.
          Just i -> (Right Nothing, Right i : accum)
      else if BS.isPrefixOf "f: " line then
        case curr of
          Nothing -> 
            let
              path = unpack . decodeUtf8 $ BS.drop 3 line
              newImg = ImageFeatures { filePath = path, features = [] }
            in
            (Right $ Just newImg, accum)
          Just _ -> 
            (Left $ "Additinal file entry, l: " <> show pos <> ".", accum)
      else if BS.isPrefixOf "<<<" line then
        case curr of
          Nothing ->
            (Left $ "Out of order image ending, l: " <> show pos <> ".", accum)
          Just i -> (Right Nothing, Right i : accum)
      else if BS.isPrefixOf "objs: " line then
        case curr of
          Nothing ->
            (Left $ "Out of order class info, l: " <> show pos <> ".", accum)
          Just i ->
            let
              info = BS.drop 6 line
            in if info == "<nothing>" then
                -- TODO: confirm there's nothing else to do when hitting a no-op.
                (eiCurr, accum)
              else if info == "<skip>" then
                -- TODO: confirm there's nothing else to do when hitting a no-op.
                (eiCurr, accum)
              else
                let
                  -- Check ':' => ascii 58
                  (lClass, rest) = BS.break (== 58) info
                  -- Split on ',' => ascii 44
                  values = BS.split 44 (BS.drop 2 rest)
                in if length values /= 5 then
                  (Left $ "Invalid number of items: " <> unpack (decodeUtf8 rest) <> ", l: " <> show pos <> ".", accum)
                else
                  let
                    mbConf = readMaybe . unpack . decodeUtf8 $ values !! 0 :: Maybe Float
                    -- need to drop the leading parenthesis.
                    mbPx = readMaybe . unpack . decodeUtf8 $ BS.drop 2 (values !! 1) :: Maybe Int
                    mbPy = readMaybe . unpack . decodeUtf8 $ values !! 2 :: Maybe Int
                    mbSx = readMaybe . unpack . decodeUtf8 $ values !! 3 :: Maybe Int
                    -- need to drop the trailing parenthesis.
                    mbSy = readMaybe . unpack . decodeUtf8 $ BS.dropEnd 1 (values !! 4) :: Maybe Int
                  in if isNothing mbConf || isNothing mbPx || isNothing mbPy || isNothing mbSx || isNothing mbSy then
                      (Left $ "Invalid values: " <> show values <> ", l: " <> show pos <> ".", accum)
                  else
                    let
                      feature = Feature { fClass = decodeUtf8 lClass, confidence = fromJust mbConf, startCorner = (fromJust mbPx, fromJust mbPy), size = (fromJust mbSx, fromJust mbSy)}
                      newFeatures = i.features <> [ feature ]
                      newImg = Just $ i { features = newFeatures }
                    in
                    (Right newImg, accum)
      else
        -- Ignore:
        (eiCurr, accum)

