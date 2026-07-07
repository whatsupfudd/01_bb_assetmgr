module BunnySync.Http
  ( BunnyConfig (..)
  , BunnyConn (..)
  , BunnyObjectInfo (..)
  , makeBunnyConn
  , putFile
  , listDirectory
  , objectExistsWithSize
  , bunnyCdnUrl
  ) where

import qualified Data.Aeson as Aes
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as B8
import Data.Char (isAsciiLower, isAsciiUpper, isDigit)
import Data.Int (Int64)
import Data.List (find)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Network.HTTP.Client
  ( RequestBody (..)
  , httpLbs
  , httpNoBody
  , method
  , parseRequest
  , requestBody
  , requestHeaders
  , responseBody
  , responseStatus
  )
import qualified Network.HTTP.Client as Http
import Network.HTTP.Client.TLS (tlsManagerSettings)
import Network.HTTP.Types.Status (statusCode, statusMessage)
import Data.Word (Word8)
import BunnySync.Types (BunnyConfig (..), BunnyConn (..), BunnyObjectInfo (..))


makeBunnyConn :: BunnyConfig -> IO BunnyConn
makeBunnyConn conf = do
  mgr <- Http.newManager tlsManagerSettings
  pure BunnyConn { configBc = conf, connBc = mgr }


putFile :: BunnyConn -> Text -> Maybe Text -> FilePath -> IO (Either String ())
putFile conn destKey mbContentType sourceFile = do
  req0 <- parseRequest . T.unpack $ storageUrl conn.configBc destKey False
  fileContents <- BS.readFile sourceFile
  let
    authHeader = ("AccessKey", T.encodeUtf8 conn.configBc.accessKey)
    contentTypeHeaders = case mbContentType of
      Nothing -> []
      Just ct -> [("Content-Type", T.encodeUtf8 ct)]
    req = req0
      { method = "PUT"
      , requestHeaders = authHeader : contentTypeHeaders
      , requestBody = RequestBodyBS fileContents
      }

  -- putStrLn $ "@[putFile] req: " <> show req
  response <- httpNoBody req conn.connBc
  -- putStrLn $ "@[putFile] response: " <> show response
  let
    st = responseStatus response
    code = statusCode st
  if code >= 200 && code < 300 then
    pure $ Right ()
  else
    pure . Left $ "Bunny PUT failed for " <> T.unpack destKey
      <> ": HTTP " <> show code <> " " <> B8.unpack (statusMessage st)


listDirectory :: BunnyConn -> Text -> IO (Either String [BunnyObjectInfo])
listDirectory conn directory = do
  req0 <- parseRequest . T.unpack $ storageUrl conn.configBc directory True
  let req = req0 { requestHeaders = [("AccessKey", T.encodeUtf8 conn.configBc.accessKey)] }
  -- putStrLn $ "@[listDirectory] req: " <> show req
  response <- httpLbs req conn.connBc
  -- putStrLn $ "@[listDirectory] response: " <> show response
  let st = responseStatus response
      code = statusCode st
  if code >= 200 && code < 300
    then pure $ case Aes.eitherDecode' (responseBody response) of
      Left err -> Left $ "Bunny list JSON decode failed for " <> T.unpack directory <> ": " <> err
      Right items -> Right items
    else pure . Left $ "Bunny list failed for " <> T.unpack directory
      <> ": HTTP " <> show code <> " " <> B8.unpack (statusMessage st)


objectExistsWithSize :: BunnyConn -> Text -> Maybe Int64 -> IO (Either String Bool)
objectExistsWithSize conn destKey mbExpectedSize = do
  let
    (parentDir, objectBaseName) = splitBunnyPath destKey
  eiItems <- listDirectory conn parentDir
  pure $ do
    items <- eiItems
    let
      mbItem = find (\item -> not item.isDirectory && item.name == objectBaseName) items
    pure $ case mbItem of
      Nothing -> False
      Just item -> case mbExpectedSize of
        Nothing -> True
        Just expectedSize -> item.size == expectedSize


bunnyCdnUrl :: BunnyConfig -> Text -> Maybe Text
bunnyCdnUrl conf key = do
  base <- conf.pullZoneUrl
  pure $ trimRightSlash base <> "/" <> encodePath key

storageUrl :: BunnyConfig -> Text -> Bool -> Text
storageUrl conf key asDirectory =
  let base = trimRightSlash conf.endpoint
      path = encodePath $ joinPathText [conf.zoneName, key]
      url = base <> "/" <> path
  in if asDirectory then ensureTrailingSlash url else url

splitBunnyPath :: Text -> (Text, Text)
splitBunnyPath rawPath =
  let parts = filter (not . T.null) $ T.splitOn "/" rawPath
  in case reverse parts of
    [] -> ("", "")
    name : revParents -> (T.intercalate "/" . reverse $ revParents, name)

joinPathText :: [Text] -> Text
joinPathText = T.intercalate "/" . filter (not . T.null) . map trimSlashes

trimSlashes :: Text -> Text
trimSlashes = T.dropAround (== '/')

trimRightSlash :: Text -> Text
trimRightSlash = T.dropWhileEnd (== '/')

ensureTrailingSlash :: Text -> Text
ensureTrailingSlash txt
  | T.isSuffixOf "/" txt = txt
  | otherwise = txt <> "/"

encodePath :: Text -> Text
encodePath pathTxt =
  T.intercalate "/" $ map encodeSegment $ filter (not . T.null) $ T.splitOn "/" pathTxt

encodeSegment :: Text -> Text
encodeSegment = T.concatMap encodeChar

encodeChar :: Char -> Text
encodeChar c
  | isUnreserved c = T.singleton c
  | otherwise = T.pack . concatMap pctEncode . BS.unpack $ T.encodeUtf8 (T.singleton c)

isUnreserved :: Char -> Bool
isUnreserved c =
  isAsciiUpper c || isAsciiLower c || isDigit c || c `elem` ['-', '.', '_', '~']

pctEncode :: Word8 -> String
pctEncode w =
  let digits = "0123456789ABCDEF"
      n = fromEnum w
      hi = n `div` 16
      lo = n `mod` 16
  in ['%', digits !! hi, digits !! lo]
