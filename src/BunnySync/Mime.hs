{-# LANGUAGE OverloadedStrings #-}

module BunnySync.Mime
  ( AssetMime (..)
  , detectAssetMime
  , guessAssetMime
  , mimeFromExtension
  , extensionForMime
  , ensureExtension
  ) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as B8
import Data.Char (isSpace)
import Data.Maybe (fromMaybe, listToMaybe, mapMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.Encoding.Error as Ter
import System.FilePath (takeExtension)


data AssetMime = AssetMime
  { mimeType         :: Text
  , defaultExtension :: Text
  } deriving (Show, Eq)


-- | Prefer name-based detection when the DB/taxonomy has the original filename.
-- Fall back to content sniffing from the temporary downloaded file.
detectAssetMime :: Maybe Text -> Text -> FilePath -> IO AssetMime
detectAssetMime mbNameHint sourceKey localPath =
  case guessAssetMime mbNameHint sourceKey of
    Just detected ->
      pure detected

    Nothing -> do
      header <- BS.readFile localPath
      pure $ fromMaybe octetStream (mimeFromBytes header)


-- | Guess from original DB node label first, then from the storage locator.
guessAssetMime :: Maybe Text -> Text -> Maybe AssetMime
guessAssetMime mbNameHint sourceKey =
  listToMaybe $
    mapMaybe mimeFromPathText [fromMaybe "" mbNameHint, sourceKey]


mimeFromPathText :: Text -> Maybe AssetMime
mimeFromPathText txt =
  let ext = T.toLower . T.pack . takeExtension . T.unpack $ txt
  in mimeFromExtension ext


mimeFromExtension :: Text -> Maybe AssetMime
mimeFromExtension ext =
  case T.toLower ext of
    ".avif" -> Just $ AssetMime "image/avif" "avif"
    ".bmp"  -> Just $ AssetMime "image/bmp" "bmp"
    ".gif"  -> Just $ AssetMime "image/gif" "gif"
    ".heic" -> Just $ AssetMime "image/heic" "heic"
    ".heif" -> Just $ AssetMime "image/heif" "heif"
    ".jpeg" -> Just $ AssetMime "image/jpeg" "jpg"
    ".jpg"  -> Just $ AssetMime "image/jpeg" "jpg"
    ".png"  -> Just $ AssetMime "image/png" "png"
    ".svg"  -> Just $ AssetMime "image/svg+xml" "svg"
    ".tif"  -> Just $ AssetMime "image/tiff" "tiff"
    ".tiff" -> Just $ AssetMime "image/tiff" "tiff"
    ".webp" -> Just $ AssetMime "image/webp" "webp"

    ".css"  -> Just $ AssetMime "text/css" "css"
    ".csv"  -> Just $ AssetMime "text/csv" "csv"
    ".htm"  -> Just $ AssetMime "text/html" "html"
    ".html" -> Just $ AssetMime "text/html" "html"
    ".txt"  -> Just $ AssetMime "text/plain" "txt"

    ".js"   -> Just $ AssetMime "application/javascript" "js"
    ".json" -> Just $ AssetMime "application/json" "json"
    ".pdf"  -> Just $ AssetMime "application/pdf" "pdf"
    ".wasm" -> Just $ AssetMime "application/wasm" "wasm"
    ".xml"  -> Just $ AssetMime "application/xml" "xml"
    ".zip"  -> Just $ AssetMime "application/zip" "zip"

    ".glb"  -> Just $ AssetMime "model/gltf-binary" "glb"
    ".gltf" -> Just $ AssetMime "model/gltf+json" "gltf"

    ".m4v"  -> Just $ AssetMime "video/x-m4v" "m4v"
    ".mp4"  -> Just $ AssetMime "video/mp4" "mp4"
    ".webm" -> Just $ AssetMime "video/webm" "webm"

    _       -> Nothing


mimeFromBytes :: BS.ByteString -> Maybe AssetMime
mimeFromBytes bs
  | BS.take 3 bs == BS.pack [0xFF, 0xD8, 0xFF] =
      Just $ AssetMime "image/jpeg" "jpg"

  | BS.take 8 bs == BS.pack [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A] =
      Just $ AssetMime "image/png" "png"

  | BS.take 6 bs == B8.pack "GIF87a" || BS.take 6 bs == B8.pack "GIF89a" =
      Just $ AssetMime "image/gif" "gif"

  | BS.take 2 bs == B8.pack "BM" =
      Just $ AssetMime "image/bmp" "bmp"

  | isWebp bs =
      Just $ AssetMime "image/webp" "webp"

  | isAvif bs =
      Just $ AssetMime "image/avif" "avif"

  | isHeic bs =
      Just $ AssetMime "image/heic" "heic"

  | isTiff bs =
      Just $ AssetMime "image/tiff" "tiff"

  | looksLikeSvg bs =
      Just $ AssetMime "image/svg+xml" "svg"

  | BS.take 5 bs == B8.pack "%PDF-" =
      Just $ AssetMime "application/pdf" "pdf"

  | BS.take 4 bs == B8.pack "glTF" =
      Just $ AssetMime "model/gltf-binary" "glb"

  | isMp4 bs =
      Just $ AssetMime "video/mp4" "mp4"

  | looksLikeJson bs =
      Just $ AssetMime "application/json" "json"

  | looksLikeHtml bs =
      Just $ AssetMime "text/html" "html"

  | otherwise =
      Nothing


isWebp :: BS.ByteString -> Bool
isWebp bs =
  BS.take 4 bs == B8.pack "RIFF"
    && BS.take 4 (BS.drop 8 bs) == B8.pack "WEBP"


isAvif :: BS.ByteString -> Bool
isAvif bs =
  hasFtypBrand ["avif", "avis"] bs


isHeic :: BS.ByteString -> Bool
isHeic bs =
  hasFtypBrand ["heic", "heix", "hevc", "hevx", "mif1", "msf1"] bs


isMp4 :: BS.ByteString -> Bool
isMp4 bs =
  hasFtypBrand ["isom", "iso2", "mp41", "mp42", "avc1"] bs


hasFtypBrand :: [BS.ByteString] -> BS.ByteString -> Bool
hasFtypBrand brands bs =
  BS.take 4 (BS.drop 4 bs) == B8.pack "ftyp"
    && any (`BS.isInfixOf` BS.take 32 bs) brands


isTiff :: BS.ByteString -> Bool
isTiff bs =
  BS.take 4 bs == BS.pack [0x49, 0x49, 0x2A, 0x00]
    || BS.take 4 bs == BS.pack [0x4D, 0x4D, 0x00, 0x2A]


looksLikeSvg :: BS.ByteString -> Bool
looksLikeSvg bs =
  let sample = T.toLower . T.stripStart . TE.decodeUtf8With Ter.lenientDecode $ BS.take 512 bs
  in "<svg" `T.isPrefixOf` sample || "<!doctype svg" `T.isPrefixOf` sample || "<\65279svg" `T.isPrefixOf` sample


looksLikeJson :: BS.ByteString -> Bool
looksLikeJson bs =
  case firstNonSpace bs of
    Just 0x7B -> True -- {
    Just 0x5B -> True -- [
    _        -> False


looksLikeHtml :: BS.ByteString -> Bool
looksLikeHtml bs =
  let sample = T.toLower . T.stripStart . TE.decodeUtf8With Ter.lenientDecode $ BS.take 512 bs
  in any (`T.isPrefixOf` sample)
      [ "<!doctype html"
      , "<html"
      ]


firstNonSpace :: BS.ByteString -> Maybe Word
firstNonSpace =
  fmap fromIntegral
    . BS.find (not . isSpace . toEnum . fromIntegral)


extensionForMime :: Text -> Text
extensionForMime mt =
  defaultExtension $
    fromMaybe octetStream $
      case T.toLower mt of
        "image/avif"              -> Just $ AssetMime mt "avif"
        "image/bmp"               -> Just $ AssetMime mt "bmp"
        "image/gif"               -> Just $ AssetMime mt "gif"
        "image/heic"              -> Just $ AssetMime mt "heic"
        "image/heif"              -> Just $ AssetMime mt "heif"
        "image/jpeg"              -> Just $ AssetMime mt "jpg"
        "image/png"               -> Just $ AssetMime mt "png"
        "image/svg+xml"           -> Just $ AssetMime mt "svg"
        "image/tiff"              -> Just $ AssetMime mt "tiff"
        "image/webp"              -> Just $ AssetMime mt "webp"
        "application/pdf"         -> Just $ AssetMime mt "pdf"
        "model/gltf-binary"       -> Just $ AssetMime mt "glb"
        "model/gltf+json"         -> Just $ AssetMime mt "gltf"
        "video/mp4"               -> Just $ AssetMime mt "mp4"
        "video/webm"              -> Just $ AssetMime mt "webm"
        "application/json"        -> Just $ AssetMime mt "json"
        "application/javascript"  -> Just $ AssetMime mt "js"
        "text/css"                -> Just $ AssetMime mt "css"
        "text/html"               -> Just $ AssetMime mt "html"
        "text/plain"              -> Just $ AssetMime mt "txt"
        _                         -> Nothing


ensureExtension :: AssetMime -> Text -> Text
ensureExtension assetMime key =
  let
    currentExt =
      T.toLower . T.pack . takeExtension . T.unpack $ key

    wantedExt =
      "." <> defaultExtension assetMime
  in
    case mimeFromExtension currentExt of
      Just known
        | mimeType known == mimeType assetMime ->
            key

      _ ->
        key <> wantedExt


octetStream :: AssetMime
octetStream =
  AssetMime "application/octet-stream" "bin"