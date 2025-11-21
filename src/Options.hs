module Options  (
  module Options.Cli
  , module Options.ConfFile
  , module R
  , mergeOptions
 )
where

import qualified Data.Text as DT
import qualified Data.Text.Encoding as DT

import qualified DB.Connect as D
import qualified Filing.S3 as S

import Options.Cli
import Options.ConfFile
import qualified Options.Runtime as R


mergeOptions :: CliOptions -> FileOptions -> EnvOptions -> R.RunOptions
mergeOptions cli file env =
  -- TODO: put proper priority filling of values for the Runtime Options.
  let
    defO = R.defaultRun
    -- Update from config file:
    fileO =
      let
        dbgO = case file.debug of
          Nothing -> defO
          Just aVal -> defO { R.debug = aVal }
        dbO = case file.db of
            Nothing -> dbgO
            Just aVal ->
              let
                portO = case aVal.port of
                  Nothing -> dbgO.db
                  Just anInt -> dbgO.db { D.port = fromIntegral anInt }
                hostO = case aVal.host of
                  Nothing -> portO
                  Just aStr -> portO { D.host = DT.encodeUtf8 . DT.pack $ aStr }
                userO = case aVal.user of
                  Nothing -> hostO
                  Just aStr -> hostO { D.user = DT.encodeUtf8 . DT.pack $ aStr }
                pwdO =  case aVal.passwd of
                  Nothing -> userO
                  Just aStr -> userO { D.passwd = DT.encodeUtf8 . DT.pack $ aStr }
                dbaseO =  case aVal.dbase of
                  Nothing -> pwdO
                  Just aStr -> pwdO { D.dbase = DT.encodeUtf8 . DT.pack $ aStr }
              in
              dbgO { R.db = dbaseO }
        rootO = case file.rootDir of
          Nothing -> dbO
          Just aVal -> dbO { R.root = DT.pack aVal }
        ownerO = case file.owner of
          Nothing -> rootO
          Just aVal -> rootO { R.owner = DT.pack aVal }
        s3O = case file.s3 of
          Nothing -> ownerO
          Just aVal ->
            let
              s3uO = case aVal.user of
                Nothing -> ownerO.s3opts
                Just aStr -> ownerO.s3opts { S.user = aStr }
              s3pO = case aVal.passwd of
                Nothing -> s3uO
                Just aStr -> s3uO { S.passwd = aStr }
              s3hO = case aVal.host of
                Nothing -> s3pO
                Just aStr -> s3pO { S.host = aStr }
              s3bO = case aVal.bucket of
                Nothing -> s3hO
                Just aStr -> s3hO { S.bucket = aStr } :: S.S3Config
            in
            ownerO { R.s3opts = s3bO }
      in
      s3O
    -- TODO: update from CLI options
    cliO = case cli.debug of
      Nothing -> fileO
      Just aVal -> fileO { R.debug = aVal }
    -- TODO: update from ENV options
    envO = cliO
  in
  envO
