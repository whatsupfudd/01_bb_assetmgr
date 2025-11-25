module Options  (
  module Cl
  , module Fo
  , module Rt
  , mergeOptions
 )
where

import Control.Monad.State ( MonadState (put), MonadIO, runStateT, State, StateT, modify, lift, liftIO )
import Control.Monad.Except ( ExceptT, MonadError (throwError) )
import Data.Functor.Identity ( Identity (..) )

import Data.Foldable (for_)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

import qualified System.IO.Error as Serr
import qualified Control.Exception as Cexc
import qualified System.Posix.Env as Senv
import qualified System.Directory as Sdir


import qualified Options.Cli as Cl (CliOptions (..), EnvOptions (..))
import qualified Options.ConfFile as Fo
import qualified Storage.Types as St (S3Config (..), defaultS3Conf)
import qualified Options.Runtime as Rt (RunOptions (..), defaultRun, PgDbConfig (..), defaultPgDbConf)
import qualified DB.Connect as Db


type ConfError = Either String ()
type RunOptSt = State Rt.RunOptions ConfError
type RunOptIOSt = StateT Rt.RunOptions IO ConfError
type PgDbOptIOSt = StateT Rt.PgDbConfig (StateT Rt.RunOptions IO) ConfError
type S3OptIOSt = StateT St.S3Config (StateT Rt.RunOptions IO) ConfError


mconf :: MonadState s m => Maybe t -> (t -> s -> s) -> m ()
mconf mbOpt setter =
  case mbOpt of
    Nothing -> pure ()
    Just opt -> modify $ setter opt

innerConf :: MonadState s f => (t1 -> s -> s) -> (t2 -> StateT t1 f (Either a b)) -> t1 -> Maybe t2 -> f ()
innerConf updState innerParser defaultVal mbOpt =
  case mbOpt of
    Nothing -> pure ()
    Just anOpt -> do
      (result, updConf) <- runStateT (innerParser anOpt) defaultVal
      case result of
        Left errMsg -> pure ()
        Right _ -> modify $ updState updConf


mergeOptions :: Cl.CliOptions -> Fo.FileOptions -> Cl.EnvOptions -> IO Rt.RunOptions
mergeOptions cli file env = do
  appHome <- case T.unpack <$> env.appHome of
    Nothing -> do
      eiHomeDir <- Cexc.try Sdir.getHomeDirectory :: IO (Either Serr.IOError FilePath)
      case eiHomeDir of
        Left err -> pure ".fudd/assetmgr"
        Right aVal -> pure $ aVal <> "/.fudd/assetmgr"
    Just aVal -> pure aVal
  (result, runtimeOpts) <- runStateT (parseOptions cli file) Rt.defaultRun
  case result of
    Left errMsg -> error errMsg
    Right _ -> pure runtimeOpts
  where
  parseOptions :: Cl.CliOptions -> Fo.FileOptions -> RunOptIOSt
  parseOptions cli file = do
    mconf file.debug $ \nVal s -> s { Rt.debug = nVal }
    mconf file.rootDir $ \nVal s -> s { Rt.root = Just nVal }
    mconf file.owner $ \nVal s -> s { Rt.owner = T.pack nVal }
    mconf cli.debug $ \nVal s -> s { Rt.debug = nVal }
    innerConf (\nVal s -> s { Rt.pgDbConf = nVal }) parsePgDb Rt.defaultPgDbConf file.pgDb
    innerConf (\nVal s -> s { Rt.s3store = Just nVal }) parseS3 St.defaultS3Conf file.s3store
    pure $ Right ()


  parsePgDb :: Fo.PgDbOpts -> PgDbOptIOSt
  parsePgDb dbO = do
    mconf dbO.host $ \nVal s -> s { Rt.host = T.encodeUtf8 . T.pack $ nVal }
    mconf dbO.port $ \nVal s -> s { Rt.port = fromIntegral nVal }
    mconf dbO.user $ \nVal s -> s { Rt.user = T.encodeUtf8 . T.pack $ nVal }
    mconf dbO.passwd $ \nVal s -> s { Rt.passwd = T.encodeUtf8 . T.pack $ nVal }
    mconf dbO.dbase $ \nVal s -> s { Rt.dbase = T.encodeUtf8 . T.pack $ nVal }
    pure $ Right ()

  parseS3 :: Fo.S3Options -> S3OptIOSt
  parseS3 s3O = do
    mconf s3O.accessKey $ \nVal s -> s { St.user = nVal }
    mconf s3O.secretKey $ \nVal s -> s { St.passwd = nVal }
    mconf s3O.host $ \nVal s -> s { St.host = nVal }
    mconf s3O.region $ \nVal s -> s { St.region = nVal }
    mconf s3O.bucket $ \nVal s -> s { St.bucket = nVal }
    pure $ Right ()

-- | resolveEnvValue resolves an environment variable value.
resolveEnvValue :: FilePath -> IO (Maybe FilePath)
resolveEnvValue aVal =
  case head aVal of
      '$' ->
        let
          (envName, leftOver) = break ('/' ==) aVal
        in do
        mbEnvValue <- Senv.getEnv $ tail envName
        case mbEnvValue of
          Nothing -> pure Nothing
          Just aVal -> pure . Just $ aVal <> leftOver
      _ -> pure $ Just aVal

resolveValue :: (MonadIO f, MonadState s f) => Maybe String -> (String -> s -> s) -> f ConfError
resolveValue aVal setter = do
  case aVal of
      Nothing -> pure $ Right ()
      Just aVal -> do
        mbRezVal <- liftIO $ resolveEnvValue aVal
        case mbRezVal of
          Nothing -> pure . Left $ "Could not resolve value: " <> aVal
          Just aVal -> do
            modify $ setter aVal
            pure $ Right ()
