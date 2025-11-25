module MainLogic
where

import Data.Text (pack, unpack)
import qualified System.Environment as Env

import qualified Options.Cli as Opt (CliOptions (..), EnvOptions (..), Command (..))
import qualified Options.ConfFile as Opt (FileOptions (..))
import qualified Options as Opt
import Commands as Cmd


runWithOptions :: Opt.CliOptions -> Opt.FileOptions -> IO ()
runWithOptions cliOptions fileOptions = do
  -- putStrLn $ "@[runWithOptions] cliOpts: " <> show cliOptions
  -- putStrLn $ "@[runWithOptions] fileOpts: " <> show fileOptions
  case cliOptions.job of
    Nothing -> do
      putStrLn "@[runWithOptions] start on nil command."
    Just aJob -> do
      -- Get environmental context in case it's required in the merge. Done here to keep the merge pure:
      mbHome <- Env.lookupEnv "BBAMGRHOME"
      let
        envOptions = Opt.EnvOptions {
            appHome = pack <$> mbHome
            -- TODO: put additional env vars.
          }
        -- switchboard to command executors:
        cmdExecutor =
          case aJob of
            Opt.HelpCmd -> Cmd.helpHu
            Opt.VersionCmd -> Cmd.versionHu
            Opt.ImportCmd params -> Cmd.importCmd params
            Opt.IngestCmd filePath -> Cmd.ingestCmd (unpack filePath)
            Opt.TestCmd subCmd -> Cmd.testCmd subCmd
            Opt.ListCmd params -> Cmd.listCmd params
            Opt.FetchCmd params -> Cmd.fetchCmd params
      rtOptions <- Opt.mergeOptions cliOptions fileOptions envOptions
      result <- cmdExecutor rtOptions
      -- TODO: return a properly kind of conclusion.
      pure ()
