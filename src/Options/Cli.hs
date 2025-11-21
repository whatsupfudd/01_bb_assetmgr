{-# LANGUAGE DerivingStrategies #-}

module Options.Cli where

import Data.Text (Text)
import Options.Applicative


data EnvOptions = EnvOptions {
    home :: Maybe Text
  }

data CliOptions = CliOptions {
  debug :: Maybe Int
  , configFile :: Maybe FilePath
  , job :: Maybe Command
 }
 deriving stock (Show)

data GlobalOptions = GlobalOptions {
  confPathGO :: String
  , debugGO :: String
  }

data Command =
  HelpCmd
  | VersionCmd
  | ImportCmd Text Text
  | TestCmd Text
  | IngestCmd Text
  deriving stock (Show)


data ImportOpts = ImportOpts {
    taxonomy :: Text
    , path :: Text
  }

data TestOpts = TestOpts {
    subCmd :: Text
  }

parseCliOptions :: IO (Either String CliOptions)
parseCliOptions =
  Right <$> execParser parser

parser :: ParserInfo CliOptions
parser =
  info (helper <*> argumentsP) $
    fullDesc <> progDesc "Beebod Importer." <> header "importer - Sends data from file systems into a Big Bag of Data."


argumentsP :: Parser CliOptions
argumentsP = do
  buildOptions <$> globConfFileDef <*> hsubparser commandDefs
  where
    buildOptions :: GlobalOptions -> Command -> CliOptions
    buildOptions globs cmd =
      let
        mbConfPath = case globs.confPathGO of
          "" -> Nothing
          aValue -> Just aValue
        mbDebug = case globs.debugGO of
          "" -> Nothing
          aValue -> Just (read aValue :: Int)
      in
      CliOptions {
        debug = mbDebug
        , configFile = mbConfPath
        , job = Just cmd
      }


globConfFileDef :: Parser GlobalOptions
globConfFileDef =
  GlobalOptions <$>
    strOption (
      long "config"
      <> short 'c'
      <> metavar "IMPORTCONF"
      <> value ""
      <> showDefault
      <> help "Global config file (default is ~/.beebod/importer.yaml)."
    )
    <*>
    strOption (
      long "debug"
      <> short 'd'
      <> metavar "DEBUGLVL"
      <> value ""
      <> showDefault
      <> help "Global debug state."
    )
  

commandDefs :: Mod CommandFields Command
commandDefs =
  let
    cmdArray = [
      ("help", pure HelpCmd, "Help about any command.")
      , ("version", pure VersionCmd, "Shows the version number of importer.")
      , ("import", importOpts, "Loads up a path into BeeBoD.")
      , ("test", testOpts, "Test something on importer.")
      , ("ingest", ingestOpts, "Ingest an image description file from ImgClassifier program.")
      ]
    headArray = head cmdArray
    tailArray = tail cmdArray
  in
    foldl (\accum aCmd -> (cmdBuilder aCmd) <> accum) (cmdBuilder headArray) tailArray
  where
    cmdBuilder (label, cmdDef, desc) =
      command label (info cmdDef (progDesc desc))

importOpts :: Parser Command
importOpts =
  ImportCmd <$> strArgument (metavar "TAXO" <> help "Taxonomy root where paths are inserted.")
    <*> strArgument (metavar "PATH" <> help "Directory to import into Beebod.")

testOpts :: Parser Command
testOpts =
  TestCmd <$> strArgument (metavar "SUBCMD" <> help "A subcommand to test.")

ingestOpts :: Parser Command
ingestOpts =
  IngestCmd <$> strArgument (metavar "FILEPATH" <> help "The path of file to ingest.")
