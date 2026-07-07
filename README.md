# assetmgr

`assetmgr` is the command-line asset management utility for the Big Bag of Data / 0to1Done asset store.

In its current form, it is a Haskell CLI application that connects three concerns:

1. a local filesystem source tree;
2. a PostgreSQL metadata/index database;
3. an S3-compatible object store used for the binary asset payloads.

The current utility is primarily focused on importing local file trees into the asset store, browsing stored taxonomies, fetching stored assets back from object storage, and parsing classifier-output files. It is not currently a CDN synchronisation tool; the Bunny.net synchronisation feature should be added as a new command in the same CLI command framework.

---

## Repository status

The repository currently contains a minimal placeholder README in upstream form. This document describes the utility as implemented by the current source layout, with special focus on the `Commands` namespace, which contains the implementations of all CLI-callable operations.

Important current source areas:

```text
app/Main.hs
src/MainLogic.hs
src/Options.hs
src/Options/Cli.hs
src/Options/ConfFile.hs
src/Options/Runtime.hs
src/Commands.hs
src/Commands/
src/DB/
src/Logic/
src/Storage/
src/Tree/
```

The executable name is `assetmgr`.

---

## High-level architecture

The runtime flow is:

```text
app/Main.hs
  -> Options.Cli.parseCliOptions
  -> Options.ConfFile.parseConfigFile
  -> MainLogic.runWithOptions
  -> command dispatch
  -> Commands.<Command>.xxxCmd
```

At a conceptual level:

```text
CLI options + YAML config
        ↓
merged runtime options
        ↓
command-specific executor
        ↓
PostgreSQL metadata operations and/or S3 object-store operations
```

The main configuration sources are resolved in this order:

1. CLI `--config` / `-c` path;
2. `AMGRCONF` environment variable;
3. default config path: `$HOME/.fudd/assetmgr/config.yaml`.

`MainLogic` also reads `BBAMGRHOME` into an environment-options record. The current command behaviour is mostly driven by the parsed YAML config and CLI command parameters.

---

## Build and run

Typical Stack usage:

```bash
stack build
stack run assetmgr -- --help
```

Typical direct executable usage after build/install:

```bash
assetmgr --help
assetmgr --config ./config.yaml version
```

Global CLI options are parsed before the command:

```bash
assetmgr --config ./config.yaml --debug 2 import media ./incoming
```

---

## Configuration

The utility expects a YAML configuration file.

Example:

```yaml
debug: 0
owner: "user"
rootDir: "$BB_ASSET_ROOT"

pgDb:
  host: "localhost"
  port: 5432
  user: "assetmgr"
  passwd: "assetmgr"
  dbase: "assetmgr"

s3store:
  accessKey: "S3_ACCESS_KEY"
  secretKey: "S3_SECRET_KEY"
  host: "https://s3.example.net"
  region: "garage"
  bucket: "bb-assets"
```

Current config sections:

| Section | Purpose |
|---|---|
| `debug` | Default runtime debug level. Can be overridden with `--debug`. |
| `owner` | Default logical owner for import/list operations. |
| `rootDir` | Optional filesystem root used to resolve relative import paths. If it starts with `$`, the value is resolved from an environment variable. |
| `pgDb` | PostgreSQL connection settings. |
| `s3store` | S3-compatible object-store connection settings. |

The S3 backend is mandatory for `import`, `fetch`, and `test` to perform useful object-store work. Some commands either fail or silently do nothing when `s3store` is missing; this should be tightened in future CLI UX work.

---

## CLI commands

The CLI command type is defined in `Options.Cli` and currently includes:

```haskell
HelpCmd
VersionCmd
ImportCmd ImportOpts
TestCmd Text
IngestCmd Text
ListCmd ListOpts
FetchCmd FetchOpts
```

The commands are dispatched in `MainLogic.runWithOptions` to the corresponding `Commands.*` module.

---

## Commands namespace

`src/Commands.hs` is a re-export module. It gathers the command implementations from:

```text
Commands.Help
Commands.Version
Commands.Import
Commands.Test
Commands.IngestClass
Commands.List
Commands.Fetch
```

Every CLI-callable operation is implemented under this namespace. To add a new command, add a new `Commands.<Name>` module, export it from `Commands.hs`, add a constructor and parser in `Options.Cli`, then dispatch it in `MainLogic.runWithOptions`.

---

## `help`

```bash
assetmgr help
```

Current implementation module:

```text
src/Commands/Help.hs
```

Current behaviour:

- prints a placeholder/stub message;
- does not currently render the full optparse-applicative help text itself.

For normal CLI help, use:

```bash
assetmgr --help
assetmgr import --help
assetmgr list --help
```

---

## `version`

```bash
assetmgr version
```

Current implementation module:

```text
src/Commands/Version.hs
```

Current behaviour:

- prints the package version;
- prints the current Git hash;
- prints the Git commit date.

This command uses compile-time Git metadata.

---

## `import`

```bash
assetmgr --config ./config.yaml import [--no-recurse] TAXO PATH [ROOT]
```

Examples:

```bash
assetmgr -c ./config.yaml import media ./incoming
assetmgr -c ./config.yaml import --no-recurse media ./incoming imported/root
```

Current implementation module:

```text
src/Commands/Import.hs
```

Options and arguments:

| Parameter | Meaning |
|---|---|
| `TAXO` | Target taxonomy name. |
| `PATH` | Filesystem path to import. |
| `ROOT` | Optional anchor/root path used when rebasing imported tree paths. |
| `--no-recurse`, `-1` | Import only the top-level item instead of the full recursively explored tree. |

Current behaviour:

1. Requires `s3store` configuration.
2. Creates an S3 connection.
3. Creates a PostgreSQL connection pool.
4. Resolves the source path:
   - absolute paths are used directly;
   - relative paths are resolved from `rootDir` if provided, otherwise from the current working directory.
5. Explores the local folder/file tree.
6. Optionally limits the import to the top-level entry when `--no-recurse` is used.
7. Rebases the discovered file tree using either the provided `ROOT` anchor or the source directory name.
8. Delegates ingestion to `Logic.Ingestion.loadFileTree`.

Conceptually:

```text
local filesystem tree
        ↓
Storage.Explore.loadFolderTree
        ↓
Tree/Core representation
        ↓
Logic.Ingestion.loadFileTree
        ↓
PostgreSQL metadata + S3 object payloads
```

Debug bits currently expose additional import diagnostics, including resolved paths and tree display.

---

## `list`

```bash
assetmgr --config ./config.yaml list [--recurse] [--maxdepth N] [OWNER] [TAXO]
```

Examples:

```bash
assetmgr -c ./config.yaml list
assetmgr -c ./config.yaml list user
assetmgr -c ./config.yaml list -m 2 user media
```

Current implementation module:

```text
src/Commands/List.hs
```

Options and arguments:

| Parameter | Meaning |
|---|---|
| `OWNER` | Owner to list. Defaults to an empty string at the parser level. |
| `TAXO` | Taxonomy to list. Defaults to an empty string at the parser level. |
| `--maxdepth`, `-m` | Maximum depth when listing taxonomy paths. Defaults to `1`. |
| `--recurse`, `-r` | Parsed by the CLI, but current list implementation is effectively controlled by `--maxdepth`. |

Current behaviour:

- if `OWNER` is empty, fetches all taxonomies;
- if `OWNER` is provided and `TAXO` is empty, lists taxonomies for that owner;
- if both `OWNER` and `TAXO` are provided, lists paths under that owner/taxonomy up to `--maxdepth`;
- formats output through `Logic.Listing.showListResults`.

This is a metadata/database browsing command. It does not read object payloads from S3.

---

## `fetch`

```bash
assetmgr --config ./config.yaml fetch NODEID OUTPUTFILE
```

Example:

```bash
assetmgr -c ./config.yaml fetch 123 ./node-123.bin
```

Current implementation module:

```text
src/Commands/Fetch.hs
```

Options and arguments:

| Parameter | Meaning |
|---|---|
| `NODEID` | Numeric database node identifier. |
| `OUTPUTFILE` | Local file path where the fetched object should be written. |

Current behaviour:

1. Requires `s3store` configuration.
2. Opens an S3 connection.
3. Opens a PostgreSQL connection pool.
4. Fetches metadata for the requested node ID.
5. Reads the stored object locator from the database result.
6. Downloads the object from S3 into the requested local output path.

Conceptually:

```text
node id
  ↓
PostgreSQL metadata lookup
  ↓
object locator
  ↓
S3 get object
  ↓
local output file
```

---

## `ingest`

```bash
assetmgr --config ./config.yaml ingest CLASSIFIER_OUTPUT_FILE
```

Example:

```bash
assetmgr -c ./config.yaml ingest ./classifier-output.txt
```

Current implementation module:

```text
src/Commands/IngestClass.hs
```

Current behaviour:

- reads a classifier output file;
- parses image-feature sections;
- prints parsed image classification results;
- ignores images with no parsed features in the final display.

The parser recognises line patterns such as:

```text
f: <file>
objs: <count>
<<<
<classification-lines>
>>>
```

It also handles `<nothing>` and `<skip>` markers.

This command currently appears to be a classifier-output parser/reviewer rather than a full database/S3 ingestion path.

---

## `test`

```bash
assetmgr --config ./config.yaml test S3_OBJECT_KEY
```

Example:

```bash
assetmgr -c ./config.yaml test some/object/key.pdf
```

Current implementation module:

```text
src/Commands/Test.hs
```

Current behaviour:

- requires `s3store` configuration;
- creates an S3 connection;
- attempts to fetch the provided S3 object key;
- writes the downloaded object to `/tmp/gaga.out`.

This command is a development/debug command, not a polished user-facing command.

The module also contains helper routines for S3 listing experiments, but the current `testCmd` path is focused on object fetching.

---

## Storage layer

Current S3 support lives under:

```text
src/Storage/S3.hs
src/Storage/Types.hs
```

The S3 implementation uses `minio-hs` and provides operations for:

- creating an S3 connection;
- putting a file into the configured bucket;
- fetching a file to local disk;
- fetching an object through a conduit-based path;
- listing files under an optional prefix;
- listing files across a vector of prefixes.

Current exported operations include:

```haskell
makeS3Conn
putFile
getFile
getFileB
listFiles
listFilesWith
```

The storage code currently treats the configured S3-compatible bucket as the canonical binary payload store for imported assets.

---

## Database layer

Current database support lives under:

```text
src/DB/Connect.hs
src/DB/Operations.hs
src/DB/Statements.hs
```

The command modules use the DB layer to:

- open a PostgreSQL connection pool;
- load imported file-tree metadata;
- fetch taxonomy listings;
- fetch node metadata before retrieving object payloads.

The actual database schema and statements are encapsulated behind the `DB.Operations` and `DB.Statements` modules.

---

## Logic layer

Current business logic is grouped under:

```text
src/Logic/Ingestion.hs
src/Logic/Listing.hs
```

The command modules use this layer instead of embedding all business logic directly in command handlers.

Current responsibilities:

| Module | Responsibility |
|---|---|
| `Logic.Ingestion` | Load a discovered filesystem tree into the database and S3 object store. |
| `Logic.Listing` | Query and format taxonomy/path listings. |

---

## Current limitations and cleanup targets

The current implementation is usable as a developer utility, but several areas should be cleaned up before broader operational use:

1. The upstream README is only a placeholder.
2. Some help strings still refer to earlier names such as “importer” or “Beebod Importer”.
3. The `help` command is a stub; `--help` is the useful path today.
4. `list --recurse` is parsed but the current implementation is effectively controlled by `--maxdepth`.
5. `import` silently does nothing if `s3store` is missing; it should report a clear configuration error.
6. `test` writes to a hard-coded `/tmp/gaga.out` path.
7. Current S3 listing returns path-oriented results but does not expose a rich metadata listing API suitable for robust synchronisation.
8. There is no Bunny.net, CDN, or replication command yet.
9. Error handling is mostly command-local and should be standardised before adding more operational commands.
10. There is no manifest-oriented sync/audit output yet.

---

## Recommended insertion point for the Bunny.net synchronisation feature

The Bunny.net synchronisation feature should be added as a new command rather than modifying `import`, `fetch`, or the current S3 storage path.

Recommended module additions:

```text
src/Commands/Sync.hs
src/Storage/Bunny.hs
src/Storage/Bunny/Types.hs
src/Logic/Sync.hs
```

or, if the team wants the first version to be explicitly Bunny-specific:

```text
src/Commands/BunnySync.hs
src/Storage/Bunny.hs
src/Logic/BunnySync.hs
```

Recommended CLI shape:

```bash
assetmgr --config ./config.yaml bunny-sync \
  --limit 1000 \
  --source-prefix "" \
  --dest-prefix "s3-mirror-test" \
  --dry-run
```

Recommended implementation steps:

1. Add Bunny config parsing in `Options.ConfFile`.
2. Add Bunny runtime config in `Options.Runtime` and `Options.mergeOptions`.
3. Add a new command constructor in `Options.Cli.Command`.
4. Add the command parser in `Options.Cli.commandDefs`.
5. Add `Commands.BunnySync` with a `bunnySyncCmd` executor.
6. Export the new module from `Commands.hs`.
7. Add a dispatch case in `MainLogic.runWithOptions`.
8. Add S3 metadata listing support if needed by the sync planner.
9. Implement Bunny HTTP Storage API upload/list/verify operations.
10. Emit a JSON sync manifest for dry-runs and completed syncs.

The first synchronisation implementation should be additive-only, limited to 1,000 assets, and should not delete or overwrite Bunny-side objects unless explicitly requested.

---

## Development convention for new commands

To add a command consistently with the current architecture:

1. Define command options in `Options.Cli`.
2. Add a constructor to `Command`.
3. Add a parser entry under `commandDefs`.
4. Implement the command in `src/Commands/<Name>.hs`.
5. Export the command module from `src/Commands.hs`.
6. Add the dispatch case in `MainLogic.runWithOptions`.
7. Keep command-specific orchestration in `Commands.<Name>`.
8. Push reusable business logic into `Logic.<Name>`.
9. Push external service access into `Storage.<Backend>` or a dedicated backend namespace.

This pattern keeps the CLI layer thin and makes the new synchronisation functionality testable independently from the command parser.
