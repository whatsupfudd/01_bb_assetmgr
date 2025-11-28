{-# LANGUAGE QuasiQuotes #-}

module DB.Statements where

import qualified Data.Foldable as Fld
import qualified Data.Vector as V
import Data.Int (Int16, Int32, Int64)
import qualified Data.Sequence as Seq
import Data.Text (Text)
import Data.Time (UTCTime)
import Data.Vector (Vector)

import Hasql.Session (Session, statement)
import qualified Hasql.TH as TH


type TaxoLabelOut = (Text, Int32, Text)
fetchTaxos :: Session (Vector TaxoLabelOut)
fetchTaxos =
  statement () [TH.vectorStatement|
    select
      a.label::text, a.id::int4, b.internalname::text
    from taxo.catalogs as a
    join taxo.owners b on a.ownerid = b.id
  |]


fetchTaxosForOwner :: Int32 -> Session (Vector TaxoLabelOut)
fetchTaxosForOwner ownerID =
  statement ownerID [TH.vectorStatement|
    select
      a.label::text, a.id::int4, b.internalname::text
    from taxo.catalogs as a
    join taxo.owners b on a.ownerid = b.id
    where a.ownerid = $1::int4
  |]

fetchTaxosForOwnerLabel :: (Text, Text) -> Session (Maybe Int32)
fetchTaxosForOwnerLabel (owner, label) =
  statement (owner, label) [TH.maybeStatement|
    select
      a.id::int4
    from taxo.catalogs a
    join taxo.owners b on a.ownerid = b.id
    where a.label = $2::text
          and b.internalname = $1::text
  |]


-- id, ownerid
type TaxoOut = (Int32, Int32)
fetchTaxoByLabel :: (Text, Text) -> Session (Maybe TaxoOut)
fetchTaxoByLabel params =
  statement params [TH.maybeStatement|
    select
      a.id::int4, b.id::int4
    from taxo.catalogs a
      join taxo.owners b on a.ownerid = b.id
    where a.label = $2::text
          and b.internalname = $1::text
  |]


type OwnerOut = Int32
fetchOwnerByName :: Text -> Session (Maybe OwnerOut)
fetchOwnerByName name =
  statement name [TH.maybeStatement|
    select
      a.id::int4
    from taxo.owners a
    where a.internalname = $1::text
  |]


-- ownerID + New taxo label (label must be unique)
type NewTaxoOut = (Int32)
addTaxonomy :: (Int32, Text) -> Session NewTaxoOut
addTaxonomy params =
  statement params [TH.singletonStatement|
    insert into taxo.catalogs
      (ownerid, label)
      values ($1::int4, $2::text)
    returning id::int4
  |]


type RootOut = (Text, Int32)
fetchRootsForTaxo :: Int32 -> Session (Vector RootOut)
fetchRootsForTaxo taxoID =
  statement taxoID [TH.vectorStatement|
    select a.label::text, a.id::int4
      from taxo.nodes a
      where a.arboid = $1::int4
        and a.parentid is null
  |]


-- path, top-level id, depth:
type PathOut = (Text, Int32, Int32)
fetchPathForNode :: Int32 -> Session (Vector PathOut)
fetchPathForNode taxoID =
  statement taxoID [TH.vectorStatement|
    with recursive pathup as (
      select a.label, a.id, a.parentid, a.label || ';' as tpath, 1 as depth
        from taxo.nodes a
        where a.id = $1::int4
      union all
       select nd.label, nd.id, nd.parentid, nd.label || '/' || pu.tpath as tpath, pu.depth+1 as depth
         from taxo.nodes as nd
          inner join pathup pu on pu.parentid = nd.id
    ) select tpath::varchar::text, id::int4, depth::int4 from pathup order by depth desc limit 1
  |]

-- id, label, parentID, assetID, lastMod, depth:
type NodeOut = (Int32, Text, Maybe Int32, Maybe Int32, Maybe UTCTime, Int32)
fetchNodesForTaxoLimited :: Int32 -> Int32 -> Session (Vector NodeOut)
fetchNodesForTaxoLimited taxoID maxDepth =
  statement (taxoID, maxDepth) [TH.vectorStatement|
  with recursive xtree as (
    select id, label, parentid, assetid, lastmod, 1 as depth
      from taxo.nodes
      where arboid = $1::int4 and parentid is null
    union all
     select nd.id, nd.label, nd.parentid, nd.assetid, nd.lastmod, xt.depth+1 as depth
       from taxo.nodes as nd
        inner join xtree xt on xt.id = nd.parentid
      where depth <= $2::int4
    ) select id::int4, label::text, parentid::int4?, assetid::int4?, lastmod::timestamptz?, depth::int4 from xtree
  |]


-- top-level id, label, parentID, md5ID, lastMod, depth:
fetchNodesForTaxo :: Int32 -> Session (Vector NodeOut)
fetchNodesForTaxo taxoID =
  statement taxoID [TH.vectorStatement|
  with recursive xtree as (
    select id, label, parentid, assetid, lastmod, 1 as depth
      from taxo.nodes
      where arboid = $1::int4 and parentid is null
    union all
     select nd.id, nd.label, nd.parentid, nd.assetid, nd.lastmod, xt.depth+1 as depth
       from taxo.nodes as nd
        inner join xtree xt on xt.id = nd.parentid
    ) select id::int4, label::text, parentid::int4?, assetid::int4?, lastmod::timestamptz?, depth::int4 from xtree
  |]


fetchSubTreeForNode :: Int32 -> Int32 -> Session (Vector NodeOut)
fetchSubTreeForNode nodeID maxDepth =
  statement (nodeID, maxDepth) [TH.vectorStatement|
  with recursive xtree as (
    select id, label, parentid, assetid, lastmod, 1 as depth
      from taxo.nodes
      where id = $1::int4
    union all
     select nd.id, nd.label, nd.parentid, nd.assetid, nd.lastmod, xt.depth+1 as depth
       from taxo.nodes as nd
        inner join xtree xt on xt.id = nd.parentid
        where xt.depth <= $2::int4
    ) select id::int4, label::text, parentid::int4?, assetid::int4?, lastmod::timestamptz?, depth::int4 from xtree
  |]


type NodeInfoOut = (Text, Maybe Int32, Maybe UTCTime, Maybe Text, Maybe Int64, Maybe Text, Maybe UTCTime)
fetchNodeInfo :: Int32 -> Session (Maybe NodeInfoOut)
fetchNodeInfo nodeID =
  statement nodeID [TH.maybeStatement|
    select
      a.label::text, a.parentid::int4?, a.lastmod::timestamptz?
      , b.md5::text?, b.size::int8?, b.locator::text?, b.lastmod::timestamptz?
      from taxo.nodes a
      join data.assets b on a.assetid = b.id
    where a.id = $1::int4
  |]


fetchAssetsByMD5 :: V.Vector Text -> Session (Vector (Text, Int32))
fetchAssetsByMD5 assetBlock =
  statement assetBlock [TH.vectorStatement|
    select
      b.md5::text, b.id::int4
    from data.assets b
    where b.md5 = any($1::text[])
  |]


-- md5 : string -> text, size : int64 -> bigint, locator: s3-path -> varchar

type NewAssetIn = (Text, Int64, Text)
type NewAssetOut = (Int32, UTCTime)
insertAsset :: NewAssetIn -> Session NewAssetOut
insertAsset params =
  statement params [TH.singletonStatement|
    insert into data.assets
      (md5, size, locator)
      values ($1::text, $2::int8, $3::text)
    returning id::int4, enteredon::timestamptz
  |]

-- label, parentid, assetid, lastmod, rights, arboid
type NewNodeIn = (Text, Maybe Int32, Maybe Int32, Int64, Text, Int32)
type NewNodeOut = (Int32, UTCTime)
insertNode :: NewNodeIn -> Session NewNodeOut
insertNode params =
  statement params [TH.singletonStatement|
    insert into taxo.nodes
      (label, parentid, assetid, lastmod, rights, arboid)
      values ($1::text, $2::int4?, $3::int4?, to_timestamp($4::int8), $5::text, $6::int4)
    returning id::int4, to_timestamp($4::int8)::timestamptz
  |]

derefPathToNode :: Int32 -> Vector Text -> Session (Maybe Int32)
derefPathToNode arboid path =
  statement (arboid, path) [TH.maybeStatement|
    WITH RECURSIVE pathwalk AS (
      SELECT
          n.id,
          n.parentid,
          n.label,
          n.arboid,
          1 AS depth
      FROM taxo.nodes n
      WHERE n.label = ($2::text[])[1]
        AND n.assetid IS NULL
        AND n.arboid = $1::int4

      UNION ALL

      SELECT
          c.id,
          c.parentid,
          c.label,
          c.arboid,
          pw.depth + 1 AS depth
      FROM taxo.nodes c
      JOIN pathwalk pw ON c.parentid = pw.id
      WHERE c.label = ($2::text[])[pw.depth + 1]
        AND c.assetid IS NULL
  )
  SELECT id::int4
  FROM pathwalk
  WHERE depth = array_length($2::text[], 1)
  LIMIT 1
  |]