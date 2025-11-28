CREATE OR REPLACE FUNCTION taxo.fix_leaf_labels_from_path(
    p_path   text,
    p_arboid integer DEFAULT NULL
)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
    path_clean     text;
    elems          text[];
    path_len       integer;
    root_node_id   integer;
    root_arboid    integer;
    base_path      text;   -- full path from tree root to the chosen directory
    updated_count  bigint;
BEGIN
    -- 0. Normalize and split the input path (no regexp).
    path_clean := btrim(p_path, '/');
    elems      := string_to_array(path_clean, '/');
    path_len   := COALESCE(array_length(elems, 1), 0);

    IF path_len = 0 THEN
        RAISE EXCEPTION 'Empty or invalid path: %', p_path;
    END IF;

    ----------------------------------------------------------------------
    -- 1. Resolve p_path ("a/b/c") to a directory node id via parentid chain
    ----------------------------------------------------------------------
    WITH RECURSIVE pathwalk AS (
        -- first component
        SELECT
            n.id,
            n.parentid,
            n.label,
            n.arboid,
            1 AS depth
        FROM taxo.nodes n
        WHERE n.label = elems[1]
          AND n.assetid IS NULL              -- must be a directory
          AND (p_arboid IS NULL OR n.arboid = p_arboid)

        UNION ALL

        -- further components
        SELECT
            c.id,
            c.parentid,
            c.label,
            c.arboid,
            pw.depth + 1 AS depth
        FROM taxo.nodes c
        JOIN pathwalk pw ON c.parentid = pw.id
        WHERE c.label = elems[pw.depth + 1]
          AND c.assetid IS NULL             -- only follow directory nodes
    )
    SELECT id, arboid
    INTO root_node_id, root_arboid
    FROM pathwalk
    WHERE depth = path_len
    LIMIT 1;

    IF root_node_id IS NULL THEN
        RAISE EXCEPTION 'No directory node found for path % (arboid=%)', p_path, p_arboid;
    END IF;

    select elems[path_len] into base_path;

    ----------------------------------------------------------------------
    -- 3. For each directory in the subtree, compute:
    --    - full_path   : full path from tree root (as used in labels)
    --    - path_prefix : full_path || '/'
    --    - prefix_len  : char_length(path_prefix)
    --
    --    Then use that prefix_len to truncate file labels.
    ----------------------------------------------------------------------
    WITH RECURSIVE subtree_dirs AS (
        -- root directory of the subtree
        SELECT
            n.id,
            n.parentid,
            n.label,
            n.arboid,
            base_path::text                         AS full_path,
            (base_path || '/')::text                AS path_prefix,
            char_length(base_path || '/')           AS prefix_len
        FROM taxo.nodes n
        WHERE n.id = root_node_id
          AND n.assetid IS NULL

        UNION ALL

        -- descendant directories
        SELECT
            child.id,
            child.parentid,
            child.label,
            child.arboid,
            (sd.full_path || '/' || child.label)::text              AS full_path,
            (sd.full_path || '/' || child.label || '/')::text       AS path_prefix,
            char_length(sd.full_path || '/' || child.label || '/')  AS prefix_len
        FROM taxo.nodes child
        JOIN subtree_dirs sd ON child.parentid = sd.id
        WHERE child.assetid IS NULL
          AND child.arboid = sd.arboid
    )

    UPDATE taxo.nodes f
    SET label = substring(f.label FROM sd.prefix_len + 1)
    FROM subtree_dirs sd
    WHERE f.parentid = sd.id
      AND f.assetid IS NOT NULL          -- file-equivalent nodes only
      AND f.arboid = sd.arboid
      -- ensure the label actually starts with the computed prefix
      AND char_length(f.label) > sd.prefix_len
      AND substring(f.label FROM 1 FOR sd.prefix_len) = sd.path_prefix;

    GET DIAGNOSTICS updated_count = ROW_COUNT;
    RETURN updated_count;
END;
$$;


CREATE OR REPLACE FUNCTION taxo.test_a(
    p_path text,
    p_arboid integer DEFAULT NULL
)
RETURNS table(rfileid int, rolabel text, rid int, rparentid int, rlabel varchar, rfullpath text)
AS $$
DECLARE
    path_clean     text;
    elems          text[];
    path_len       integer;
    root_node_id   integer;
    root_arboid    integer;
    t_base_path      text;   -- full path from tree root to the chosen directory
    base_path      text;   -- full path from tree root to the chosen directory
    updated_count  bigint;
    sublabel text;
BEGIN
    -- 0. Normalize and split the input path (no regexp).
    path_clean := btrim(p_path, '/');
    elems      := string_to_array(path_clean, '/');
    path_len   := COALESCE(array_length(elems, 1), 0);

    IF path_len = 0 THEN
        RAISE EXCEPTION 'Empty or invalid path: %', p_path;
    END IF;

    ----------------------------------------------------------------------
    -- 1. Resolve p_path ("a/b/c") to a directory node id via parentid chain
    ----------------------------------------------------------------------
    WITH RECURSIVE pathwalk AS (
        -- first component
        SELECT
            n.id,
            n.parentid,
            n.label,
            n.arboid,
            1 AS depth
        FROM taxo.nodes n
        WHERE n.label = elems[1]
          AND n.assetid IS NULL              -- must be a directory
          AND (p_arboid IS NULL OR n.arboid = p_arboid)

        UNION ALL

        -- further components
        SELECT
            c.id,
            c.parentid,
            c.label,
            c.arboid,
            pw.depth + 1 AS depth
        FROM taxo.nodes c
        JOIN pathwalk pw ON c.parentid = pw.id
        WHERE c.label = elems[pw.depth + 1]
          AND c.assetid IS NULL             -- only follow directory nodes
    )
    SELECT id, arboid
    INTO root_node_id, root_arboid
    FROM pathwalk
    WHERE depth = path_len
    LIMIT 1;

    IF root_node_id IS NULL THEN
        RAISE EXCEPTION 'No directory node found for path % (arboid=%)', p_path, p_arboid;
    END IF;

    select elems[path_len] into base_path;

    ----------------------------------------------------------------------
    -- 3. For each directory in the subtree, compute:
    --    - full_path   : full path from tree root (as used in labels)
    --    - path_prefix : full_path || '/'
    --    - prefix_len  : char_length(path_prefix)
    --
    --    Then use that prefix_len to truncate file labels.
    ----------------------------------------------------------------------
    return query WITH RECURSIVE subtree_dirs AS (
        -- root directory of the subtree
        SELECT
            n.id,
            n.parentid,
            n.label,
            n.arboid,
            base_path::text AS full_path,
            (base_path || '/')::text AS path_prefix,
            char_length(base_path || '/') AS prefix_len
        FROM taxo.nodes n
        WHERE n.id = root_node_id
          AND n.assetid IS NULL

        UNION ALL

        -- descendant directories
        SELECT
            child.id,
            child.parentid,
            child.label,
            child.arboid,
            (sd.full_path || '/' || child.label)::text AS full_path,
            (sd.full_path || '/' || child.label || '/')::text AS path_prefix,
            char_length(sd.full_path || '/' || child.label || '/')  AS prefix_len
        FROM taxo.nodes child
        JOIN subtree_dirs sd ON child.parentid = sd.id
        WHERE child.assetid IS NULL
          AND child.arboid = sd.arboid
    )
    select
      f.id as rfileid
      -- , substring(f.label FROM sd.prefix_len + 1) as rfilelabel
      -- table: , rfilelabel text
      , f.label::text as rolabel
      , sd.id as rid, sd.parentid as rparentid
      , sd.label as rlabel, sd.full_path as rfullpath
    from subtree_dirs sd
    join taxo.nodes f on sd.id = f.parentid
    where
      f.assetid is not null
      and f.arboid = sd.arboid;
      -- AND char_length(f.label) > sd.prefix_len
      -- AND substring(f.label FROM 1 FOR sd.prefix_len) = sd.path_prefix;

END;
$$ LANGUAGE plpgsql;

