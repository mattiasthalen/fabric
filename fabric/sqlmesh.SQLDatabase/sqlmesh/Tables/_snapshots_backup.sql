CREATE TABLE [sqlmesh].[_snapshots_backup] (
    [name]         VARCHAR (450) NULL,
    [identifier]   VARCHAR (450) NULL,
    [version]      VARCHAR (450) NULL,
    [snapshot]     VARCHAR (MAX) NULL,
    [kind_name]    VARCHAR (450) NULL,
    [updated_ts]   BIGINT        NULL,
    [unpaused_ts]  BIGINT        NULL,
    [ttl_ms]       BIGINT        NULL,
    [unrestorable] BIT           NULL
);


GO

