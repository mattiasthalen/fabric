CREATE TABLE [sqlmesh].[_snapshots] (
    [name]         VARCHAR (450) NOT NULL,
    [identifier]   VARCHAR (450) NOT NULL,
    [version]      VARCHAR (450) NULL,
    [snapshot]     VARCHAR (MAX) NULL,
    [kind_name]    VARCHAR (450) NULL,
    [updated_ts]   BIGINT        NULL,
    [unpaused_ts]  BIGINT        NULL,
    [ttl_ms]       BIGINT        NULL,
    [unrestorable] BIT           NULL,
    PRIMARY KEY CLUSTERED ([name] ASC, [identifier] ASC)
);


GO

CREATE NONCLUSTERED INDEX [_snapshots_name_version_idx]
    ON [sqlmesh].[_snapshots]([name] ASC, [version] ASC);


GO

