CREATE TABLE [sqlmesh].[_intervals] (
    [id]                     VARCHAR (450) NOT NULL,
    [created_ts]             BIGINT        NULL,
    [name]                   VARCHAR (450) NULL,
    [identifier]             VARCHAR (450) NULL,
    [version]                VARCHAR (450) NULL,
    [start_ts]               BIGINT        NULL,
    [end_ts]                 BIGINT        NULL,
    [is_dev]                 BIT           NULL,
    [is_removed]             BIT           NULL,
    [is_compacted]           BIT           NULL,
    [is_pending_restatement] BIT           NULL,
    [dev_version]            VARCHAR (450) NULL,
    PRIMARY KEY CLUSTERED ([id] ASC)
);


GO

CREATE NONCLUSTERED INDEX [_intervals_name_identifier_idx]
    ON [sqlmesh].[_intervals]([name] ASC, [identifier] ASC);


GO

CREATE NONCLUSTERED INDEX [_intervals_name_version_idx]
    ON [sqlmesh].[_intervals]([name] ASC, [version] ASC);


GO

