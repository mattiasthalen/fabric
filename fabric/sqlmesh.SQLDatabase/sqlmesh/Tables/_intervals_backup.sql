CREATE TABLE [sqlmesh].[_intervals_backup] (
    [id]                     VARCHAR (450) NULL,
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
    [dev_version]            VARCHAR (450) NULL
);


GO

