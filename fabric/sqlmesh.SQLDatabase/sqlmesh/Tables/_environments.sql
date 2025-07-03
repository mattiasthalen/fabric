CREATE TABLE [sqlmesh].[_environments] (
    [name]                         VARCHAR (450) NOT NULL,
    [snapshots]                    VARCHAR (MAX) NULL,
    [start_at]                     VARCHAR (MAX) NULL,
    [end_at]                       VARCHAR (MAX) NULL,
    [plan_id]                      VARCHAR (MAX) NULL,
    [previous_plan_id]             VARCHAR (MAX) NULL,
    [expiration_ts]                BIGINT        NULL,
    [finalized_ts]                 BIGINT        NULL,
    [promoted_snapshot_ids]        VARCHAR (MAX) NULL,
    [suffix_target]                VARCHAR (MAX) NULL,
    [catalog_name_override]        VARCHAR (MAX) NULL,
    [previous_finalized_snapshots] VARCHAR (MAX) NULL,
    [normalize_name]               BIT           NULL,
    [requirements]                 VARCHAR (MAX) NULL,
    [gateway_managed]              BIT           NULL,
    PRIMARY KEY CLUSTERED ([name] ASC)
);


GO

