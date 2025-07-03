CREATE TABLE [sqlmesh].[_auto_restatements] (
    [snapshot_name]            VARCHAR (450) NOT NULL,
    [snapshot_version]         VARCHAR (450) NOT NULL,
    [next_auto_restatement_ts] BIGINT        NULL,
    PRIMARY KEY CLUSTERED ([snapshot_name] ASC, [snapshot_version] ASC)
);


GO

