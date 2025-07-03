CREATE TABLE [sqlmesh].[_environment_statements] (
    [environment_name]       VARCHAR (450) NOT NULL,
    [plan_id]                VARCHAR (MAX) NULL,
    [environment_statements] VARCHAR (MAX) NULL,
    PRIMARY KEY CLUSTERED ([environment_name] ASC)
);


GO

