CREATE TABLE [sqlmesh].[_plan_dags] (
    [request_id] VARCHAR (450) NOT NULL,
    [dag_id]     VARCHAR (450) NULL,
    [dag_spec]   VARCHAR (MAX) NULL,
    PRIMARY KEY CLUSTERED ([request_id] ASC)
);


GO

CREATE NONCLUSTERED INDEX [dag_id_idx]
    ON [sqlmesh].[_plan_dags]([dag_id] ASC);


GO

