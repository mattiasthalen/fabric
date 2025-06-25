MODEL (
  kind VIEW,
  enabled FALSE
);

SELECT
  *
FROM data_according_to_requirements.uss._bridge__as_of
WHERE
  1 = 1 AND is_current_record = 1