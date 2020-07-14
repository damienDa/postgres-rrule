
--WITH start AS (SELECT start_date::TIMESTAMP from data.appointment limit 10)
--SELECT start_date, _rrule.last('RRULE:FREQ=MONTHLY;INTERVAL=1;BYDAY=+1FR;COUNT=3'::TEXT::_rrule.RRULE, start_date::TIMESTAMP) FROM start;

--+3MO =>
-- bymonthday=1, FREQ=MONTHLY, NO BY DAY
-- BYDAY=MO, FREQ=WEEKELY, COUNT=bynthday

-- ('MONTHLY'::_rrule.FREQ, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)::_rrule.RRULE

SET lc_messages TO 'en_US.UTF-8';

CREATE OR REPLACE FUNCTION _rrule.classic_occurrences(
  "rrule" _rrule.RRULE,
  "dtstart" TIMESTAMP
)
RETURNS TIMESTAMP[] AS $$
  WITH "starts" AS (
    SELECT "start"
    FROM _rrule.all_starts($1, $2) "start"
  ),
  "params" AS (
    SELECT
      "until",
      "interval"
    FROM _rrule.until($1, $2) "until"
    FULL OUTER JOIN _rrule.build_interval($1) "interval" ON (true)
  ),
  "generated" AS (
    SELECT generate_series("start", "until", "interval") "occurrence"
    FROM "params"
    FULL OUTER JOIN "starts" ON (true)
  ),
  "ordered" AS (
    SELECT DISTINCT "occurrence"
    FROM "generated"
    WHERE "occurrence" >= "dtstart"
    ORDER BY "occurrence"
  ),
  "tagged" AS (
    SELECT
      row_number() OVER (),
      "occurrence"
    FROM "ordered"
  )
  SELECT array_agg("occurrence" ORDER BY "occurrence")
  FROM "tagged"
  WHERE "row_number" <= "rrule"."count"
  OR "rrule"."count" IS NULL;
$$ LANGUAGE SQL STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION _rrule.nth_occurrences(
  "rrule" _rrule.RRULE,
  "dtstart" TIMESTAMP
)
RETURNS TIMESTAMP[] AS $$
DECLARE
  l_month_recurrence _rrule.RRULE;
  l_week_recurrence _rrule.RRULE;
  l_occurrences TIMESTAMP[];
  l_result_occurrences TIMESTAMP[];
  l_day _rrule.DAY;
BEGIN
  l_month_recurrence := $1;
  l_month_recurrence."freq" := 'MONTHLY'::_rrule.freq;
  l_month_recurrence."bymonthday" := '{1}';
  l_month_recurrence."byday" := NULL;
  l_month_recurrence."bynthday" := NULL;

  FOREACH l_day IN ARRAY $1."byday"
  LOOP
    l_week_recurrence := $1;
    l_week_recurrence."byday" := ARRAY [l_day];
    l_week_recurrence."freq" := 'WEEKLY'::_rrule.freq;
    l_week_recurrence."count" := $1."bynthday";
    l_week_recurrence."bynthday" := NULL;

    RAISE NOTICE '% %', l_month_recurrence, l_week_recurrence;

    WITH "starts" AS (
      SELECT "start"
      FROM _rrule.occurrences(l_month_recurrence, $2) "start"
    )
    SELECT array_agg(_rrule.last(l_week_recurrence, "start")) 
      FROM "starts" INTO l_occurrences;
    
    l_result_occurrences := l_result_occurrences || l_occurrences;

  END LOOP;

RAISE NOTICE '%', l_result_occurrences;

  SELECT array_agg(x) from (SELECT unnest(l_result_occurrences) as x ORDER BY x) as sub INTO l_result_occurrences;

RAISE NOTICE '%', l_result_occurrences;


  RETURN l_result_occurrences;
END;
$$ LANGUAGE plpgsql STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION _rrule.occurrences(
  "rrule" _rrule.RRULE,
  "dtstart" TIMESTAMP
)
RETURNS SETOF TIMESTAMP AS $$
  SELECT unnest(
    CASE WHEN $1."bynthday" IS NULL 
    THEN 
      _rrule.classic_occurrences($1, $2)
    ELSE
      _rrule.nth_occurrences($1, $2)
    END
  )
$$ LANGUAGE SQL STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION _rrule.occurrences("rrule" _rrule.RRULE, "dtstart" TIMESTAMP, "between" TSRANGE)
RETURNS SETOF TIMESTAMP AS $$
  SELECT "occurrence"
  FROM _rrule.occurrences("rrule", "dtstart") "occurrence"
  WHERE "occurrence" <@ "between";
$$ LANGUAGE SQL STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION _rrule.occurrences("rrule" TEXT, "dtstart" TIMESTAMP, "between" TSRANGE)
RETURNS SETOF TIMESTAMP AS $$
  SELECT "occurrence"
  FROM _rrule.occurrences(_rrule.rrule("rrule"), "dtstart") "occurrence"
  WHERE "occurrence" <@ "between";
$$ LANGUAGE SQL STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION _rrule.occurrences(
  "rruleset" _rrule.RRULESET,
  "tsrange" TSRANGE
)
RETURNS SETOF TIMESTAMP AS $$
  WITH "rrules" AS (
    SELECT
      "rruleset"."dtstart",
      "rruleset"."dtend",
      "rruleset"."rrule"
  ),
  "rdates" AS (
    SELECT _rrule.occurrences("rrule", "dtstart", "tsrange") AS "occurrence"
    FROM "rrules"
    UNION
    SELECT unnest("rruleset"."rdate") AS "occurrence"
  ),
  "exrules" AS (
    SELECT
      "rruleset"."dtstart",
      "rruleset"."dtend",
      "rruleset"."exrule"
  ),
  "exdates" AS (
    SELECT _rrule.occurrences("exrule", "dtstart", "tsrange") AS "occurrence"
    FROM "exrules"
    UNION
    SELECT unnest("rruleset"."exdate") AS "occurrence"
  )
  SELECT "occurrence" FROM "rdates"
  EXCEPT
  SELECT "occurrence" FROM "exdates"
  ORDER BY "occurrence";
$$ LANGUAGE SQL STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION _rrule.occurrences("rruleset" _rrule.RRULESET)
RETURNS SETOF TIMESTAMP AS $$
  SELECT _rrule.occurrences("rruleset", '(,)'::TSRANGE);
$$ LANGUAGE SQL STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION _rrule.occurrences(
  "rruleset_array" _rrule.RRULESET[],
  "tsrange" TSRANGE
  -- TODO: add a default limit and then use that limit from `first` and `last`
)
RETURNS SETOF TIMESTAMP AS $$
DECLARE
  i int;
  lim int;
  q text := '';
BEGIN
  lim := array_length("rruleset_array", 1);
  RAISE NOTICE 'lim %', lim;

  IF lim IS NULL THEN
    q := 'VALUES (NULL::TIMESTAMP) LIMIT 0;';
    RAISE NOTICE 'q %', q;
  ELSE
    FOR i IN 1..lim
    LOOP
      q := q || $q$SELECT _rrule.occurrences('$q$ || "rruleset_array"[i] ||$q$'::_rrule.RRULESET, '$q$ || "tsrange" ||$q$'::TSRANGE)$q$;
      IF i != lim THEN
        q := q || ' UNION ';
      END IF;
    END LOOP;
    q := q || ' ORDER BY occurrences ASC';
  END IF;

  RETURN QUERY EXECUTE q;
END;
$$ LANGUAGE plpgsql STRICT IMMUTABLE;