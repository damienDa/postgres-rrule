CREATE OR REPLACE FUNCTION _rrule.integer_array (TEXT)
RETURNS integer[] AS $$
  SELECT ('{' || $1 || '}')::integer[];
$$ LANGUAGE SQL IMMUTABLE STRICT;
COMMENT ON FUNCTION _rrule.integer_array (text) IS 'Coerce a text string into an array of integers';



CREATE OR REPLACE FUNCTION _rrule.nth_day_array (TEXT)
RETURNS RECORD AS $$
  DECLARE
    i INTEGER;
	l_tmp_day TEXT;
	l_arr TEXT[];
	nthday TEXT;
	days _rrule.DAY[];
  BEGIN
  nthday := 3;
  	SELECT ('{' || $1 || '}')::TEXT[] INTO l_arr;
	  FOR i IN array_lower(l_arr, 1) .. array_upper(l_arr, 1) 
	  LOOP
		l_tmp_day := RIGHT(l_arr[i],2);
		days := days || l_tmp_day::_rrule.DAY;
		nthday := REPLACE(l_arr[i], l_tmp_day, '');
	  END LOOP;
	 RETURN (nthday, days);
  END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION _rrule.day_array (TEXT)
RETURNS _rrule.DAY[] AS $$
  SELECT days FROM _rrule.nth_day_array ($1) AS (nthday TEXT, days _rrule.DAY[]);
$$ LANGUAGE SQL IMMUTABLE STRICT;
COMMENT ON FUNCTION _rrule.day_array (text) IS 'Coerce a text string into an array of "rrule"."day"';

CREATE OR REPLACE FUNCTION _rrule.nth_day (TEXT)
RETURNS TEXT AS $$
  SELECT CASE WHEN nthday = '' THEN NULL ELSE nthday END FROM _rrule.nth_day_array ($1) AS (nthday TEXT, days _rrule.DAY[]);
$$ LANGUAGE SQL IMMUTABLE STRICT;
COMMENT ON FUNCTION _rrule.nth_day (text) IS 'Coerce a text string into an array of "rrule"."day"';

CREATE OR REPLACE FUNCTION _rrule.array_join(ANYARRAY, "delimiter" TEXT)
RETURNS TEXT AS $$
  SELECT string_agg(x::text, "delimiter")
  FROM unnest($1) x;
$$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION _rrule.get_text_days(nthday INTEGER, days _rrule.DAY[], delimiter TEXT)
RETURNS TEXT AS $$
DECLARE 
	l_tmp _rrule.DAY;
	l_out TEXT;
BEGIN
  IF days IS NULL
  THEN
	RETURN NULL;
  END IF;

  l_out := '';
  FOREACH l_tmp IN ARRAY days
  LOOP
	l_out := l_out || CASE WHEN nthday IS NULL THEN '' ELSE nthday::TEXT END || l_tmp || delimiter;
  END LOOP;
  RETURN LEFT(l_out, char_length(l_out) - 1);
END;
$$ LANGUAGE plpgsql;
