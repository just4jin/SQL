/**************************************************************************

3rd party data validation function

Input: time id, client id, demographics labels, 3rd party data table
Output: validation notice

note: demographics labels and table names used here are arbitrary

***************************************************************************/

CREATE OR REPLACE FUNCTION demo_validation_jli( month_id int, week_id int, client_name character varying,
	demo1 character varying, demo2 character varying, input_table character varying )                   
RETURNS SETOF record AS

$BODY$

DECLARE 
_month_id1 int = 0;
_month_id2 int = 0;
_month_id3 int = 0;
_week_string character varying;
_client_name character varying;
_month_string character varying;
_demo1 character varying;
_input_table character varying;

BEGIN 

_month_id1 :=month_id-1;
_month_id2 :=month_id;
_month_id3 :=month_id+1;
_client_name :=client_name;
_input_table :=input_table;
_week_string :=week_id||'w';
_demo1 :=demo1;


EXECUTE($$
DROP TABLE IF EXISTS machine_ids;
CREATE TEMP TABLE machine_ids AS
SELECT DISTINCT a.*, b.machine_id
FROM $$||_input_table||$$ a
     INNER JOIN cookie_jar_$$||_month_id1||$$m  b
     ON a.UIDCompress = b.Cookie_Val
        AND b.domain_name = '*.com'  -- arbitrary domain name
        AND b.cookie_id   = 'uid'
DISTRIBUTED BY (uidcompress);$$);

EXECUTE($$
INSERT INTO machine_ids
SELECT DISTINCT a.*, b.machine_id
FROM $$||_input_table||$$ a
INNER JOIN cookie_jar_$$||_month_id1||$$m  b
ON a.UIDCompress = b.Cookie_Val
   AND b.domain_name = '*.com' 
   AND b.cookie_id   = 'uid'
;$$);

EXECUTE($$
INSERT INTO machine_ids
SELECT DISTINCT a.*, b.machine_id
FROM $$||_input_table||$$ a
INNER JOIN cookie_jar_$$||_month_id2||$$m  b
ON a.UIDCompress = b.Cookie_Val
   AND b.domain_name = '*.com' 
   AND b.cookie_id   = 'uid';$$);

-- LOOKING FOR UIDS OBSERVED ON MULTIPLE MACHINES. 
DROP TABLE IF EXISTS multi_uid_machines;
CREATE TEMP TABLE multi_uid_machines AS
SELECT UIDCompress, COUNT(DISTINCT machine_id) AS machines
FROM Machine_IDs
GROUP BY UIDCompress 
DISTRIBUTED BY (UIDCompress);

-- LOOKING FOR MACHINE_ID WITH MUTIPLE UIDS 
DROP TABLE IF EXISTS mutiple_uid_machine_id;
CREATE TEMP TABLE mutiple_uid_machine_id
AS
SELECT machine_id,COUNT(distinct uidcompress) AS uid_COUNT
FROM machine_ids
GROUP BY machine_id
DISTRIBUTED BY (machine_id);

-- EXCLUDE MACHINE_IDS COUNT >1 
DROP TABLE IF EXISTS machine_id_select_1;
CREATE TEMP TABLE machine_id_select_1
AS
SELECT *
FROM machine_ids
WHERE machine_id IN (
	SELECT machine_id 
	FROM mutiple_uid_machine_id 
	WHERE uid_COUNT=1) as foo
DISTRIBUTED BY (machine_id);

EXECUTE($$
DROP TABLE IF EXISTS machine_id_select_$$||_client_name||$$_jli;
CREATE TABLE machine_id_select_$$||_client_name||$$_jli
AS
SELECT *
FROM machine_id_select_1
WHERE uidcompress IN (
	SELECT uidcompress 
	FROM multi_uid_machines 
	WHERE machines=1) as foo1
DISTRIBUTED BY (machine_id);$$);

IF demo1 = 'label1' -- user specified demographics label
THEN 
 RAISE NOTICE '%', 'IN THE 1ST EXECUTE'; 

-- Mapping for different variables.
EXECUTE($$
	DROP TABLE IF EXISTS comparison_$$||demo1||$$;
	CREATE TEMP TABLE comparison_$$||demo1||$$
	AS
		SELECT DISTINCT a.uidcompress, b.v_COUNTry, b.partner_id, b.label1, a.label1, 
		CASE
		        WHEN a.label1 = 'Yes'     THEN 'Present'
		        WHEN a.label1 = 'No'      THEN 'Not Present' 
		        ELSE ''
		END,
		b.label2,
		CASE
		        WHEN b.label1  = 0         THEN 'Not Present'
		        WHEN b.label1  = 1         THEN 'Present'
		        ELSE ''
		END
		FROM machine_id_select_$$||_client_name||$$_jli a
		INNER JOIN browser_$$||_week_string||$$ b
		ON a.machine_id = b.machine_id
		AND label1 <>'null'
		DISTRIBUTED BY (uidcompress);$$);
RAISE NOTICE '%', 'LABEL1 VALIDATED';
		     
ELSEIF demo2 = 'gender' 
	THEN
	EXECUTE ($$
		DROP TABLE IF EXISTS comparison_$$||demo2||$$; 
		CREATE TEMP TABLE comparison_ppc_$$||demo2||$$
		AS
		SELECT DISTINCT a.uidcompress, b.v_COUNTry, b.partner_id, a.gender as gender_1, b.gender as gender_2
		FROM machine_id_select_$$||client_name||$$_jli a
		INNER JOIN browser_$$||_week_string||$$ b
		ON a. machine_id=b.machine_id
		AND gender <> 'null'
		GROUP BY 1,2,3,4,5,6
		DISTRIBUTED BY (uidcompress)
		$$);
RAISE NOTICE '%', 'GENDER VALIDATED';


END IF;
END;
$BODY$
LANGUAGE plpgsql VOLATILE; 
