/******************************************************************************
3rd party demographics data validation function

Jin Li

Demographics:
Gender
Gender2
Age
Age2
...

******************************************************************************/

/***************************************************************************

	Match with Lookup Table
	
****************************************************************************/
DROP TABLE IF EXISTS jli_Demos;
CREATE TEMP TABLE jli_Demos 
WITH (APPENDONLY=TRUE, ORIENTATION=COLUMN, COMPRESSTYPE=ZLIB, COMPRESSLEVEL=9) AS
-- EXPLAIN
SELECT DISTINCT time_id, idfa
CASE
      WHEN s1.Demo_Group = 'Gender' THEN s1.Demo_Value
      WHEN s2.Demo_Group = 'Gender' THEN s2.Demo_Value
      WHEN s3.Demo_Group = 'Gender' THEN s3.Demo_Value
      WHEN s4.Demo_Group = 'Gender' THEN s4.Demo_Value
      WHEN s5.Demo_Group = 'Gender' THEN s5.Demo_Value
      WHEN s6.Demo_Group = 'Gender' THEN s6.Demo_Value
      WHEN s7.Demo_Group = 'Gender' THEN s7.Demo_Value
      WHEN s8.Demo_Group = 'Gender' THEN s8.Demo_Value
      ELSE ''
  END AS Gender,
CASE
    WHEN s1.Demo_Group = 'Gender2' THEN s1.Demo_Value
    WHEN s2.Demo_Group = 'Gender2' THEN s2.Demo_Value
    WHEN s3.Demo_Group = 'Gender2' THEN s3.Demo_Value
    WHEN s4.Demo_Group = 'Gender2' THEN s4.Demo_Value
    WHEN s5.Demo_Group = 'Gender2' THEN s5.Demo_Value
    WHEN s6.Demo_Group = 'Gender2' THEN s6.Demo_Value
    WHEN s7.Demo_Group = 'Gender2' THEN s7.Demo_Value
    WHEN s8.Demo_Group = 'Gender2' THEN s8.Demo_Value
    ELSE ''
  END AS Gender2,
CASE
    WHEN s1.Demo_Group = 'Household Age' THEN s1.Demo_Value
    WHEN s2.Demo_Group = 'Household Age' THEN s2.Demo_Value
    WHEN s3.Demo_Group = 'Household Age' THEN s3.Demo_Value
    WHEN s4.Demo_Group = 'Household Age' THEN s4.Demo_Value
    WHEN s5.Demo_Group = 'Household Age' THEN s5.Demo_Value
    WHEN s6.Demo_Group = 'Household Age' THEN s6.Demo_Value
    WHEN s7.Demo_Group = 'Household Age' THEN s7.Demo_Value
    WHEN s8.Demo_Group = 'Household Age' THEN s8.Demo_Value
    ELSE ''
  END AS Age,
CASE
    WHEN s1.Demo_Group = 'Household Age2' THEN s1.Demo_Value
    WHEN s2.Demo_Group = 'Household Age2' THEN s2.Demo_Value
    WHEN s3.Demo_Group = 'Household Age2' THEN s3.Demo_Value
    WHEN s4.Demo_Group = 'Household Age2' THEN s4.Demo_Value
    WHEN s5.Demo_Group = 'Household Age2' THEN s5.Demo_Value
    WHEN s6.Demo_Group = 'Household Age2' THEN s6.Demo_Value
    WHEN s7.Demo_Group = 'Household Age2' THEN s7.Demo_Value
    WHEN s8.Demo_Group = 'Household Age2' THEN s8.Demo_Value
    ELSE ''
  END AS Age2      
FROM public.data e
LEFT JOIN 
Analytics.Demo_Lookup s1
ON
      e.segid1 = s1.Segment_ID
LEFT JOIN
      Analytics.jlm_Demo_Lookup s2
ON
      e.segid2 = s2.Segment_ID
LEFT JOIN
      Analytics.jlm_Demo_Lookup s3
ON
      e.segid3 = s3.Segment_ID
LEFT JOIN
      Analytics.jlm_Demo_Lookup s4
ON
      e.segid4 = s4.Segment_ID
LEFT JOIN
      Analytics.jlm_Demo_Lookup s5
ON
      e.segid5 = s5.Segment_ID
LEFT JOIN
      Analytics.jlm_Demo_Lookup s6
ON
      e.segid6 = s6.Segment_ID
LEFT JOIN
      Analytics.jlm_Demo_Lookup s7
ON
      e.segid7 = s7.Segment_ID
LEFT JOIN
      Analytics.jlm_Demo_Lookup s8
ON
      e.segid8 = s8.Segment_ID
DISTRIBUTED BY (idfa);


/********************************************************************************************

	METRICS

********************************************************************************************/

-- no of records with age2
SELECT time_id, count(*) AS age2_record_count FROM jli_Demos WHERE age2!='' GROUP BY 1;
SELECT time_id, age2, count(*) AS record_count FROM jli_Demos WHERE age2!='' GROUP BY 1,2 ORDER BY 1,2;


-- no of idfa with age2
SELECT time_id, count(*) AS age2_record_count FROM jli_Demos WHERE age2!='' GROUP BY 1;
SELECT time_id, count(idfa) AS age2_idfa_count FROM(SELECT time_id, idfa FROM jli_Demos WHERE age2!='' GROUP BY 1,2 ) a GROUP BY 1

-- gender
SELECT time_id, count(*) AS gender_record_count FROM jli_Demos WHERE gender!='' GROUP BY 1 ORDER BY 1
SELECT a.time_id, count(a.idfa) AS gender_idfa_count
FROM(
SELECT time_id, idfa FROM jli_Demos WHERE gender!='' GROUP BY 1,2 ) a
GROUP BY 1
ORDER BY 1;

-- age
SELECT time_id, count(*) AS age_record_count FROM jli_Demos WHERE age!='' GROUP BY 1 ORDER BY 1;
SELECT a.time_id, count(a.idfa) AS age_idfa_count
FROM(
SELECT time_id, idfa FROM jli_Demos WHERE age!='' GROUP BY 1,2 ) a
GROUP BY 1
ORDER BY 1;

-- no. of idfa with different age/age2 and gender/gender2 values
SELECT time_id, count(idfa) AS idfa_count
FROM jli_Demos
WHERE age!=age2 and age!='' and age2!='' and gender!=gender2 and gender!='' and gender2!=''
GROUP BY time_id
ORDER BY time_id;

-- no. of idfa with same age/age2 and gender/gender2 values
SELECT time_id, count(idfa) AS idfa_count
FROM jli_Demos
WHERE age=age2 and age!='' and age2!='' and gender=gender2 and gender!='' and gender2!=''
GROUP BY time_id
ORDER BY time_id;

/**********************************************************************************

	FINAL RESULT TABLE

**********************************************************************************/

DROP TABLE IF EXISTS qa_metrics;
create temp table qa_metrics
AS 
(SELECT time_id, 'no. of records'::character varying AS metric, count(*) AS record_count FROM jli_Demos GROUP BY 1) union all
(SELECT time_id, 'no. of idfAS'::character varying AS metric, count(idfa) AS idfa_count FROM( SELECT time_id, idfa FROM jli_Demos GROUP BY 1,2 ) a GROUP BY 1)union all
(SELECT time_id, 'no. of records without gender2'::character varying AS metric, count(*) AS gender2_count FROM jli_Demos WHERE gender2='' GROUP BY 1) union all
(SELECT time_id, 'no. of records without age2'::character varying AS metric,count(*) AS age2_count FROM jli_Demos WHERE age2='' GROUP BY 1)union all
(SELECT time_id, 'no. of idfAS with gender2 male'::character varying AS metric,count(idfa) AS male_count FROM jli_Demos WHERE gender2='Male' GROUP BY 1)union all
(SELECT time_id, 'no. of idfAS with gender2 female'::character varying AS metric, count(idfa) AS female_count FROM jli_Demos WHERE gender2='Female' GROUP BY 1)union all
(SELECT time_id, 'no. of idfAS with gender male'::character varying AS metric, count(idfa) FROM jli_Demos WHERE gender='Male' GROUP BY 1,2) union all
(SELECT time_id, 'no. of idfAS with gender female'::character varying AS metric, count(idfa) FROM jli_Demos WHERE gender='Female' GROUP BY 1,2) union all
(SELECT time_id, 'no. of idfAS with age'::character varying AS metric, age, count(idfa) FROM jli_Demos WHERE age!='' GROUP BY 1,2)union all
(SELECT time_id, 'no. of idfAS with same gender/gender2 and age/age2 values'::character varying AS metric, count(idfa) AS idfa_count FROM jli_Demos WHERE age=age2 and age!='' and age2!='' and gender=gender2 and gender!='' and gender2!='' GROUP BY time_id)
DISTRIBUTED BY (time_id);

/*********************************************************************************

	EXPORT INTO .TXT

 ********************************************************************************/

-- create external temp table for exporting data
DROP EXTERNAL TABLE IF EXISTS qa;
CREATE WRITABLE EXTERNAL TEMP TABLE qa
(
	time_id int,
	data_source character varying,
	time_type character varying,
	metric character varying,
	value character varying
)
LOCATION
 (
'.../jli/qa.txt'
 ) 
FORMAT 'text' (DELIMITER e'\t' NULL 'null' ESCAPE 'off')
ENCODING 'utf8';

 -- insert contents into writable table
 -- EXPLAIN
INSERT INTO qa
 SELECT *
 FROM qa_metrics
 ORDER BY time_id;
