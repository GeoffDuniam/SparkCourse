set mapreduce.map.memory.mb    3072;
set mapreduce.reduce.memory.mb 3072;
set mapreduce.map.java.opts -Xmx2764m;
set mapreduce.reduce.java.opts -Xmx2764m;
set mapreduce.task.io.sort.mb 756;

A = LOAD '/user/hduser/raw_pulsar_data//J1910-0112.d1.2*9.tim' using PigStorage(',','-tagFile') as (filename:chararray, strData:chararray);
A1 = FOREACH A GENERATE REPLACE(filename, '.tim', '') as filename ,REPLACE( TRIM(strData) ,'  *', ',' ) as fields;
-- dump A1;
B = FOREACH A1 GENERATE filename, 
	FLATTEN(STRSPLIT(fields ,',')) as (
    	RecNo:chararray,
        ObsFreqMHz:chararray,
        SiteArrTimeMJD:float,
        UncertaintyArrTime:float,
        TelescopeCode:chararray,
	UserFlag1:chararray,
	UserFlag1Value:chararray,
	Userflag2:chararray,
	Userflag2Value:chararray,
	UserFlag3:chararray,
	UserFlag3Value:float,
	UserFlag4:chararray,
	UserFlag4Value:float
    );

C = FOREACH (fiLTER B BY (RecNo == 'FORMAT') OR (RecNo == 'MODE')) GENERATE filename, RecNo, ObsFreqMHz;
--C = FOREACH (fiLTER B BY (RecNo == 'FORMAT') OR (RecNo == 'MODE')) GENERATE REPLACE(filename, '.tim', '');

dump C;
