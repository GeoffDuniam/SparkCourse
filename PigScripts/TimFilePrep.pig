set mapreduce.map.memory.mb    3072;
set mapreduce.reduce.memory.mb 3072;
set mapreduce.map.java.opts -Xmx2764m;
set mapreduce.reduce.java.opts -Xmx2764m;
set mapreduce.task.io.sort.mb 756;

-- set the compression
set output.compression.codec org.apache.hadoop.io.compress.GzipCodec;

-- set large split size to merge small files together and then compress
-- This number is in bytes - which translates to 2.5GB
-- This is a large number but only because my files had a 10:1 compression ratio
-- i.e.the attempted to create 2.5GB file(s) shrink down to 256MB when compressed
set pig.maxCombinedSplitSize 2684354560;

A = LOAD '/user/hduser/raw_pulsar_data/*.tim' using PigStorage(',','-tagFile') as (filename:chararray, strData:chararray);
A1 = FOREACH A GENERATE REPLACE(filename, '.tim', '') as filename, REPLACE( TRIM(strData) ,'  *', ',' ) as fields;

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

C = FOREACH (FILTER B BY (RecNo == 'FORMAT') OR (RecNo == 'MODE')) GENERATE filename, RecNo, ObsFreqMHz;

grp = GROUP C BY filename;

TAGS = FOREACH grp {
        col1  = filter C by RecNo == 'FORMAT';
        col2  = filter C by RecNo == 'MODE';
    generate flatten(group) as Id,
        flatten(col1.ObsFreqMHz)    as FORMAT,
        flatten(col2.ObsFreqMHz)    as MODE;
	};

R = FOREACH (FILTER B BY (RecNo != 'FORMAT') AND (RecNo != 'MODE')) GENERATE 
	filename, 
	ObsFreqMHz, 
	SiteArrTimeMJD, 
	UncertaintyArrTime, 
	TelescopeCode, 
	UserFlag1Value, 
	Userflag2Value, 
	UserFlag3Value, 
	UserFlag4Value;
JND = JOIN R BY filename, TAGS by Id USING 'replicated';

RESULTS = FOREACH JND GENERATE 
        filename,
        ObsFreqMHz,
        SiteArrTimeMJD,
        UncertaintyArrTime,
        TelescopeCode,
        UserFlag1Value,
        Userflag2Value,
        UserFlag3Value,
        UserFlag4Value,
	FORMAT,
	MODE;

STORE RESULTS INTO '/user/hduser/clean_pulsar_data/All_Tim.COMPRESSED.csv' USING PigStorage(',');
