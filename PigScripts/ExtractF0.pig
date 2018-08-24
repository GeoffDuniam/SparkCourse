A = LOAD '/user/hduser/raw_pulsar_data/*.par' using PigStorage(',','-tagFile') as (filename:chararray, strData:chararray);
A1 = FOREACH A GENERATE REPLACE(filename, '.par', '') as filename, REPLACE(strData,'  *', ',' ) as fields;
--B = FOREACH A1 GENERATE filename, val1, val2, val3, val4;
B = FOREACH A1 GENERATE filename, 
	FLATTEN(STRSPLIT(fields ,',')) as (
    	ColName:chararray,
        Value1:chararray,
        Value2:float,
        Value3:float,
        Value4:float
    );

grp = group B by filename;
FData = FOREACH grp {
        col1  = filter B by ColName == 'F0';
        col2  = filter B by ColName == 'F1';
    generate flatten(group) as Id,
        flatten(col1.Value1) as F01,
        flatten(col1.Value2) as F02,
        flatten(col1.Value3) as F03,
        flatten(col2.Value1) as F11,
        flatten(col2.Value2) as F12,
        flatten(col2.Value3) as F13;
        };

Store FData INTO '/user/hduser/clean_pulsar_data/All_F0_F1.csv' USING PigStorage(',');
