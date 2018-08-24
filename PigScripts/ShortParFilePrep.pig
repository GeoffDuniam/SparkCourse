A = LOAD '/user/hduser/raw_pulsar_data/*.par' using PigStorage(',','-tagFile') as (filename:chararray, strData:chararray);
A1 = FOREACH A GENERATE filename,REPLACE(strData,'  *', ',' ) as fields;
-- A_GROUP = GROUP A ALL;
-- A_COUNT = FOREACH A_GROUP GENERATE COUNT(A);
-- dump A_COUNT;

--B = FOREACH A1 GENERATE filename, val1, val2, val3, val4;
B = FOREACH A1 GENERATE filename, 
	FLATTEN(STRSPLIT(fields ,',')) as (
    	ColName:chararray,
        Value1:chararray,
        Value2:float,
        Value3:float,
        Value4:float
    );


-- See  https://stackoverflow.com/questions/11578815/pivoting-in-pig
grp = group B by filename;
maps = FOREACH grp {
    col1  = filter B by ColName == 'PSRJ';
    col2  = filter B by ColName == 'RAJ';
    col3  = filter B by ColName == 'DECJ';
    col4  = filter B by ColName == 'PEPOCH';
    col5  = filter B by ColName == 'POSEPOCH';
    col6  = filter B by ColName == 'DMEPOCH';
    col7  = filter B by ColName == 'DM';
    generate flatten(group) as Id, 
    	flatten(col1.Value1)	as Column1, 
        flatten(col2.Value1)	as Column2, 
        flatten(col3.Value1)	as Column3,
        flatten(col4.Value1) as Column4,
        flatten(col5.Value1) as Column5,
        flatten(col6.Value1) as Column6,
        flatten(col7.Value1) as Column7;
        };
--describe maps;
--maps_grp = GROUP maps ALL;
--maps_count = FOREACH maps_grp GENERATE COUNT (maps);
--dump maps_count;

binary = FOREACH grp {
    col1  = filter B by ColName == 'BINARY';
    generate flatten(group) as Id, 
        flatten(col1.Value1) as Binary;
        };
describe binary;
--bin_grp = GROUP binary ALL;
--bin_count = FOREACH bin_grp GENERATE COUNT (binary);
--dump bin_count;

R = join maps by Id LEFT OUTER, binary by Id;
STORE R INTO '/user/hduser/clean_pulsar_data/All_Par.csv' USING PigStorage(',');
--R_grp = GROUP R ALL;
--R_count = FOREACH R_grp GENERATE COUNT(R);
--dump R_count;  
--dump R;
