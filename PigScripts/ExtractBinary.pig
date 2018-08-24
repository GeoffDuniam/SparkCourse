A = LOAD '/user/hduser/raw_pulsar_data/*.par' using PigStorage(',','-tagFile') as (filename:chararray, strData:chararray);
A1 = FOREACH A GENERATE filename,REPLACE(strData,'  *', ',' ) as fields;
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
binary = FOREACH grp {
        col8  = filter B by ColName == 'PMRA';
        col9  = filter B by ColName == 'PMDEC';
        col10 = filter B by ColName == 'PX';
        col11 = filter B by ColName == 'BINARY';
        col12 = filter B by ColName == 'PB';
        col13 = filter B by ColName == 'T0';
        col14 = filter B by ColName == 'A1';
        col15 = filter B by ColName == 'OM';
        col16 = filter B by ColName == 'ECC';
        col17 = filter B by ColName == 'PBDOT';
        col18 = filter B by ColName == 'OMDOT';
        col19 = filter B by ColName == 'M2';
        col26 = filter B by ColName == 'KOM';
        col27 = filter B by ColName == 'KIN';
    generate flatten(group) as Id,
        flatten(col8.Value1) as Column8,
        flatten(col9.Value1) as Column9,
        flatten(col10.Value1) as Column10,
        flatten(col11.Value1) as Column11,
        flatten(col12.Value1) as Column12,
        flatten(col13.Value1) as Column13,
        flatten(col14.Value1) as Column14,
        flatten(col15.Value1) as Column15,
        flatten(col16.Value1) as Column16,
        flatten(col17.Value1) as Column17,
        flatten(col18.Value1) as Column18,
        flatten(col19.Value1) as Column19,
        flatten(col26.Value1) as Column26,
        flatten(col27.Value1) as Column27;
        };


binary_grp = GROUP binary ALL;
binary_count = FOREACH binary_grp GENERATE COUNT (binary);
dump binary_count;

