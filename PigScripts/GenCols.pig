A =  LOAD '/user/hduser/clean_pulsar_data/ J0437-4715.msp.100.par' using PigStorage(',','-tagFile') as (Column:chararray, value:chararray); 
B = FOREACH A GENERATE $0 as Column;
dump B:
