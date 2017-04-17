# BDA-Project

Team Members : 

Chiquita R Prabhu	crp380

Harita A Jagad	haj263

Manjiri S Acharekar	msa530





FOLDERS:

COUNT_CHECK: CHECK THE COUNT OF NULL AND NON-NULL VALUES OF ATTRIBUTES

TESTING FOR VALIDITY: SCRIPTS OFOR LABELLING NULL, VALID AND INVALID VALUES

PLOTS: VISUALS PLOTTED USING PYTHON pandas



RUN ENIVRIONMENT 

FOR COUNCT-CHECK AND TESTING FOR VALIDITY SCRIPTS:



#set environment variables

alias hfs='/usr/bin/hadoop fs ' 

export HAS=/opt/cloudera/parcels/CDH-5.9.0-1.cdh5.9.0.p0.23/lib 

export HSJ=hadoop-mapreduce/hadoop-streaming.jar  

alias hjs='/usr/bin/hadoop jar $HAS/$HSJ'



#run the program

hadoop fs -copyFromLocal 'name_of_the_file.csv'

spark-submit 'name_of_the_file.py' 'name_of_the_file.csv'



#get output file

hadoop fs -getmerge 'output_file_name.out' 'output_file_name.out'


