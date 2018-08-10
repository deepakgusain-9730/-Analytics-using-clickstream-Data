##load userdata to tbale
CREATE EXTERNAL TABLE IF NOT EXISTS userdata(sessionid BIGINT,clicktime STRING,ipaddress STRING,producturl STRING,swid string,language string,
domain string,regtime STRING,sysspec STRING,city STRING,country STRING,areacode STRING,state STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'  STORED AS TEXTFILE  LOCATION '/user/cloudera/clickstreamforhive/userdata' tblproperties('serialization.null.format'='');



##load urlmap data to table
 create external table if not exists urlmap(url String,category String) 
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
 STORED AS TEXTFILE 
 LOCATION '/user/cloudera/clickstreamforhive/urlmap';
 
 
 ##load registeruser to table
  create external table if not exists registeruser2(swid String,birthdate String,gender String) 
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
 STORED AS TEXTFILE 
 LOCATION '/user/cloudera/clickstreamforhive/reguser'
 tblproperties("skip.header.line.count"="1",'serialization.null.format'='');
 
 ##adding jar file
ADD JAR hdfs:///user/cloudera/jar/hiveudf.jar


##creating temporary function
CREATE TEMPORARY FUNCTION getAge as 'FirstUdf'


##calling function for getting age
create table registeruser as SELECT swid,birthdate,gender,getAge(birthdate) as age FROM registeruser2

##age category
CREATE view one as SELECT substr(swid,2,length(swid)-2) as id,producturl FROM userdata ;


CREATE view two as SELECT o.producturl,r.age FROM one o join registeruser r ON o.id=r.swid ;

CREATE view third as select t.producturl,t.age,u.category from two t join urlmap u on t.producturl=u.url;

create table agecategory as select age,category,count(*) as choice from third group by category,age sort by age,choice asc;

##target segmnet by age
CREATE view targetsegment1 as SELECT age,max(choice) as choicecategory from agecategory GROUP BY age;

create table targetsegmentbyage as SELECT f.age,f.category,fi.choicecategory from agecategory f JOIN targetsegment1 fi on f.age = fi.age AND f.choice = fi.choicecategory;


##per click from location
CREATE view second as SELECT u.country,u.city,u.areacode,url.producturl,url.category FROM userdata u LEFT OUTER JOIN urlmap url ON u.producturl=url.url ;

create view locationClick as select category,city,country,count(*) as click from second group by country,city,category sort by  click desc;







##top 10 page with bounce rate
create table bouncerate1 as 
with tte as (select count(click) as click,producturl from(SELECT count(ipaddress) as click,
producturl from userdata GROUP BY producturl,ipaddress)result 
where click=1 group by producturl),tte2 as (select count(click) as click,producturl
from(SELECT count(ipaddress) as click,producturl from userdata GROUP BY producturl,ipaddress)result 
where click!=1 group by producturl) 
select (click/totalclick)*100 as bouncerate,producturl,"100" as percentage from
(select t.click as click,t.click+t2.click as totalclick,t.producturl as producturl 
from tte t join tte2 t2 where t.producturl=t2.producturl)
result order by bouncerate desc limit 10;



create table bouncerate as  select b.bouncerate,b.percentage,u.category,b.producturl from bouncerate1 b join urlmap u on u.url=b.producturl; 
