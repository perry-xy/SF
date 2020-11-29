--1 查询" 01 "课程比" 02 "课程成绩高的学生的信息及课程分数
√
--2 查询同时存在" 01 "课程和" 02 "课程的情况
√
--3 查询存在" 01 "课程但可能不存在" 02 "课程的情况(不存在时显示为 null )
√
--4 查询不存在" 01 "课程但存在" 02 "课程的情况
√
--5 查询平均成绩大于等于 60 分的同学的学生编号和学生姓名和平均成绩
√
--6 查询在 SC 表存在成绩的学生信息
√
--7 查询所有同学的学生编号、学生姓名、选课总数、所有课程的总成绩(没成绩的显示为 null )
√
--8 查有成绩的学生信息
√
--9 查询「李」姓老师的数量
√ like '李%'
--10 查询学过「张三」老师授课的同学的信息
√
--11 查询没有学全所有课程的同学的信息
√
--12 查询至少有一门课与学号为" 01 "的同学所学相同的同学的信息
select b.* 
from
(select a.sid sid_1,
       b.sid sid_2,
       count(*) num 
       from 
            (select * 
                    from tmp_dm_as.cxy_exercise_kuan 
                    where score!='未选' 
                        and sid='01') a
        join 
            (select * 
                    from tmp_dm_as.cxy_exercise_kuan 
                    where score!='未选' 
                        and sid!='01') b 
        on a.cid=b.cid) aa 
join  tmp_dm_as.cxy_sid_score bb 
on aa. sid_2=bb.sid
where num>=1;
--13 查询和" 01 "号的同学学习的课程 完全相同的其他同学的信息
select b.* 
from
(select *,
       case when yuwen is not null then 1 else 0 end yuwen_,
       case when shuxue is not null then 1 else 0 end shuxue_,
       case when yingyu is not null then 1 else 0 end yingyu_
       from tmp_dm_as.cxy_sid_score
       where sid='01') a 
join 
(select *,
       case when yuwen is not null then 1 else 0 end yuwen_,
       case when shuxue is not null then 1 else 0 end shuxue_,
       case when yingyu is not null then 1 else 0 end yingyu_
       from tmp_dm_as.cxy_sid_score
       where sid！='01') b 
on a.yuwen_=b.yuwen_ and a.shuxue_=b.shuxue_ and a.yingyu_=b.yingyu_;
--14 查询没学过"张三"老师讲授的任一门课程的学生姓名
√
--15 查询两门及其以上不及格课程的同学的学号，姓名及其平均成绩
select 
sid,
sname,
av_score
from tmp_dm_as.cxy_sid_score
where ((yuwen<60 and shuxue<60) or (yuwen<60 and yingyu<60) or (shuxue<60 and yingyu<60));
--16 检索" 01 "课程分数小于 60，按分数降序排列的学生信息
√
--17 按平均成绩从高到低显示所有学生的所有课程的成绩以及平均成绩
√
--18 查询各科成绩最高分、最低分和平均分：
--以如下形式显示：课程 ID，课程 name，最高分，最低分，平均分，及格率，中等率，优良率，优秀率
--及格为>=60，中等为：70-80，优良为：80-90，优秀为：>=90
--要求输出课程号和选修人数，查询结果按人数降序排列，若人数相同，按课程号升序排列
select cid,
        cname,
        max(score) max_,
        min(score) min_,
        avg(score) avg_,
        count(distinct case when score>=60 then sid end)/count(distinct sid) 'jige',
        count(distinct case when score>=70 and score<80 then sid end)/count(distinct sid) 'zhongdeng',
        count(distinct case when score>=80 and score<90 then sid end)/count(distinct sid) 'youliang',
        count(distinct case when score>=90 then sid end)/count(distinct sid) 'youxiu'
        from tmp_dm_as.cxy_exercise_kuan 
        where score != '未选'
        group by cid,
                 cname;
--19 按各科成绩进行排序，并显示排名， Score 重复时保留名次空缺
select 
sid,
sname,
sage,
ssex,
cid,
cname,
score,
rank() over(partition by cid order by score desc) rank_ 
from tmp_dm_as.cxy_exercise_kuan
where score!='未选'
--20 按各科成绩进行排序，并显示排名， Score 重复时合并名次
select 
sid,
sname,
sage,
ssex,
cid,
cname,
score,
dense_rank() over(partition by cid order by score desc) dense_rank_ 
from tmp_dm_as.cxy_exercise_kuan
where score!='未选'
--21 查询学生的总成绩，并进行排名，总分重复时保留名次空缺
rank()
--22 查询学生的总成绩，并进行排名，总分重复时不保留名次空缺
dense_rank()
--23 统计各科成绩各分数段人数：课程编号，课程名称，[100-85]，[85-70]，[70-60]，[60-0] 及所占百分比
select cid,
        cname,
        count(distinct case when score<60 then sid end)/count(*) '0-60',
        count(distinct case when score>=60 and score<70 then sid end)/count(*) '60-70',
        count(distinct case when score>=70 and score<85 then sid end)/count(*) '70-85',
        count(distinct case when score>=85 then sid end)/count(*) '85-100'
        from tmp_dm_as.cxy_exercise_kuan 
        where score !='未选'
        group by cid,
                 cname;
--24 查询各科成绩前三名的记录
select *
       from
        (select *,
                dense_rank() over(partition by c_id order by score desc) rank_
                from tmp_dm_as.cxy_exercise_kuan) a 
        where rank_<=3;
--25 查询每门课程被选修的学生数
√
--26 查询出只选修两门课程的学生学号和姓名
√
--27 查询男生、女生人数
√
--28 查询名字中含有「风」字的学生信息
like '%风%'
--29 查询同名同性学生名单，并统计同名人数
select * 
from
(select sname,
        ssex,
        count(*) num
        from tmp_dm_as.cxy_sid_score 
        group by sname,
                ssex) a 
join 
tmp_dm_as.cxy_sid_score  b 
on a.sname=b.sname and a.ssex=b.ssex
where num>1;
--30 查询 1990 年出生的学生名单
substr()
--31 查询每门课程的平均成绩，结果按平均成绩降序排列，平均成绩相同时，按课程编号升序排列
order by avg_ desc,
         cid;
--32 查询平均成绩大于等于 85 的所有学生的学号、姓名和平均成绩
√
--33 查询课程名称为「数学」，且分数低于 60 的学生姓名和分数
√
--34 查询所有学生的课程及分数情况（存在学生没成绩，没选课的情况）
√
--35 查询任何一门课程成绩在 70 分以上的姓名、课程名称和分数
√
--36 查询不及格的课程
√
--37 查询课程编号为 01 且课程成绩在 80 分以上的学生的学号和姓名
√
--38 求每门课程的学生人数
√
--39 成绩不重复，查询选修「张三」老师所授课程的学生中，成绩最高的学生信息及其成绩
select b.*
       from
            (select *,
                    row_number() over(order by score desc) rank_ 
                    from tmp_dm_as.cxy_exercise_kuan
                    where tname='张三') a 
            join tmp_dm_as.cxy_sid_score b 
            on a.sid=b.sid
--40 成绩有重复的情况下，查询选修「张三」老师所授课程的学生中，成绩最高的学生信息及其成绩
rank() over()
--41 查询不同课程成绩相同的学生的学生编号、课程编号、学生成绩
select a.sid,
       a.cid,
       a.score,
       b.sid,
       b.cid,
       b.score 
       from tmp_dm_as.cxy_exercise_kuan  a 
       join tmp_dm_as.cxy_exercise_kuan  b 
       on 
       a.cid != b.cid and a.score=b.score and a.sid != b.sid
       where a.score != '未选' and b.score != '未选'
       order by a.sid,
                a.cid;
--42 查询每门功成绩最好的前两名
rank() or dense_rank() <=2
--43 统计每门课程的学生选修人数（超过 5 人的课程才统计）。
select * 
from
（select cid,
       count(*) num 
       from tmp_dm_as.cxy_exercise_kuan 
       where socre!='未选'）a 
where num>5;
--44 检索至少选修两门课程的学生学号
√
--45 查询选修了全部课程的学生信息
√
--46 查询各学生的年龄，只按年份来算
√
--47 按照出生日期来算，当前月日 < 出生年月的月日则，年龄减一
select *,
        case when month(ssage)<month(current_date()) then year(current_date())-year(ssage)
             when month(ssage)>month(current_date()) then year(current_date())-year(ssage)-1
             when month(ssage)=month(current_date()) and day(ssage)<=day(current_date()) then year(current_date())-year(ssage)
             when month(ssage)=month(current_date()) and day(ssage)>day(current_date()) then year(current_date())-year(ssage)-1
             end age
        from tmp_dm_as.cxy_sid_score
--48 查询本周过生日的学生
select *
        from tmp_dm_as.cxy_sid_score
        where weekofyear(current_date()) = weekofyear(s_birth)
--49 查询下周过生日的学生
select *
        from 
        (select *,
                substr(ssage,6,5) birth_rq,
                substr(date_add(current_date(),1),6,5) add_1,
                substr(date_add(current_date(),2),6,5) add_2,
                substr(date_add(current_date(),3),6,5) add_3,
                substr(date_add(current_date(),4),6,5) add_4,
                substr(date_add(current_date(),5),6,5) add_5,
                substr(date_add(current_date(),6),6,5) add_6,
                substr(date_add(current_date(),7),6,5) add_7
                from tmp_dm_as.cxy_sid_score) a
        where birth_rq=add_1 or birth_rq=add_2 or birth_rq=add_3 birth_rq=add_4 or birth_rq=add_5 or birth_rq=add_6 or birth_rq=add_7

select *
        from tmp_dm_as.cxy_sid_score
        where weekofyear(current_date())+1 = weekofyear(s_birth)
--50 查询本月过生日的学生
select * 
        from tmp_dm_as.cxy_sid_score 
        where month(sage)=month(current_date());
--51 查询下月过生日的学生
select * 
        from 
        (select *,
                case when month(ssage)=1 then 13 else month(ssage) end month_
                from tmp_dm_as.cxy_sid_score) a
        where month_=month(current_date())+1;