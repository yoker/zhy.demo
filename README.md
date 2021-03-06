## 纯数据库实现方案

### 1、对原表结构进行索引构建（方便后续重建表结构后的数据导出）
```sql
create index ix_hm1 on hm_tb(hm1)
create index ix_hm2 on hm_tb(hm2)
create index ix_hm3 on hm_tb(hm3)
create index ix_hm4 on hm_tb(hm4)
```

### 2、重建表结构
```sql
set nocount on
declare @sql varchar(100)
declare @ii int
set @index = 0
while @index <= 65535
begin
	set @tb_index = cast(@index as varchar(7))
	set @sql = 'create table tb_'+ @tb_index + ' (id int not null, hm1 int, hm2 int, hm3 int, hm4 int)'
	execute(@sql)
	set @index = @index + 1
end
```


### 3、数据导入到新表结构中
```sql
set nocount on
declare @sql1 varchar(100)
declare @sql2 varchar(100)
declare @sql3 varchar(100)
declare @sql4 varchar(100)
declare @tb_index varchar(7)
declare @index int
set @index = 0
while @index <= 65535
begin
	set @tb_index = cast(@index as varchar(7))
	set @sql1 = 'insert into tb_'+ @tb_index + ' select id, hm1,hm2,hm3,hm4 from hm_tb where hm1='+ @tb_index
	execute(@sql1)
	set @sql2 = 'insert into tb_'+ @tb_index + ' select id, hm1,hm2,hm3,hm4 from hm_tb where hm2='+ @tb_index
	execute(@sql2)
	set @sql3 = 'insert into tb_'+ @tb_index + ' select id, hm1,hm2,hm3,hm4 from hm_tb where hm3='+ @tb_index
	execute(@sql3)
	set @sql4 = 'insert into tb_'+ @tb_index + ' select id, hm1,hm2,hm3,hm4 from hm_tb where hm4='+ @tb_index
	execute(@sql4)
	set @index = @index + 1
end
print @index
```


### 4、构建查询存储过程
```sql
CREATE PROCEDURE [dbo].[getrows]
	@hm1 int, 
	@hm2 int,
	@hm3 int,
	@hm4 int
AS
BEGIN
	SET NOCOUNT ON;
	declare @hash1 varchar(7);
	declare @hash2 varchar(7);
	declare @hash3 varchar(7);
	declare @hash4 varchar(7);
	declare @hash5 varchar(7);
	declare @hash6 varchar(7);
	declare @hash7 varchar(7);
	declare @hash8 varchar(7);
	declare @sql varchar(1981);
	set @hash1 = cast(@hm1 as varchar(7))
	set @hash2 = cast(@hm2 as varchar(7))
	set @hash3 = cast(@hm3 as varchar(7))
	set @hash4 = cast(@hm4 as varchar(7))
	set @hash5 = cast(@hm1^1 as varchar(7))
	set @hash6 = cast(@hm2^1 as varchar(7))
	set @hash7 = cast(@hm3^1 as varchar(7))
	set @hash8 = cast(@hm4^1 as varchar(7))
	set @sql = 'select distinct id,hm1,hm2,hm3,hm4 from ('
	+' select id,hm1,hm2,hm3,hm4 from tb_' + @hash1 + ' where dbo.bit_count(hm1^' + @hash1 + ')+dbo.bit_count(hm2^' + @hash2 + ')+dbo.bit_count(hm3^' + @hash3 + ')+dbo.bit_count(hm4^' + @hash4 + ')<=4 '
	+' union all select id,hm1,hm2,hm3,hm4 from tb_' + @hash2 + ' where dbo.bit_count(hm1^' + @hash1 + ')+dbo.bit_count(hm2^' + @hash2 + ')+dbo.bit_count(hm3^' + @hash3 + ')+dbo.bit_count(hm4^' + @hash4 + ')<=4 '
	+' union all select id,hm1,hm2,hm3,hm4 from tb_' + @hash3 + ' where dbo.bit_count(hm1^' + @hash1 + ')+dbo.bit_count(hm2^' + @hash2 + ')+dbo.bit_count(hm3^' + @hash3 + ')+dbo.bit_count(hm4^' + @hash4 + ')<=4 '
	+' union all select id,hm1,hm2,hm3,hm4 from tb_' + @hash4 + ' where dbo.bit_count(hm1^' + @hash1 + ')+dbo.bit_count(hm2^' + @hash2 + ')+dbo.bit_count(hm3^' + @hash3 + ')+dbo.bit_count(hm4^' + @hash4 + ')<=4 '
	+' union all select id,hm1,hm2,hm3,hm4 from tb_' + @hash5 + ' where dbo.bit_count(hm1^' + @hash1 + ')+dbo.bit_count(hm2^' + @hash2 + ')+dbo.bit_count(hm3^' + @hash3 + ')+dbo.bit_count(hm4^' + @hash4 + ')<=4 '
	+' union all select id,hm1,hm2,hm3,hm4 from tb_' + @hash6 + ' where dbo.bit_count(hm1^' + @hash1 + ')+dbo.bit_count(hm2^' + @hash2 + ')+dbo.bit_count(hm3^' + @hash3 + ')+dbo.bit_count(hm4^' + @hash4 + ')<=4 '
	+' union all select id,hm1,hm2,hm3,hm4 from tb_' + @hash7 + ' where dbo.bit_count(hm1^' + @hash1 + ')+dbo.bit_count(hm2^' + @hash2 + ')+dbo.bit_count(hm3^' + @hash3 + ')+dbo.bit_count(hm4^' + @hash4 + ')<=4 '
	+' union all select id,hm1,hm2,hm3,hm4 from tb_' + @hash8 + ' where dbo.bit_count(hm1^' + @hash1 + ')+dbo.bit_count(hm2^' + @hash2 + ')+dbo.bit_count(hm3^' + @hash3 + ')+dbo.bit_count(hm4^' + @hash4 + ')<=4 '
	+') as T'
	execute(@sql);
END
```

### 4、终极完整模式存储过程
```sql
CREATE PROCEDURE [dbo].[getrows2]
	@hm1 int, 
	@hm2 int,
	@hm3 int,
	@hm4 int
AS
BEGIN
	SET NOCOUNT ON;
	create table #T(id int, hm1 int, hm2 int, hm3 int, hm4 int)

	declare @hash1 varchar(7);
	declare @hash2 varchar(7);
	declare @hash3 varchar(7);
	declare @hash4 varchar(7);
	set @hash1 = cast(@hm1 as varchar(7))
	set @hash2 = cast(@hm2 as varchar(7))
	set @hash3 = cast(@hm3 as varchar(7))
	set @hash4 = cast(@hm4 as varchar(7))

	declare @sql varchar(5288);
	set @sql = ' select id,hm1,hm2,hm3,hm4 from tb_' + @hash1 
		+' union all select id,hm1,hm2,hm3,hm4 from tb_' + @hash2 
		+' union all select id,hm1,hm2,hm3,hm4 from tb_' + @hash3 
		+' union all select id,hm1,hm2,hm3,hm4 from tb_' + @hash4 
	insert into #T execute(@sql)

	declare @hash5 varchar(7);
	declare @hash6 varchar(7);
	declare @hash7 varchar(7);
	declare @hash8 varchar(7);
	declare @tmpsql varchar(4000);
	declare @index int;
	set @index=1;
	while @index<65535
	begin
		set @hash5 = cast(@hm1^@index as varchar(7))
		set @hash6 = cast(@hm2^@index as varchar(7))
		set @hash7 = cast(@hm3^@index as varchar(7))
		set @hash8 = cast(@hm4^@index as varchar(7))
		set @sql += ' union all select id,hm1,hm2,hm3,hm4 from tb_' + @hash5 
			+' union all select id,hm1,hm2,hm3,hm4 from tb_' + @hash6 
			+' union all select id,hm1,hm2,hm3,hm4 from tb_' + @hash7 
			+' union all select id,hm1,hm2,hm3,hm4 from tb_' + @hash8 
		set @index = @index * 2
	end

	set @sql = 'select distinct * from (' + @sql + ') as T '
		+'where dbo.bit_count(hm1^' + @hash1 + ')+dbo.bit_count(hm2^' + @hash2 + ')+dbo.bit_count(hm3^' + @hash3 + ')+dbo.bit_count(hm4^' + @hash4 + ')<=4 '
	execute(@sql)
END
```



### 5、查询测试
```sql
set statistics io on
set statistics time on
set statistics profile on

exec getrows 11,54960,5634,35976
exec getrows2 11,54960,5634,35976
```
