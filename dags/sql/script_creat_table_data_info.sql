CREATE TABLE IF NOT EXISTS data_info_v3 (
	url varchar(50) primary key not null,
	file_name varchar(50),
	year_data INT ,
	month_data INT,
	status_data varchar(10)
);
