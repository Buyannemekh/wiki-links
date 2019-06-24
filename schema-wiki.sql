CREATE TABLE  links(
	article_id int NOT NULL,
	link_name varchar NOT NULL,
	first_time_stamp timestamp NOT NULL,
	deleted_time_stamp timestamp DEFAULT NULL,
	PRIMARY KEY (article_id, link_name)
)

