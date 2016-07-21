DROP TABLE IF EXISTS customers,address,order_items,order_status,orders,product,product_category,customer_login_log;


CREATE TABLE IF NOT EXISTS `customers` (
  `id` BIGINT NOT NULL,
  `rand_id` BIGINT DEFAULT NULL,
  `name` varchar(20) DEFAULT NULL,
  `customer_info` varchar(100) DEFAULT NULL,
  `birthdate` date DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY (`birthdate`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `address` (
  `address_id` BIGINT NOT NULL,
  `customer_id` BIGINT DEFAULT NULL,
  `address_info` varchar(512) DEFAULT NULL,
  `zip_code` varchar(16) DEFAULT NULL,
  `phone_num` varchar(16) DEFAULT NULL,
  PRIMARY KEY (`address_id`),
  KEY (`customer_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE IF NOT EXISTS `orders` (
  `order_id` BIGINT NOT NULL,
  `customer_id` BIGINT NOT NULL,
  `order_info` varchar(218) DEFAULT NULL,
  `create_date` datetime NOT NULL,
  PRIMARY KEY (`order_id`),
  KEY (`customer_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `order_items` (
  `item_id` BIGINT NOT NULL,
  `order_id` BIGINT NOT NULL,
  `item_info` varchar(218) DEFAULT NULL,
  `create_date` datetime NOT NULL,
  PRIMARY KEY (`order_id`),
  KEY (`create_date`),
  FOREIGN KEY (`order_id`) REFERENCES `orders` (`order_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `order_status` (
  `status_id` BIGINT NOT NULL,
  `order_id` BIGINT NOT NULL,
  `order_status` int(2) DEFAULT NULL,
  `create_date` datetime NOT NULL,
  PRIMARY KEY (`order_id`),
  KEY (`create_date`),
  FOREIGN KEY (`order_id`) REFERENCES `orders` (`order_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `product_category` (
  `product_category_id` BIGINT NOT NULL,
  `category_info` varchar(218) DEFAULT NULL,
  `create_date` datetime NOT NULL,
  PRIMARY KEY (`product_category_id`),
  KEY (`create_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `product` (
  `product_id` BIGINT NOT NULL,
  `product_category_id` BIGINT NOT NULL,
  `product_name` varchar(218) DEFAULT NULL,
  `create_date` datetime NOT NULL,
  PRIMARY KEY (`product_id`),
  KEY (`create_date`),
  FOREIGN KEY (`product_category_id`) REFERENCES `product_category` (`product_category_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE IF NOT EXISTS `customer_login_log` (
  `id` BIGINT NOT NULL,
  `customer_id` BIGINT NOT NULL,
  `logintime` datetime NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


insert into customers(id,rand_id,name,customer_info,birthdate) values (customer_seq.nextval, customer_seq.currval,'宋江','金牌会员','1985-01-01');
insert into address(address_id,customer_id,address_info,zip_code,phone_num) values (address_seq.nextval, customer_seq.currval,'西藏·然乌湖','545254','65463546');
insert into address(address_id,customer_id,address_info,zip_code,phone_num) values (address_seq.nextval, customer_seq.currval,'阿坝州·四姑娘山','455475','65465465');

insert into customers(id,rand_id,name,customer_info,birthdate) values (customer_seq.nextval, customer_seq.currval,'卢俊义','金牌会员','1987-05-01');
insert into address(address_id,customer_id,address_info,zip_code,phone_num) values (address_seq.nextval, customer_seq.currval,'山东·烟墩角','6542','326654654');
insert into address(address_id,customer_id,address_info,zip_code,phone_num) values (address_seq.nextval, customer_seq.currval,'新疆·果子沟','5454','12654654654');
insert into address(address_id,customer_id,address_info,zip_code,phone_num) values (address_seq.nextval, customer_seq.currval,'云南·坝美','5429','54654285');

insert into customers(id,rand_id,name,customer_info,birthdate) values (customer_seq.nextval, customer_seq.currval,'吴用','银牌会员','1970-06-01');
insert into address(address_id,customer_id,address_info,zip_code,phone_num) values (address_seq.nextval, customer_seq.currval,'甘孜州·丹巴','5422','69355356');

insert into customers(id,rand_id,name,customer_info,birthdate) values (customer_seq.nextval, customer_seq.currval,'公孙胜','银牌会员','1980-07-01');
insert into address(address_id,customer_id,address_info,zip_code,phone_num) values (address_seq.nextval, customer_seq.currval,'浙江·磐安','54475','6332474');

insert into customers(id,rand_id,name,customer_info,birthdate) values (customer_seq.nextval, customer_seq.currval,'关胜','铜牌会员','1932-03-01');
insert into address(address_id,customer_id,address_info,zip_code,phone_num) values (address_seq.nextval, customer_seq.currval,'福建·霞浦','54245','654148');



