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
  `product_id` BIGINT DEFAULT NULL,
  `create_date` datetime NOT NULL,
  PRIMARY KEY (`item_id`),
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


insert into product_category(product_category_id,category_info,create_date)values(1,'衣服类',CURRENT_DATE);
insert into product_category(product_category_id,category_info,create_date)values(2,'食品类',CURRENT_DATE);
insert into product_category(product_category_id,category_info,create_date)values(3,'数码类',CURRENT_DATE);

insert into product(product_id,product_category_id,product_name,create_date)values(1,1,'白色舒适简约短袖T恤',CURRENT_DATE);
insert into product(product_id,product_category_id,product_name,create_date)values(2,1,'森马藏蓝色个性印图圆领短袖T恤',CURRENT_DATE);
insert into product(product_id,product_category_id,product_name,create_date)values(3,1,'白色舒适简约短袖T恤',CURRENT_DATE);
insert into product(product_id,product_category_id,product_name,create_date)values(4,2,'慕丝妮美味猴头菇饼干720g 酥性饼干 酥脆营养 休闲零食好糕点，好饼',CURRENT_DATE);
insert into product(product_id,product_category_id,product_name,create_date)values(5,2,'ABD芒果饼1000g整箱传统糕点新鲜芒果肉美食小吃小零食特产点心',CURRENT_DATE);
insert into product(product_id,product_category_id,product_name,create_date)values(6,2,'【葡记 小熊巧克力灌心饼800g】 夹心饼干办公休闲儿童零食大礼包好糕',CURRENT_DATE);
insert into product(product_id,product_category_id,product_name,create_date)values(7,3,'适马 SIGMA 24-70mm F2.8 IF EX DG HSM 佳能口',CURRENT_DATE);
insert into product(product_id,product_category_id,product_name,create_date)values(8,3,'360智能摄像机 夜视版 摄像头小水滴 ( 哑白色 )',CURRENT_DATE);
insert into product(product_id,product_category_id,product_name,create_date)values(9,3,'DJI大疆精灵Phantom 4新一代一体式智能',CURRENT_DATE);



insert into customers(id,rand_id,name,customer_info,birthdate) values (customer_seq.nextval, customer_seq.currval,'宋江','金牌会员','1985-01-01');
insert into address(address_id,customer_id,address_info,zip_code,phone_num) values (address_seq.nextval, customer_seq.currval,'西藏·然乌湖','545254','65463546');
insert into address(address_id,customer_id,address_info,zip_code,phone_num) values (address_seq.nextval, customer_seq.currval,'阿坝州·四姑娘山','455475','65465465');

insert into orders(order_id,customer_id,order_info,create_date)values(order_seq.nextval,customer_seq.currval,'宋江定单01',CURRENT_DATE);
insert into order_items(item_id,order_id,product_id,create_date)values(address_seq.nextval,order_seq.currval,5,CURRENT_DATE);
insert into order_status(status_id,order_id,order_status,create_date)values(address_seq.nextval,order_seq.currval,'0',CURRENT_DATE);

insert into orders(order_id,customer_id,order_info,create_date)values(order_seq.nextval,customer_seq.currval,'宋江定单02',CURRENT_DATE);
insert into order_items(item_id,order_id,product_id,create_date)values(address_seq.nextval,order_seq.currval,4,CURRENT_DATE);
insert into order_status(status_id,order_id,order_status,create_date)values(address_seq.nextval,order_seq.currval,'0',CURRENT_DATE);

insert into orders(order_id,customer_id,order_info,create_date)values(order_seq.nextval,customer_seq.currval,'宋江定单03',CURRENT_DATE);
insert into order_items(item_id,order_id,product_id,create_date)values(address_seq.nextval,order_seq.currval,1,CURRENT_DATE);
insert into order_items(item_id,order_id,product_id,create_date)values(address_seq.nextval,order_seq.currval,2,CURRENT_DATE);
insert into order_status(status_id,order_id,order_status,create_date)values(address_seq.nextval,order_seq.currval,'0',CURRENT_DATE);

insert into orders(order_id,customer_id,order_info,create_date)values(order_seq.nextval,customer_seq.currval,'宋江定单04',CURRENT_DATE);
insert into order_items(item_id,order_id,product_id,create_date)values(address_seq.nextval,order_seq.currval,3,CURRENT_DATE);
insert into order_status(status_id,order_id,order_status,create_date)values(address_seq.nextval,order_seq.currval,'1',CURRENT_DATE);

insert into orders(order_id,customer_id,order_info,create_date)values(order_seq.nextval,customer_seq.currval,'宋江定单05',CURRENT_DATE);
insert into order_items(item_id,order_id,product_id,create_date)values(address_seq.nextval,order_seq.currval,7,CURRENT_DATE);
insert into order_status(status_id,order_id,order_status,create_date)values(address_seq.nextval,order_seq.currval,'1',CURRENT_DATE);

insert into customers(id,rand_id,name,customer_info,birthdate) values (customer_seq.nextval, customer_seq.currval,'卢俊义','金牌会员','1987-05-01');
insert into address(address_id,customer_id,address_info,zip_code,phone_num) values (address_seq.nextval, customer_seq.currval,'山东·烟墩角','6542','326654654');
insert into address(address_id,customer_id,address_info,zip_code,phone_num) values (address_seq.nextval, customer_seq.currval,'新疆·果子沟','5454','12654654654');
insert into address(address_id,customer_id,address_info,zip_code,phone_num) values (address_seq.nextval, customer_seq.currval,'云南·坝美','5429','54654285');

insert into orders(order_id,customer_id,order_info,create_date)values(order_seq.nextval,customer_seq.currval,'卢俊义定单01',CURRENT_DATE);
insert into order_items(item_id,order_id,product_id,create_date)values(address_seq.nextval,order_seq.currval,3,CURRENT_DATE);
insert into order_status(status_id,order_id,order_status,create_date)values(address_seq.nextval,order_seq.currval,'0',CURRENT_DATE);

insert into orders(order_id,customer_id,order_info,create_date)values(order_seq.nextval,customer_seq.currval,'卢俊义定单02',CURRENT_DATE);
insert into order_items(item_id,order_id,product_id,create_date)values(address_seq.nextval,order_seq.currval,8,CURRENT_DATE);
insert into order_status(status_id,order_id,order_status,create_date)values(address_seq.nextval,order_seq.currval,'0',CURRENT_DATE);

insert into orders(order_id,customer_id,order_info,create_date)values(order_seq.nextval,customer_seq.currval,'卢俊义定单03',CURRENT_DATE);
insert into order_items(item_id,order_id,product_id,create_date)values(address_seq.nextval,order_seq.currval,7,CURRENT_DATE);
insert into order_items(item_id,order_id,product_id,create_date)values(address_seq.nextval,order_seq.currval,3,CURRENT_DATE);
insert into order_status(status_id,order_id,order_status,create_date)values(address_seq.nextval,order_seq.currval,'0',CURRENT_DATE);


insert into customers(id,rand_id,name,customer_info,birthdate) values (customer_seq.nextval, customer_seq.currval,'吴用','银牌会员','1970-06-01');
insert into address(address_id,customer_id,address_info,zip_code,phone_num) values (address_seq.nextval, customer_seq.currval,'甘孜州·丹巴','5422','69355356');

insert into orders(order_id,customer_id,order_info,create_date)values(order_seq.nextval,customer_seq.currval,'吴用定单01',CURRENT_DATE);
insert into order_items(item_id,order_id,product_id,create_date)values(address_seq.nextval,order_seq.currval,7,CURRENT_DATE);
insert into order_items(item_id,order_id,product_id,create_date)values(address_seq.nextval,order_seq.currval,3,CURRENT_DATE);
insert into order_status(status_id,order_id,order_status,create_date)values(address_seq.nextval,order_seq.currval,'0',CURRENT_DATE);

insert into customers(id,rand_id,name,customer_info,birthdate) values (customer_seq.nextval, customer_seq.currval,'公孙胜','银牌会员','1980-07-01');
insert into address(address_id,customer_id,address_info,zip_code,phone_num) values (address_seq.nextval, customer_seq.currval,'浙江·磐安','54475','6332474');



insert into customers(id,rand_id,name,customer_info,birthdate) values (customer_seq.nextval, customer_seq.currval,'关胜','铜牌会员','1932-03-01');
insert into address(address_id,customer_id,address_info,zip_code,phone_num) values (address_seq.nextval, customer_seq.currval,'福建·霞浦','54245','654148');

insert into orders(order_id,customer_id,order_info,create_date)values(order_seq.nextval,customer_seq.currval,'关胜定单01',CURRENT_DATE);
insert into order_items(item_id,order_id,product_id,create_date)values(address_seq.nextval,order_seq.currval,5,CURRENT_DATE);
insert into order_items(item_id,order_id,product_id,create_date)values(address_seq.nextval,order_seq.currval,2,CURRENT_DATE);
insert into order_status(status_id,order_id,order_status,create_date)values(address_seq.nextval,order_seq.currval,'0',CURRENT_DATE);

insert into orders(order_id,customer_id,order_info,create_date)values(order_seq.nextval,customer_seq.currval,'关胜定单02',CURRENT_DATE);
insert into order_items(item_id,order_id,product_id,create_date)values(address_seq.nextval,order_seq.currval,1,CURRENT_DATE);
insert into order_items(item_id,order_id,product_id,create_date)values(address_seq.nextval,order_seq.currval,3,CURRENT_DATE);
insert into order_status(status_id,order_id,order_status,create_date)values(address_seq.nextval,order_seq.currval,'0',CURRENT_DATE);

insert into orders(order_id,customer_id,order_info,create_date)values(order_seq.nextval,customer_seq.currval,'关胜定单03',CURRENT_DATE);
insert into order_items(item_id,order_id,product_id,create_date)values(address_seq.nextval,order_seq.currval,7,CURRENT_DATE);
insert into order_items(item_id,order_id,product_id,create_date)values(address_seq.nextval,order_seq.currval,6,CURRENT_DATE);
insert into order_status(status_id,order_id,order_status,create_date)values(address_seq.nextval,order_seq.currval,'0',CURRENT_DATE);




