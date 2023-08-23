CREATE KEYSPACE IF NOT EXISTS creditcard
WITH replication = {
'class': 'SimpleStrategy', 'replication_factor': 1
};

USE creditcard;

CREATE TABLE IF NOT EXISTS fraud_transaction (
  cc_num text,
  trans_time timestamp,
  trans_num text,
  category text,
  merchant text,
  amt double,
  merch_lat double,
  merch_long double,
  distance double,
  age int,
  is_fraud double,
  PRIMARY KEY(cc_num, trans_time)
)WITH CLUSTERING ORDER BY (trans_time DESC);


CREATE TABLE IF NOT EXISTS non_fraud_transaction (
  cc_num text,
  trans_time timestamp,
  trans_num text,
  category text,
  merchant text,
  amt double,
  merch_lat double,
  merch_long double,
  distance double,
  age int,
  is_fraud double,
  PRIMARY KEY(cc_num, trans_time)
)WITH CLUSTERING ORDER BY (trans_time DESC);





CREATE TABLE IF NOT EXISTS customer (
  cc_num text,
  first text,
  last text,
  gender text,
  street text,
  city text,
  state text,
  zip text,
  lat double,
  long double,
  job text,
  dob timestamp,
  min_amt double,
  max_amt double,
  mean_amt double,
  PRIMARY KEY(cc_num)
);


CREATE TABLE IF NOT EXISTS view_top10_latest (
  cc_num text,
  trans_time timestamp,
  trans_num text,
  category text,
  merchant text,
  amt double,
  PRIMARY KEY(cc_num, trans_num)
);

CREATE TABLE IF NOT EXISTS view_age_count_customer (
  age   int,
  count int,
  PRIMARY KEY(age)
);

CREATE TABLE IF NOT EXISTS view_gender_count_customer (
  gender text,
  count  int,
  PRIMARY KEY(gender)
);

CREATE TABLE IF NOT EXISTS view_transaction_by_category (
  category text,
  count int,
  PRIMARY KEY(category)
);

CREATE TABLE IF NOT EXISTS view_fraud_transaction_by_category_year (
  category text,
  year int,
  count int,
  PRIMARY KEY( category,year )
);

CREATE TABLE IF NOT EXISTS view_fraud_transaction_by_month (
  month int,
  count int,
  PRIMARY KEY(month)
);



CREATE TABLE IF NOT EXISTS eventtransaction (
  cc_num text,
  trans_time timestamp,
  trans_num text,
  category text,
  merchant text,
  amt double,
  merch_lat double,
  merch_long double,
  distance double,
  PRIMARY KEY(cc_num, trans_time)
)WITH CLUSTERING ORDER BY (trans_time DESC);


