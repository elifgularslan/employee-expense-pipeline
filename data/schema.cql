CREATE KEYSPACE IF NOT EXISTS sau_pro3
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS sau_pro3.expenses (
    empno int,
    topic text,
    kafka_partition int,
    kafka_offset bigint,
    occurred_at timestamp,
    description text,
    expense_type text,
    quantity int,
    payment decimal,
    PRIMARY KEY ((empno), topic, kafka_partition, kafka_offset)
);
