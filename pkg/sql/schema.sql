create table messages (
	"key" uuid NOT NULL,
	"topic" text NOT NULL,
	"offset" BIGSERIAL,
	"uuid" VARCHAR(36) NOT NULL,
	"created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	"payload" JSON DEFAULT NULL,
	"metadata" JSON DEFAULT NULL,
	"transaction_id" xid8 NOT NULL,
	PRIMARY KEY ("transaction_id", "key", "topic")
);
CREATE TABLE IF NOT EXISTS messages_offsets (
	topic VARCHAR(255) NOT NULL,
	consumer_group VARCHAR(255) NOT NULL,
	offset_acked BIGINT,
	key_acked uuid,
	last_processed_transaction_id xid8 NOT NULL,
	PRIMARY KEY(consumer_group, topic)
);