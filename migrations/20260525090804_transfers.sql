DROP TYPE IF EXISTS transaction_status;

CREATE TYPE transaction_status as ENUM (
    'New',
    'Pending',
    'Expired',
    'Done',
    'Failed'
);

CREATE TABLE transfers (
    transaction_hash            VARCHAR(64) NOT NULL,
    transaction_lt              NUMERIC NOT NULL,
    transaction_time            NUMERIC NOT NULL,
    sender_wc                   INT NOT NULL,
    sender_account              VARCHAR(64) NOT NULL,
    recipient_wc                INT NOT NULL,
    recipient_account           VARCHAR(64) NOT NULL,
    value                       NUMERIC NOT NULL,
    status                      transaction_status NOT NULL,
    sending_message_hash        VARCHAR(64),
    expired_at                  NUMERIC,
    sent_transaction_hash       VARCHAR(64),
    created_at                  TIMESTAMP NOT NULL DEFAULT current_timestamp,
    updated_at                  TIMESTAMP NOT NULL DEFAULT current_timestamp,

    CONSTRAINT transfers_pk PRIMARY KEY (transaction_hash)
);
