CREATE TABLE raw_transactions (
    transaction_hash            VARCHAR(64) NOT NULL,
    transaction_lt              NUMERIC NOT NULL,
    transaction_time            NUMERIC NOT NULL,
    account_wc                  INT NOT NULL,
    account                     VARCHAR(64) NOT NULL,
    transaction_boc             BYTEA NOT NULL,
    status                      VARCHAR NOT NULL,
    skip_reason                 VARCHAR,
    created_at                  TIMESTAMP NOT NULL DEFAULT current_timestamp,
    updated_at                  TIMESTAMP NOT NULL DEFAULT current_timestamp,

    CONSTRAINT raw_transactions_pk PRIMARY KEY (transaction_hash)
);
