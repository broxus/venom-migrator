CREATE TABLE token_transfers
(
    transaction_hash            VARCHAR(64)        NOT NULL,
    transaction_lt              NUMERIC            NOT NULL,
    transaction_time            NUMERIC            NOT NULL,
    recipient_wc                INT                NOT NULL,
    recipient_account           VARCHAR(64)        NOT NULL,
    value                       NUMERIC            NOT NULL,
    ticker                      VARCHAR            NOT NULL,
    source_token_root_wc        INT                NOT NULL,
    source_token_root_account   VARCHAR(64)        NOT NULL,
    target_token_root_wc        INT                NOT NULL,
    target_token_root_account   VARCHAR(64)        NOT NULL,
    source_token_wallet_wc      INT                NOT NULL,
    source_token_wallet_account VARCHAR(64)        NOT NULL,
    target_token_wallet_wc      INT                NOT NULL,
    target_token_wallet_account VARCHAR(64)        NOT NULL,
    status                      transaction_status NOT NULL,
    sending_message_hash        VARCHAR(64),
    expired_at                  NUMERIC,
    sent_transaction_hash       VARCHAR(64),
    created_at                  TIMESTAMP          NOT NULL DEFAULT current_timestamp,
    updated_at                  TIMESTAMP          NOT NULL DEFAULT current_timestamp,

    CONSTRAINT token_transfers_pk PRIMARY KEY (transaction_hash)
);
