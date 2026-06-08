CREATE INDEX transfers_search_created_at_idx
    ON transfers (created_at, transaction_hash);

CREATE INDEX transfers_search_sender_created_at_idx
    ON transfers (sender_wc, sender_account, created_at, transaction_hash);

CREATE INDEX token_transfers_search_created_at_idx
    ON token_transfers (created_at, transaction_hash);

CREATE INDEX token_transfers_search_sender_created_at_idx
    ON token_transfers (sender_wc, sender_account, created_at, transaction_hash);
