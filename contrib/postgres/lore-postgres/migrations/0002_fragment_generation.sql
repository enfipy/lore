-- Copyright 2026 David
-- SPDX-License-Identifier: MIT

CREATE SEQUENCE fragment_generation_seq AS BIGINT;

ALTER TABLE fragment_state
    ADD COLUMN generation BIGINT NOT NULL DEFAULT nextval('fragment_generation_seq');

ALTER SEQUENCE fragment_generation_seq OWNED BY fragment_state.generation;
