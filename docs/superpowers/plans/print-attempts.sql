-- Print attempts: apply by hand with psql, in this order.
--
-- print_jobs.arrived is "timestamp with time zone" in the contest database, so
-- the timestamp columns below are timestamptz. Check with \d print_jobs if
-- yours differs. print_jobs itself is not altered.

BEGIN;

CREATE TABLE print_renders (
    id       bigserial   PRIMARY KEY,
    job_id   bigint      NOT NULL,
    format   text        NOT NULL,
    pages    integer     NOT NULL,
    data     bytea       NOT NULL,
    created  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE print_attempts (
    id              bigserial PRIMARY KEY,
    job_id          bigint    NOT NULL,
    kind            text      NOT NULL,
    created         timestamptz NOT NULL DEFAULT now(),
    requested_by    text,
    printer         text      NOT NULL,
    render_id       bigint,
    status          text      NOT NULL,
    finished        timestamptz,
    pages_printed   integer   NOT NULL DEFAULT 0,
    spooler_job_id  bigint,
    error           text      NOT NULL DEFAULT '',
    detail          text      NOT NULL DEFAULT ''
);

ALTER TABLE print_renders
    ADD CONSTRAINT print_renders_job_fk FOREIGN KEY (job_id) REFERENCES print_jobs (id),
    ADD CONSTRAINT print_renders_format_ck CHECK (format IN ('ps', 'pdf'));

ALTER TABLE print_attempts
    ADD CONSTRAINT print_attempts_job_fk FOREIGN KEY (job_id) REFERENCES print_jobs (id),
    ADD CONSTRAINT print_attempts_render_fk FOREIGN KEY (render_id) REFERENCES print_renders (id),
    ADD CONSTRAINT print_attempts_kind_ck CHECK (kind IN ('full', 'print_only')),
    ADD CONSTRAINT print_attempts_status_ck CHECK (status IN ('queued', 'rendered', 'printed', 'failed'));

CREATE INDEX print_attempts_job_idx ON print_attempts (job_id);
CREATE INDEX print_renders_job_idx ON print_renders (job_id);

COMMIT;

-- Rollback:
--   DROP TABLE print_attempts;
--   DROP TABLE print_renders;
