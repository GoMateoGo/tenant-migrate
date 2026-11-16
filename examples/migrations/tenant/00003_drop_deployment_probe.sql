-- +goose Up
ALTER TABLE migration_example DROP COLUMN deployment_probe;

-- +goose Down
ALTER TABLE migration_example ADD COLUMN deployment_probe text;
