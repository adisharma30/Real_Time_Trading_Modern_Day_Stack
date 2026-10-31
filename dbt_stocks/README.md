# dbt Stocks warehouse

This dbt project transforms stock quote events in Snowflake into normalized
and analytics-ready models. It uses the `dbt_stocks` profile and declares its
raw input in the `STOCKS_MDS.COMMON` schema.

## Model layers

- **Bronze** (`models/bronze/`): reads the raw JSON quote table and extracts
  typed quote fields such as symbol, current price, daily high/low, percentage
  change, market timestamp, and fetch timestamp.
- **Silver** (`models/silver/`): keeps usable quote rows and rounds selected
  price and percentage fields for downstream use.
- **Gold** (`models/gold/`): selects the most recently fetched quote for each
  symbol, exposing current price and daily change metrics.

The raw source table is declared in `models/bronze/source.yaml`. The project
also includes the default dbt example models under `models/example/`.

## Configure a Snowflake profile

Create `infra/secrets/profiles.yml` locally with a profile named
`dbt_stocks`. Configure Snowflake connection details and authentication for
your own account. The profile, private key, and any passphrase must remain
outside version control.

The configured role needs permission to read the `STOCKS_MDS.COMMON` raw
source and create the schemas and models required by the dbt project.

## Useful commands

Run these commands from the `dbt_stocks/` directory:

```powershell
dbt debug --profiles-dir ..\infra\secrets
dbt ls --profiles-dir ..\infra\secrets
dbt build --profiles-dir ..\infra\secrets
```

`dbt build` builds applicable models and runs their configured tests in
dependency order. The project configuration currently materializes the
default `example` models as views; set model materializations explicitly if
you want bronze, silver, or gold persisted as tables.

## Project files

- `dbt_project.yml`: project name, profile, model paths, and materialization
  configuration
- `models/bronze/source.yaml`: Snowflake raw source declaration
- `models/bronze/`: raw JSON normalization
- `models/silver/`: cleaned quote records
- `models/gold/`: latest quote per symbol
- `target/`, `logs/`, and `dbt_packages/`: generated dbt output, not source
