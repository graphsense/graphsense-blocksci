# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [23.09/1.4.0] - 2023-09-20

## [23.06/1.4.0] - 2023-06-12
### Removed
- coindesk and coimarket script exchange rates scripts -> graphsense-lib provides the same features

## [23.03/1.3.0] - 2023-03-29
- no changes

## [23.01/1.3.0] - 2023-01-30
### Added
- standard dev Makefile
### Deprecated
- coindesk and coimarket script -> graphsense-lib provides the same features
- graphsense-setup -> component is deprecated and will be replaced in the future

## [22.11] 2022-11-24

## [22.10] 2022-10-11

## [1.0.1] 2022-08-26
### Fixed
- LTC RPC export

## [1.0.0] 2022-07-08
### Changed
- Disabled core files in Docker container
- Switched to Blocksci `master` branch

## [0.5.2] 2022-03-16
### Changed
- Added `--db_port` argument to export script
- Modified command-line arguments
- Updated Python dependencies

## [0.5.1] 2021-11-29
### Changed
- Improved Cassandra schema
- Store exchange rates for fiat currencies in list column
- Improved handling of missing values in exchange rate ingest script
### Added
- Table `configuration`
- Type hints

## [0.5.0] 2021-05-31
### Changed
- Updated Python dependencies (`requirements.txt`)
### Fixed
- Fixed scraping of CoinMarketCap exchange rates
### Added
- Added --concurrency command-line argument (#17)

## [0.4.5] - 2020-11-16
### Changed
- Upgrade to BlockSci v0.7.0 (requires reparsing)
- Changed command-line arguments of export script
- Upgraded Docker base image to Ubuntu 20.04
- Updated Python dependencies (`requirements.txt`)

## [0.4.4] - 2020-06-12
### Changed
- Updated dependencies (`requirements.txt`)

## [0.4.3] - 2020-05-11
### Changed
- Use `execute_concurrent_with_args` for ingest instead of batch statements
- Fixed column name in Cassandra schema
- Pinned versions numbers for pip packages
- Improved Dockerfile

## [0.4.2] - 2019-12-19
### Changed
- Removed exchange rates ingest from blocksci_export.py script (BlockSci uses the CoinDesk API, which supports only BTC)
- Updated utility script to check `block` and `exchange_rates` table
### Added
- Added scripts to ingest exchange rates from CoinDesk or CoinMarketCap
- Added script to obtain first block height on a specific date

## [0.4.1] - 2019-06-28
### Changed
- Multi-stage Dockerfile to reduce image size
### Added
- Added `--chunks` argparse argument
- Added doctests
### Removed
- Removed `tag` table from raw Cassandra schema ([GraphSense TagPacks](https://github.com/graphsense/graphsense-tagpacks))

## [0.4.0] - 2019-02-01
### Changed
- Fixed exchange rates bug (EUR/USD swapped)
- Refactored multiprocess ingests
- Specified fixed version of Python cassandra-driver
### Added
- Added coinjoin column to transaction table

## [0.3.3] - 2018-11-30
### Changed
- Updated BlockSci to v0.6 branch
