# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## Unreleased
### Changed
- Updated Python dependencies (`requirements.txt`)
### Fixed
- Fixed scraping of CoinMarketCap exchange rates

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
