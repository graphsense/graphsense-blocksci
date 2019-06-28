# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

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
