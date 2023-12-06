# Phragmen Election Candidacy Reserve Scraper

Script to help resolve https://github.com/paritytech/polkadot-sdk/issues/2507.

Scrapes every candidacy-related reserve / unreserve event from the chain and saves in a sqlite db.

Once all scraped, writes out a csv of any imbalances.

Supports Polkadot and Kusama.
