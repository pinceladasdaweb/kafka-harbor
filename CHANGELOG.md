# CHANGELOG

## 0.3.0 (2026-09-07)

* feat: share retry topics by naming; document the Docker images the Confluent client installs on by Pedro Rogério [View](https://github.com/pinceladasdaweb/kafka-harbor/commit/eec568e239131e69779944ed7f8ed602fcf1e867)
* docs: explain the duration format every time option accepts by Pedro Rogério [View](https://github.com/pinceladasdaweb/kafka-harbor/commit/4943ff34dd256b339aecf465f21ef71baab00e75)


## 0.2.0 (2026-09-06)

* fix: one member per retry level, bounded shutdown waits, no self-redrive by Pedro Rogério [View](https://github.com/pinceladasdaweb/kafka-harbor/commit/cbc35c9201e1da355b9b31692961ceca49f08770)
* fix(testing): contract covers tombstones and pause scope; memory broker stops hanging by Pedro Rogério [View](https://github.com/pinceladasdaweb/kafka-harbor/commit/af3175a942c72f97f48ad9c75bec49744fff7185)
* fix(confluent): guard the producer guarantees, classify every refusal, honour logLevel by Pedro Rogério [View](https://github.com/pinceladasdaweb/kafka-harbor/commit/82fbb4040dff383de8f0a6ce1841aa4b6a69e806)
* fix: close the shutdown race, let revoked and abandoned work finish, share one pipeline core by Pedro Rogério [View](https://github.com/pinceladasdaweb/kafka-harbor/commit/8bb1b7ca9ef681bb98d92b32abbd8862ee50cbde)


## 0.1.0 (2026-09-05)

* feat: foundation with core, confluent adapter and memory broker by Pedro Rogério [View](https://github.com/pinceladasdaweb/kafka-harbor/commit/5cd6e31b1721f8070f561f849e885b8b9d820b3a)
* feat: health snapshot and Kafka default partitioner by Pedro Rogério [View](https://github.com/pinceladasdaweb/kafka-harbor/commit/1c1e88967d39e1c14e098c5b76856cb0869964cb)
* feat: redrive dead letters back into service by Pedro Rogério [View](https://github.com/pinceladasdaweb/kafka-harbor/commit/367b25947ecaf5fcc1241fed1b53f1dbbf2a048e)
* test: basic rebalancing and end-to-end redrive on a real broker by Pedro Rogério [View](https://github.com/pinceladasdaweb/kafka-harbor/commit/2d9a17627ff9edd138cf48c95588e014bc917b6a)
* test: cover the remaining redrive branches and reword two file headers by Pedro Rogério [View](https://github.com/pinceladasdaweb/kafka-harbor/commit/99d6f6c4551919006ccd7734ae15aca74ed47cf8)
* test: health covers abort; memory broker refuses invalid topic names by Pedro Rogério [View](https://github.com/pinceladasdaweb/kafka-harbor/commit/b95c04dd7f4d7e1342515be4d398625094e26fb1)
* feat: export the adapter contract suite from kafka-harbor/testing by Pedro Rogério [View](https://github.com/pinceladasdaweb/kafka-harbor/commit/28d377ecefc13e6589912cb5def88ab6eaaa1ced)
* docs: type-check every snippet, compare against the clients, add examples by Pedro Rogério [View](https://github.com/pinceladasdaweb/kafka-harbor/commit/ef8252c62ff01c573e2dbc55278dff52689bce78)
* docs: describe only what ships; drop the quayside mention and every promise of future work by Pedro Rogério [View](https://github.com/pinceladasdaweb/kafka-harbor/commit/d31beed723779b957f8588dcb63a7194ba57c324)
* ci: gate the npm publish job behind the NPM_PUBLISH_ENABLED repository variable by Pedro Rogério [View](https://github.com/pinceladasdaweb/kafka-harbor/commit/44cd613efa616d84065a6cbafb4795b18626a819)
* ci: keep dependabot off the eslint and typescript majors the toolchain cannot take yet by Pedro Rogério [View](https://github.com/pinceladasdaweb/kafka-harbor/commit/d45d9005ff167422616599900851271037668462)
* fix: createTopics resolves only once the topics are visible in metadata by Pedro Rogério [View](https://github.com/pinceladasdaweb/kafka-harbor/commit/d976ee462e3369c00d6d4fc5817f03ae7517d0a2)
