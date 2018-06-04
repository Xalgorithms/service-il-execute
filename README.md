[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![Build Status](https://www.travis-ci.com/Xalgorithms/service-il-execute.svg?branch=master)](https://www.travis-ci.com/Xalgorithms/service-il-execute)

# Summary

This service is responsible for executing rules against documents. It
receives events on a Kafka topic, determines the appropriate targets
(rule and document), executes the rule and records the
revisions. Unlike some of our other processing, this service is
**not** a Spark job. We figured there was no reason to make rule
execution a Spark job because there is nothing in the execution of a
rule that would benefit (directly) from Spark's capabilities while
suffering Spark's restrictions on how Scala code should be written
(serialization requirements).

