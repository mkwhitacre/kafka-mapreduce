Overview
========

For some reason a reusable and extensible Kafka InputFormat does not exist.  With Kafka 0.9.x.x release,
the InputFormat becomes even easier to do.


Usage
=========

More detailed instructions on using this will be coming but the short gist is consumers will be responsible for
retrieving the start and end offsets for Kafka partitions from which data will be retrieved. 


Contributors
========

Special shout out to [Andrew Olson](https://github.com/noslowerdna) and [Bryan Baugher](https://github.com/bbaugher) who
helped to provide the testing infrastructure code for getting Kafka setup easily.  Also for some design advice.