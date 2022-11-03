#!/bin/bash

sh ../src/test/resources/embedded-jg/janusgraph-0.6.2/bin/janusgraph-server.sh console ../custom-gremlin-conf.yaml start &
cat ../src/test/resources/embedded-jg/custom-index.txt | sh ../src/test/resources/embedded-jg/janusgraph-0.6.2/bin/gremlin.sh
