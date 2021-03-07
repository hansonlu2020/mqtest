#!/bin/bash
#usage java -jar a1.jar host port exchange queue routingkey no_ack consumer_count
java -jar a1.jar 127.0.0.1 5672 exchange_test x1 rk1 1 1 &
