#!/bin/bash 
#usage java -jar c1.jar host port exchange routingkey msg_cnt interval
java -jar c1.jar 127.0.0.1 5672 exchange_test rk1 100 100 &
