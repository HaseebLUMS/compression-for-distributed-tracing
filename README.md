# compression-for-distributed-tracing
A compression technique motivated by the semantic characteristics of microservices' traces

## Initial results:
Percentage decrease in the number of stored spans = 28%
Percentage decrease in the number of stored fields (key value pairs extracted from logs) = 88%
Percentage decrease in the storage (in MBs) = 70.8%

Steps:
1) Follow the [article](https://medium.com/opentracing/take-opentracing-for-a-hotrod-ride-f6e3141f7941) for setting up distributed tracing and hosting a microservices site
2) Visit to the hosted site and do some actions (in future, I will automate this step)
3) Run `extract-traces.py` and traces should get stored in a newly created directory `traces`