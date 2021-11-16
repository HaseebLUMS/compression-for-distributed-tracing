# compression-for-distributed-tracing
A compression technique motivated by the semantic characteristics of microservices' traces

## Initial results:
Percentage decrease in the number of stored spans = <b> 28% </b> <br />
Percentage decrease in the number of stored fields (key value pairs extracted from logs) = <b> 88% </b> <br />
Percentage decrease in the storage (in MBs) = <b> 70.8% </b> <br />

Steps:
1) Follow the [article](https://medium.com/opentracing/take-opentracing-for-a-hotrod-ride-f6e3141f7941) for setting up distributed tracing and hosting a microservices site
2) Visit the hosted site and do some actions that will generate some traces(in future, this step will be automated)
3) Setup MySQL and create two empty databases: "original" and "compressed" (In future, this step will be automated)
3) Run `run.py`
    3.1) The traces will get stored in a newly created directory `traces`
    3.2) The two databases will get populated
    3.3) The original db will have all the traces in full form
    3.4) The compressed db will also have all traces but in the compressed form.  