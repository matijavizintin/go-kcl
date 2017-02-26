# Examples

To try the examples you will need an AWS account and an instance of [Aerospike](http://www.aerospike.com/docs/operations/install). 
Set the required environment variables and run the writer that will create and fill the Kinesis stream.
```
go run writer.go
```

**NOTE: don't forget to delete the created stream to avoid extra costs.**

Now that your stream is filling up you can start reading from the stream. 
Set the required environment variables and run the selected reader.
```
go run <CHOSEN_READER>.go
```