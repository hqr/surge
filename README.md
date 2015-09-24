# Overview

SURGE is a discrete event simulation framework written in Go. Targeted
modeling area includes (but is not limited to) large and super-large storage
clusters with multiple access points (referred to as "gateways") connected to
both user/application and storage networks.

The second simulated type of a clustered node is a storage target aka "server".
Both "gateway" and "server" are fundamental abstractions with a great variety
of realizations in real world of distributed clusters where "gateways"
typically are responsible for the first stages of IO pipeline (including
chunking/striping, checkumming and distributing user content), while "servers"
provide the ultimate stable storage. Sometimes the two are colocated on the
same physical or virtual machines.
						  
Each storage node (gateway or server) in the SURGE is a separate
lightweight thread: a goroutine. The framework connects all configured nodes
bidirectionally via a pair of per node (Tx, Rx) Go channels. At each model's
startup all clustered nodes (of this model) get automatically connected and
ready to Go: send, receive and handle events and IO requests from all other
nodes.

All models are named. There are currently 4 built-in models named "one", "two",
"three" and "four" respectively, provided both for illustration purposes as well
as self-testing reasons. The Usage section below has some examples and options
to run and test any/all of these built-in models with all supported command-line
options, including numbers of gateways and servers.

Overall, the idea and the motivation to do this framework and concrete models
based on it comes out of:

* unanswered questions also sometimes called ideas
* chronic lack of hardware to run massive benchmarks
* and of course, total lack of months and years to build/validate the former and
then run the latter 

# Services

The SURGE framework currently provides the following initial services:

* Time 

Each time a model starts running, the (modeled) time starts from 0 (literal zero
- not to be confused with 00:00:00 1/1/1970) and then advances forward in
(configurable) steps. Each step of the internal global universal timer is
currently set to 10 nanoseconds, which also means that the margin of error to
execute a given timed event can be up to 9 nanoseconds (finer time-stepping
granularity may slow down your simulated benchmarks..)

The framework enforces the following simple rule: each event scheduled to
trigger at a given time does get executed at approximately this time with a very
high level of precision (see above). This statement may sound a bit circular but
the consequence of this is that modeled NOW does not advance until all the
events scheduled at NOW do in fact execute. Which is exactly how the time is (or
at least supposed to be) in a real world unless we lose track of it..

* Logging

By default the logger will log into a file named /tmp/log.csv. Both the filename and the
verbosity levels from quiet (default) to super-super-verbose 'vvv' - are
configurable.

* Statistics

There is initial generic support for per-model custom counters. Each new
counter is declared and registered at a model's init time via StatsDescriptor:

```go
type StatsDescriptor struct {
	name  string
	kind  StatsKindEnum
	scope StatsScopeEnum
}
```

Once registered, the framework will keep track of summing it up and/or
averaging, depending on the kind and scope of the counter. For this to happen,
the modeled gateways and servers provide GetStats() method as per examples
in the code (- grep for "\"event"\" and "\"tio"\")

* Multi-tasking and concurrency

Accomplished via Go routines and Go channels. Ultimately, each concrete model
must provide NewGateway()/NewServer() constructors for its own clustered nodes
and their specific Run() methods. It is also expected that at startup the
gateways start generating traffic (thus simulating the user/application IOs that
would arrive at the gateways of real clusters). The rest is done by the
SURGE framework, at least that's the intent.

* Reusable code

Much of the common routine is offloaded to a basic class (type) called
RunnerBase. The latter provides utility functions and implements part of the
abstract interfaces that all clustered gateways, servers, and in the future -
(remote) disks and network switches - must implement as well..

# Usage

To run all built-in models, one after another back to back:

```go
package main

import "surge"

func main() {
	surge.RunAllModels()
}
```

Use -h or --help to list command-line options; example below assumes the code
that contains main() (see e.g. above) is named example.go:

```
$ go run cmd/ck.go -h
$ go run cmd/ck.go --help
```

If you don't want to build/install surge package, run it using 'go test'
from the local project's directory:

```
$ cd surge
$ go test -m three -servers 20 -gateways 10 -v
```
The example runs a certain built-in model named 'three' with 20
simulated servers, 10 gateways and (normal) log level verbosity '-v'.
The same can be accomplished via:

```
$ go run cmd/ck.go -m three -servers 20 -gateways 10 -v
```
