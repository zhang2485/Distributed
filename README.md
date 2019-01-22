# Distributed File System

Our MP implements the following algorithm for membership list and failure
detection:

## Topology

The topology of our system uses a circular topology where each node's neighbors
are its two surrounding indexes (ie. For a membership list of [0, 1, 2, 3, 4,
5], member 1's neighbors are {5, 0, 2, 3}).

## Design

There are two categories of threads running at every node. 
1) Ping/ACK - this pair of threads handles failure detection and updates the
membership lists of its neighbors depending on if it detects a failure. In the
case of failure detection, it piggybacks its membership list onto its neighbors.
When there is no failure detected, it simply sends a "ping" to its neighbors to
check for their status.
2) Introducer/Connect - this pair of threads handles the connection of new
nodes. It does this by using a known introducer thread that disseminates connect
updates to every other node in its membership list.

## Running the code

### Prerequisites
We are using Java to develop and Ansible to deploy.

### Setting up Ansible 
There are two steps we need to do to use Ansible properly.
1. Change ansible/hosts to match your server ips
2. run ```./install.sh```

### Deploying to VMs
We are using Ansible to deploy our code to the VMs. All the configuration files
used for Ansible are in ```ansible/```. Use ```./deploy.sh``` to deploy code and
start server. Use ```./shutdown.sh``` to shutdown all the servers.

### Making
Simply run ```make``` to compile files.

### Server
```java
java Server
```

### Client
```java
java Client
```
#### Commands
```
quit (ip) # naturally quits machine at ip address
grep (string) # greps string from logs of all machines active
print # prints membership list from all machines
```

**NOTE:** 
All ip addresses are hardcoded, in order to run on different VMs, IP addresses must be changed.

## Measurements

Bandwidth measurements are taken using the following command:
```sudo tcpdump -i eth0 -l -n -nn -p -e "port 2010 or port 2011 or port 2012 or port 2020" | ./tools/netbps.perl```

Packet loss is simulated by dropping a percentage of packets when sending UDP
packets in the code.

#### 2 Machine Measurements

| 2 Machines                                                              | 1       | 2       | 3       | 4       | 5       | Average | Stdev         |
|-------------------------------------------------------------------------|---------|---------|---------|---------|---------|---------|---------------|
| Average background bandwidth usage (Bps)                                | 272.78  | 233.78  | 256.28  | 272.52  | 234.06  | 253.88  | 19.41173563   |
| Average bandwidth usage whenever a node joins (Bps)                     | 296.86  | 277.07  | 278.07  | 277.39  | 277.65  | 281.41  | 8.645687943   |
| Average bandwidth usage whenever a node leaves (Bps)                    | 24.00   | 32.81   | 8.69    | 32.79   | 26.15   | 24.89   | 9.873652819   |
| Average bandwidth usage whenever a node fails (Bps)                     | 13.37   | 26.64   | 32.25   | 30.87   | 17.51   | 24.13   | 8.325624301   |
| False positive rate of membership service when message loss rate is 3%  | 10.00%  | 20.00%  | 4.00%   | 1.14%   | 2.50%   | 7.53%   | 0.07749948335 |
| False positive rate of membership service when message loss rate is 10% | 16.70%  | 33.37%  | 7.69%   | 20.00%  | 100.00% | 35.55%  | 0.3718823564  |
| False positive rate of membership service when message loss rate is 30% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 100.00% | 0             |

#### 4 Machines Measurements
| 4 Machines                                                              | 1      | 2       | 3      | 4       | 5      | Average | Stdev       | 
|-------------------------------------------------------------------------|--------|---------|--------|---------|--------|---------|-------------| 
| Average background bandwidth usage (Bps)                                | 484.04 | 485.08  | 523.95 | 506.20  | 521.18 | 504.09  | 19.06778959 | 
| Average bandwidth usage whenever a node joins (Bps)                     | 577.11 | 579.29  | 412.66 | 572.01  | 564.08 | 541.03  | 71.9991559  | 
| Average bandwidth usage whenever a node leaves (Bps)                    | 496.78 | 515.08  | 477.16 | 512.17  | 535.75 | 507.39  | 21.86720764 | 
| Average bandwidth usage whenever a node fails (Bps)                     | 418.52 | 439.76  | 439.65 | 451.83  | 460.47 | 442.05  | 15.80830257 | 
| False positive rate of membership service when message loss rate is 3%  | 2.38%  | 10.00%  | 2.38%  | 6.66%   | 3.03%  | 4.89%   | 3.37%       | 
| False positive rate of membership service when message loss rate is 10% | 6.67%  | 20.00%  | 10.00% | 3.33%   | 10.00% | 10.00%  | 6.24%       | 
| False positive rate of membership service when message loss rate is 30% | 10.00% | 100.00% | 20.00% | 100.00% | 80.00% | 62.00%  | 43.82%      | 
