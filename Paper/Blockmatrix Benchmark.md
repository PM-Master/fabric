# BlockMatrix Benchmark:


This document presents the results of a benchmark conducted using Caliper to evaluate the performance of a network. Caliper is a blockchain benchmarking tool that allows for standardized testing of different blockchain networks.In this benchmark, the number of workers, transaction load, and assets were changed in the configuration file to measure the network’s performance under different conditions.
But first let’s explain how to use Caliper.


### How to install and run Caliper

1. Create a folder named **caliper-workspace** at the same level as the **fabric-samples** directory, and then within the **caliper-workspace folder**, create three folders named **networks**, **benchmarks**, and **workload** respectively.

	Caliper installation and use will be based on a local npm installation. Within the **caliper-workspace** directory, install caliper CLI using the following terminal command:
    
    ```
    npm install --only=prod @hyperledger/caliper-cli@0.5.0
    ```

	Bind the SDK using the following terminal command:

    ```
    npx caliper bind --caliper-bind-sut fabric:2.4
    ```
    
2. Under the networks folder create a template file called **networkConfig.yaml** with the following content:

```
name: Caliper test
version: "2.0.0"

caliper:
  blockchain: fabric

channels:
    - channelName: dbmchannel
      contracts: 
      - id: DBM_chaincode

organizations:
  - mspid: Org1MSP
    peers:
    - peer0.org1.example.com
    identities:
      certificates:
      - name: 'User1'
        clientPrivateKey:
          path: 'C:/Users/scc7/Documents/caliper-benchmarks/caliper-benchmarks/networks/fabric/redledger-samples-network/organizations/peerOrganizations/org1.example.com/users/User1@org1.example.com/msp/keystore/priv_sk'
        clientSignedCert:
          path: 'C:/Users/scc7/Documents/caliper-benchmarks/caliper-benchmarks/networks/fabric/redledger-samples-network/organizations/peerOrganizations/org1.example.com/users/User1@org1.example.com/msp/signcerts/User1@org1.example.com-cert.pem'
    connectionProfile:
      path: 'C:/Users/scc7/Documents/caliper-benchmarks/caliper-benchmarks/networks/fabric/redledger-samples-network/organizations/peerOrganizations/org1.example.com/connection-org1.yaml'
      discover: true  
```      

3. Build a test workload Module.

	The workload module interacts with the deployed smart contract during the benchmark round. The workload module extends the Caliper class WorkloadModuleBase from caliper-core. The workload module provides 3 overrides:

	**initializeWorkloadModule** - used to initialize any required items for the benchmark

	**submitTransaction** - used to interact with the smart contract method during the monitored phase of the benchmark

	**cleanupWorkloadModule** - used to clean up after the completion of the benchmark


	The workload we will be driving aims to benchmark the querying of existing assets within the world state database. Consequently we will use all 3 phases available in the workload module:

	**initializeWorkloadModule** - to create assets that may be queried in the submitTransaction phase

	**submitTransaction** - to query assets created in the initializeWorkloadModule phase

	**cleanupWorkloadModule** - used to remove assets created in the initializeWorkloadModule phase so that the benchmark may be repeated.


	Within the **workload** folder create a file called **readAsset.js** with the following content:
	
```
	'use strict';

	const { WorkloadModuleBase } = require('@hyperledger/caliper-core');

	class MyWorkload extends WorkloadModuleBase {
    constructor() {
        super();
    }

	    async initializeWorkloadModule(workerIndex, totalWorkers, roundIndex, roundArguments, sutAdapter, sutContext) {
	        await super.initializeWorkloadModule(workerIndex, totalWorkers, roundIndex, roundArguments, sutAdapter, sutContext);
	
	        for (let i=0; i<this.roundArguments.assets; i++) {
            const assetID = `${this.workerIndex}_${i}`;
            console.log(`Worker ${this.workerIndex}: Creating asset ${assetID}`);
            const request = {
                contractId: 'DBM_chaincode',
                contractFunction: 'Put',
                invokerIdentity: 'User1',
                contractArguments: ["[{\"Key\":\"key1\",\"Value\":\"value1\"}]"],
                readOnly: false
            };

	            await this.sutAdapter.sendRequests(request);
    }
    }

	    async submitTransaction() {
        const randomId = Math.floor(Math.random()*this.roundArguments.assets);
        const myArgs = {
            contractId: 'DBM_chaincode',
            contractFunction: 'Get',
            invokerIdentity: 'User1',
            contractArguments: [`${this.workerIndex}_${randomId}`],
            readOnly: true
        };

	        await this.sutAdapter.sendRequests(myArgs);
    }

	    async cleanupWorkloadModule() {
        for (let i=0; i<this.roundArguments.assets; i++) {
            const assetID = `${this.workerIndex}_${i}`;
            console.log(`Worker ${this.workerIndex}: Deleting asset ${assetID}`);
            const request = {
                contractId: 'DBM_chaincode',
                contractFunction: 'Delete',
                invokerIdentity: 'User1',
                contractArguments: ["[{\"Key\":\"key1\"}]"],
                readOnly: false
            };

	            await this.sutAdapter.sendRequests(request);
        }
    }
}

	function createWorkloadModule() {
    return new MyWorkload();
}

	module.exports.createWorkloadModule = createWorkloadModule;

```

4. Build a benchmark configuration file.

	The benchmark configuration file defines the benchmark rounds and references the defined workload module(s). It will specify the number of test workers to use when generating the load, the number of test rounds, the duration of each round, the rate control applied to the transaction load during each round, and options relating to monitors. This particular tutorial will not make use of any of the available monitors or transaction observers; for these details please refer to the documentation.

	The benchmark configuration file may be provided in a yaml or json format: here we will use a yaml format. Please note that yaml files are case sensitive and all labels are in lowercase.

	Under the **benchmarks** folder create a file called **myAssetBenchmark.yaml** with the following content:
	
```
	test:
    name: DBM-chaincode-contract-benchmark
    description: test benchmark
    workers:
      number: 2
    rounds:
      - label: readAsset
        description: Read asset benchmark
        txDuration: 30
        rateControl:
          type: fixed-load
          opts:
            transactionLoad: 2
        workload:
          module: workload/readAsset.js
          arguments:
            assets: 10
            contractId: DBM_chaincode
            
```
5. Run the Caliper Benchmark.

```
	npx caliper launch manager --caliper-workspace ./ --caliper-networkconfig networks/networkConfig.yaml --caliper-benchconfig benchmarks/myAssetBenchmark.yaml --caliper-flow-only-test --caliper-fabric-gateway-enabled
```
Result should look like this:

```
2023.03.07-11:21:01.261 info  [caliper] [report-builder]        ### Test result ###
2023.03.07-11:21:01.266 info  [caliper] [report-builder]
+-----------+------+------+-----------------+-----------------+-----------------+-----------------+------------------+
| Name      | Succ | Fail | Send Rate (TPS) | Max Latency (s) | Min Latency (s) | Avg Latency (s) | Throughput (TPS) |
|-----------|------|------|-----------------|-----------------|-----------------|-----------------|------------------|
| readAsset | 5436 | 0    | 183.8           | 0.04            | 0.00            | 0.01            | 183.8            |
+-----------+------+------+-----------------+-----------------+-----------------+-----------------+------------------+

2023.03.07-11:21:01.267 info  [caliper] [round-orchestrator]    Finished round 1 (readAsset) in 30.111 seconds
2023.03.07-11:21:01.267 info  [caliper] [monitor.js]    Stopping all monitors
2023.03.07-11:21:01.267 info  [caliper] [report-builder]        ### All test results ###
2023.03.07-11:21:01.268 info  [caliper] [report-builder]
+-----------+------+------+-----------------+-----------------+-----------------+-----------------+------------------+
| Name      | Succ | Fail | Send Rate (TPS) | Max Latency (s) | Min Latency (s) | Avg Latency (s) | Throughput (TPS) |
|-----------|------|------|-----------------|-----------------|-----------------|-----------------|------------------|
| readAsset | 5436 | 0    | 183.8           | 0.04            | 0.00            | 0.01            | 183.8            |
+-----------+------+------+-----------------+-----------------+-----------------+-----------------+------------------+
```

After we managed to fully install and use Caliper, we can change the inputs in the configuration files to proceed to the Benchmarks.







### Benchmark:

To set up the benchmark, we used our Blockmatrix network with a Blockmatrix channel to test and ensured it was configured properly for testing.
We changed the inputs in the configuration file **myAssetBenchmark** so it would be able to test all the methods of our chaincode:

```
FullMethodAssetBenchmark.yaml :
test:
    name: DBM-chaincode-contract-benchmark
    description: test benchmark
    workers:
      number: 2
    rounds:
      - label: GetAsset
        description: Read asset benchmark
        txDuration: 30
        rateControl:
          type: fixed-load
          opts:
            transactionLoad: 200
        workload:
          module: workload/GetAsset.js
          arguments:
            assets: 1000
            contractId: DBM_chaincode
      - label: PutAsset
        description: Put asset benchmark
        txDuration: 30
        rateControl:
          type: fixed-load
          opts:
            transactionLoad: 200
        workload:
          module: workload/PutAsset.js
          arguments:
            assets: 1000
            contractId: DBM_chaincode
      - label: DeleteAsset
        description: Delete asset benchmark
        txDuration: 30
        rateControl:
          type: fixed-load
          opts:
            transactionLoad: 200
        workload:
          module: workload/DeleteAsset.js
          arguments:
            assets: 1000
            contractId: DBM_chaincode
```

By adding different rounds for each method, we manage to test the 3 methods of our chaincode **Get**, **Put**, **Delete**.  
Each test run was repeated many times to ensure consistency.  
We used Caliper to execute transactions on the network and recorded the response time for each transaction. 
We chose to modify the transaction load and the number of assets for our benchmarks.  

**Transaction load** refers to the number of transactions that are being sent to the network with a specific period of time, usually measured in transactions per second TPS. A higher transaction load means that more transactions are being sent to the network, which can put more stress on the network and potentially impact performances.  

**Assets** can impact the network’s performance in various ways. For example if there are a large number of assets on the network, the transaction processing time can increase due to the need for additional processing power and storage requirements. On the other hand a smaller number of assets can lead to a faster processing time.
We didn’t change the number of worker to see how the network would perform with a minimum of worker.

- The **first Benchmark** launched with regular parameters:

| Benchmark | Channel     | Assets | Transaction Load | Name        | Success | Fail | Send Rate(TPS) | Max Latency (s) | Min Latency (s) | Avg Latency(s) | Throughput(TPS) | Time(s) |
|-----------|-------------|--------|------------------|-------------|---------|------|----------------|-----------------|-----------------|----------------|-----------------|---------|
| 1         | DBM Channel | 100    | 2                | GetAsset    | 4476    | 0    | 151.2          | 0.05            | 0               | 0.01           | 151.2           | 1200    |
| 1         | DBM Channel | 100    | 2                | PutAsset    | 500     | 0    | 10.4           | 2.49            | 0.05            | 0.74           | 9.9             | 1200    |
| 1         | DBM Channel | 100    | 2                | DeleteAsset | 480     | 0    | 9.7            | 2.14            | 0.05            | 0.74           | 9.3             | 1200    |

_**Observations:**_

This benchmark was conducted with relatively low parameters, resulting in a low transaction load and low asset count. The overall throughput was also relatively low, with the highest TPS achieved by GetAsset transactions.  




- The **second Benchmark** we increased some parameters:

| Benchmark | Channel     | Assets | Transaction Load | Name        | Success | Fail | Send Rate(TPS) | Max Latency (s) | Min Latency (s) | Avg Latency(s) | Throughput(TPS) | Time(s) |
|-----------|-------------|--------|------------------|-------------|---------|------|----------------|-----------------|-----------------|----------------|-----------------|---------|
| 2         | DBM Channel | 100    | 20               | GetAsset    | 9121    | 0    | 308.2          | 0.08            | 0.01            | 0.02           | 308.1           | 1170    |
| 2         | DBM Channel | 100    | 20               | PutAsset    | 4600    | 0    | 35.7           | 15.38           | 0.1             | 0.48           | 31.9            | 1170    |
| 2         | DBM Channel | 100    | 20               | DeleteAsset | 4300    | 0    | 34.4           | 3.44            | 0.14            | 0.56           | 33.8            | 1170    |


_**Observations:**_

This benchmark increased the transaction load by a factor of **10**, while keeping the asset count constant. This resulted in a significant increase in throughput for GetAsset transactions, but a decrease in throughput for PutAsset and DeleteAsset transactions. The average latencies for PutAsset and DeleteAsset transactions also increased, likely due to the increased load.  


- The **third Benchmark**, we wanted to try to have more than **100 000** transactions, here are the parameters:

| Benchmark | Channel     | Assets | Transaction Load | Name        | Success | Fail | Send Rate(TPS) | Max Latency (s) | Min Latency (s) | Avg Latency(s) | Throughput(TPS) | Time(s) |
|-----------|-------------|--------|------------------|-------------|---------|------|----------------|-----------------|-----------------|----------------|-----------------|---------|
| 3         | DBM Channel | 1000   | 200              | GetAsset    | 17728   | 0    | 599.2          | 0.08            | 0               | 0.02           | 598             | 18000   |
| 3         | DBM Channel | 1000   | 200              | PutAsset    | 215983  | 17   | 46.2           | 8.38            | 0.07            | 4.03           | 45.6            | 18000   |
| 3         | DBM Channel | 1000   | 200              | DeleteAsset | 148000  | 0    | 33.5           | 38.78           | 0.54            | 4.39           | 33.5            | 18000   |

_**Observations:**_

This benchmark was conducted with a much larger transaction load and asset count, resulting in a significantly higher number of successful transactions across all types. The throughput for GetAsset transactions continued to be much higher than for PutAsset and DeleteAsset transactions, likely due to the nature of the transactions. However, the average latencies for PutAsset and DeleteAsset transactions were much higher, likely due to the increased load and asset count.



- The **fourth Benchmark** we wanted to test the network but with a non Data Blockmatrix channel to see if it would have an impact on the performance of the network:

| Benchmark | Channel         | Assets | Transaction Load | Name        | Success | Fail | Send Rate(TPS) | Max Latency (s) | Min Latency (s) | Avg Latency(s) | Throughput(TPS) | Time(s) |
|-----------|-----------------|--------|------------------|-------------|---------|------|----------------|-----------------|-----------------|----------------|-----------------|---------|
| 4         | non DBM Channel | 1000   | 200              | GetAsset    | 15762   | 0    | 532.7          | 0.07            | 0.01            | 0.01           | 532.6           | 12300   |
| 4         | non DBM Channel | 1000   | 200              | PutAsset    | 218000  | 0    | 111.8          | 8.81            | 0.06            | 1.86           | 111.6           | 12300   |
| 4         | non DBM Channel | 1000   | 200              | DeleteAsset | 216000  | 0    | 106.7          | 10.83           | 0.06            | 1.93           | 106.6           | 12300   |

_**Observations:**_  

The results show that the system was able to handle a higher transaction load in comparison to the 3rd benchmark.Overall, the 4th benchmark performed better in terms of a better Send Rate and an average latency reduced compared to the DBM channel. The duration of the test was also much shorter (12300 sec) than the 3rd benchmark (18000 sec).

- Here is the table with all the results:

| Benchmark | Channel         | Assets | Transaction Load | Name        | Success | Fail | Send Rate(TPS) | Max Latency (s) | Min Latency (s) | Avg Latency(s) | Throughput(TPS) | Time(s) |
|-----------|-----------------|--------|------------------|-------------|---------|------|----------------|-----------------|-----------------|----------------|-----------------|---------|
| 1         | DBM Channel     | 100    | 2                | GetAsset    | 4476    | 0    | 151.2          | 0.05            | 0               | 0.01           | 151.2           | 1200    |
| 2         | DBM Channel 		| 100    | 20               | GetAsset    | 9121    | 0    | 308.2          | 0.08            | 0.01            | 0.02           | 308.1           | 1170    |
| 3         | DBM Channel 		| 1000   | 200              | GetAsset    | 17728   | 0    | 599.2          | 0.08            | 0               | 0.02           | 598             | 18000   |
| 4         | non DBM Channel | 1000   | 200              | GetAsset    | 15762   | 0    | 532.7          | 0.07            | 0.01            | 0.01           | 532.6           | 12300   |
| 1         | DBM Channel 		| 100    | 2                | PutAsset    | 500     | 0    | 10.4           | 2.49            | 0.05            | 0.74           | 9.9             | 1200    |
| 2         | DBM Channel 		| 100    | 20               | PutAsset    | 4600    | 0    | 35.7           | 15.38           | 0.1             | 0.48           | 31.9            | 1170    |
| 3         | DBM Channel 		| 1000   | 200              | PutAsset    | 215983  | 17   | 46.2           | 8.38            | 0.07            | 4.03           | 45.6            | 18000   |
| 4         | non DBM Channel | 1000   | 200              | PutAsset    | 218000  | 0    | 111.8          | 8.81            | 0.06            | 1.86           | 111.6           | 12300   |
| 1         | DBM Channel 		| 100    | 2                | DeleteAsset | 480     | 0    | 9.7            | 2.14            | 0.05            | 0.74           | 9.3             | 1200    |
| 2         | DBM Channel 		| 100    | 20               | DeleteAsset | 4300    | 0    | 34.4           | 3.44            | 0.14            | 0.56           | 33.8            | 1170    |
| 3         | DBM Channel 		| 1000   | 200              | DeleteAsset | 148000  | 0    | 33.5           | 38.78           | 0.54            | 4.39           | 33.5            | 18000   |
| 4         | non DBM Channel | 1000   | 200              | DeleteAsset | 216000  | 0    | 106.7          | 10.83           | 0.06            | 1.93           | 106.6           | 12300   |

### **Conclusion:**

**GetAsset Transactions:**.

As the transaction load increases, the throughput of GetAsset transactions generally increases as well.
The asset count does not seem to have a significant impact on the throughput of GetAsset transactions.
The average latency for GetAsset transactions remains relatively low across all benchmarks.

**PutAsset Transactions:** 

Transaction load has a significant impact on PutAsset transaction throughput, with higher loads resulting in higher throughput. While asset count can also impact throughput, it appears to have a smaller impact compared to transaction load. However, higher transaction loads and asset counts can lead to higher average latencies, which can impact overall system performance.

**DeleteAsset Transactions:** 


Similar to the PutAsset transactions, there is a significant increase in average latency for DeleteAsset transactions when the transaction load and asset count are increased. The highest average latency was observed in the benchmark with the highest transaction load and asset count.
Overall, the impact of transaction load and asset count on the throughput and latency of DeleteAsset transactions is similar to that observed for PutAsset transactions, with a significant increase in throughput observed when the transaction load is increased from 2 to 20, and a marginal increase in throughput observed when the asset count is increased from 100 to 1000. Additionally, there is a significant increase in latency when both the transaction load and asset count are increased.


Based on the analysis of the four benchmarks, it can be concluded that both the asset count and transaction load have a significant impact on the overall performance of the system. On the other hand having a minimal count of worker seem to not really impact the performance with just a few to no transactions failed on all of the 4 benchmarks.

Increasing the transaction load generally led to a higher throughput for GetAsset transactions, but the same throughput for PutAsset and DeleteAsset transactions. This is likely due to the nature of the transactions, as GetAsset transactions only retrieve data and do not modify it, whereas PutAsset and DeleteAsset transactions modify data and require more processing time.
Increasing the asset count generally led to an increase in average latency for all transaction types. This is likely due to the increased amount of data that needs to be processed and the associated overhead.
Overall, it is important to carefully consider the asset count and transaction load when designing and testing a system, as both factors can have a significant impact on the system's performance. Additionally, it is important to choose appropriate benchmarks that reflect the expected usage patterns of the system.

