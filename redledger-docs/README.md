# REDLedger Docs

## Build process

### 1. Build docker images and binaries
Form the project root, run 

```shell
make clean-all docker native
```

This will clean local Fabric docker images and binaries. Then it will build new images and binaries using REDLedger. The
created binaries will be found in `/build/bin`.

### 2. Copy binaries

Copy and paste the created binaries into the `/bin` directory of your local fabric network environment. Note: The 
fabric-ca-server and fabric-ca-client binaries will not be replaced.

## Create channel with REDLedger
The following instructions utilize the scripts provided in `fabric-samples`.

### 1. Create a blockmatrix channel

```shell
./network.sh up createChannel -c mychannel -ca -l blockmatrix
```