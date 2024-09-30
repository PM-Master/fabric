#!/bin/bash

# Define source and target image details
IMAGES=("fabric-tools:2.5.7" "fabric-ca:1.5.12" "fabric-orderer:2.5.7" "fabric-peer:2.5.7" "fabric-ccenv:2.5.7" "fabric-baseos:2.5.7")
SOURCE_REPO="hyperledger"
TARGET_REPO="csd773/redledger"

# Function to pull, retag, and push images for a specific architecture
retag_and_push_image() {
    IMAGE_NAME=$1
    IMAGE_VERSION=$2
    ARCH=$3
    TAG_SUFFIX=$4

    SOURCE_IMAGE="$SOURCE_REPO/$IMAGE_NAME:$IMAGE_VERSION"
    TARGET_IMAGE="$TARGET_REPO-$IMAGE_NAME$TAG_SUFFIX:$IMAGE_VERSION"
    TARGET_IMAGE_LATEST="$TARGET_REPO-$IMAGE_NAME$TAG_SUFFIX:latest"

    echo "Processing $IMAGE_NAME:$IMAGE_VERSION for architecture $ARCH..."

    # Pull the image for the specified architecture
    docker pull --platform linux/$ARCH $SOURCE_IMAGE

    # Retag the image
    docker tag $SOURCE_IMAGE $TARGET_IMAGE
    docker tag $SOURCE_IMAGE $TARGET_IMAGE_LATEST

    # Push the retagged image
    docker push $TARGET_IMAGE
    docker push $TARGET_IMAGE_LATEST

    echo "Image $TARGET_IMAGE and $TARGET_IMAGE_LATEST pushed successfully for architecture $ARCH!"
}

# Pull, retag, and push each image for both amd64 and arm64
for IMAGE in "${IMAGES[@]}"; do
    IMAGE_NAME="${IMAGE%%:*}"   # Extract the image name (e.g., fabric-ca)
    IMAGE_VERSION="${IMAGE##*:}" # Extract the image version (e.g., 2.5.7 or 1.5.12)

    # Pull, retag, and push for amd64
    retag_and_push_image "$IMAGE_NAME" "$IMAGE_VERSION" "amd64" ""

    # Pull, retag, and push for arm64 with '-arm' suffix
    retag_and_push_image "$IMAGE_NAME" "$IMAGE_VERSION" "arm64" "-arm"
done

echo "All images processed and pushed!"


# OLD script NOT WORKING
# docker buildx build . --build-arg FABRIC_VER=v2.5.7 --build-arg UBUNTU_VER=20.04 --build-arg GO_VER=1.21.9 --build-arg GO_TAGS=  --file images/baseos/Dockerfile --platform linux/amd64,linux/arm64 --tag docker.io/csd773/redledger-fabric-baseos:2.5.7 --tag docker.io/csd773/redledger-fabric-baseos:latest  --push 
# docker buildx build . --build-arg FABRIC_VER=v2.5.7 --build-arg UBUNTU_VER=20.04 --build-arg GO_VER=1.21.9 --build-arg GO_TAGS=  --file images/ccenv/Dockerfile --platform linux/amd64,linux/arm64 --tag docker.io/csd773/redledger-fabric-ccenv:2.5.7 --tag docker.io/csd773/redledger-fabric-ccenv:latest  --push 
# docker buildx build . --build-arg FABRIC_VER=v2.5.7 --build-arg UBUNTU_VER=20.04 --build-arg GO_VER=1.21.9 --build-arg GO_TAGS=  --file images/orderer/Dockerfile --platform linux/amd64,linux/arm64 --tag docker.io/csd773/redledger-fabric-orderer:2.5.7 --tag docker.io/csd773/redledger-fabric-orderer:latest  --push 
# docker buildx build . --build-arg FABRIC_VER=v2.5.7 --build-arg UBUNTU_VER=20.04 --build-arg GO_VER=1.21.9 --build-arg GO_TAGS=  --file images/peer/Dockerfile --platform linux/amd64,linux/arm64 --tag docker.io/csd773/redledger-fabric-peer:2.5.7 --tag docker.io/csd773/redledger-fabric-peer:latest  --push 
# docker buildx build . --build-arg FABRIC_VER=v2.5.7 --build-arg UBUNTU_VER=20.04 --build-arg GO_VER=1.21.9 --build-arg GO_TAGS=  --file images/tools/Dockerfile --platform linux/amd64,linux/arm64 --tag docker.io/csd773/redledger-fabric-tools:2.5.7 --tag docker.io/csd773/redledger-fabric-tools:latest  --push 