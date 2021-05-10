
# Dockerfile References: https://docs.docker.com/engine/reference/builder/

# Start from golang:1.12-alpine base image
FROM golang:1.12-alpine

# The latest alpine images don't have some tools like (`git` and `bash`).
# Adding git, bash and openssh to the image
RUN apk update && apk upgrade && \
    apk add --no-cache bash git openssh


# Set the Current Working Directory inside the container
WORKDIR /app

RUN go mod download
RUN go get github.com/golang/protobuf/proto
RUN go get github.com/gorilla/mux
RUN go get golang.org/x/net/http2
RUN go get golang.org/x/sys/unix
RUN go get google.golang.org/genproto/googleapis/rpc/status
RUN go get google.golang.org/protobuf/cmd/protoc-gen-go
RUN go get google.golang.org/grpc/cmd/protoc-gen-go-grpc


# Copy the source from the current directory to the Working Directory inside the container
COPY . .

# Expose port 8081 to the outside world
EXPOSE 80

# Run the executable
CMD ["go", "run", "main.go"]
