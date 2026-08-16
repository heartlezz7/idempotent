# Stage 1: Build the Go application
FROM golang:1.26 AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy the Go module files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the application code
COPY . .

# Build the Go application for Linux (Cloud Run uses Linux x86-64 architecture)
RUN GOOS=linux GOARCH=amd64 go build -o main .

# Use a minimal base image for production (e.g., Distroless or Alpine)
# FROM gcr.io/distroless/base

# use a minimal container for base image
FROM alpine:3.23

# Set environment variables
ENV PORT=8080 

# Copy the binary from the builder
COPY --from=builder /app/main /

# Expose the port that the application will run on
EXPOSE 8080

# Command to run the application
CMD ["/main"]